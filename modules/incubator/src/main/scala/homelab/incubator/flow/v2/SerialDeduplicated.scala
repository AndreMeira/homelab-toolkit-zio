package homelab.incubator.flow.v2


import scala.collection.immutable.Queue

import homelab.common.data.Batch
import homelab.common.data.Batch.LineageMismatch
import homelab.incubator.flow.v2.SerialDeduplicated.State
import zio.*


// A `Serial` batcher that coalesces by key: requests sharing a `requestKey` collapse to a single downstream
// computation AND a single shared `Promise` — every caller of a key awaits the same promise, so a duplicate
// allocates nothing, enqueues nothing, and only reads state. The drain/totality/interruption guarantees are
// `Serial`'s; dedup only reshapes the state, the admission, and the fan-out.
class SerialDeduplicated[R, E, BE, Key, In, Out](
  batchSize: Int,
  capturedScope: Scope,
  ref: Ref[SerialDeduplicated.State[Key, LineageMismatch | E | BE, In, Out]],
  requestKey: In => Key,
  logic: Batcher.Logic[R, E, BE, In, Out],
) extends Batcher[R, LineageMismatch | E | BE, In, Out] {
  private type Error = LineageMismatch | E | BE

  private val drainSize = math.max(1, batchSize)

  override def run(in: In): ZIO[R, Error, Out] =
    val key = requestKey(in)
    ZIO.uninterruptibleMask: restore =>
      ref.get.flatMap:
        // Fast path — the key is already pending: share its promise. No allocation, no enqueue, no CAS.
        case State.InFlight(_, byKey) if byKey.contains(key) =>
          restore(byKey(key)._2.await)
        // Slow path — first for this key (or the batcher is idle): allocate, install (or reuse on a race),
        // fork the drain iff we were the leader, then await the shared promise.
        case _ =>
          for
            promise           <- Promise.make[Error, Out]
            installed         <- install(key, in, promise)
            (shared, started)  = installed
            _                 <- runDrain.forkIn(capturedScope).when(started)
            out               <- restore(shared.await)
          yield out

  // One `modify` that decides leadership and installs the key's shared promise. Re-checks `byKey` because a
  // racing first-of-key fiber may have installed it since our `ref.get` — then we reuse theirs and drop ours.
  private def install(key: Key, in: In, promise: Promise[Error, Out]): UIO[(Promise[Error, Out], Boolean)] =
    ref.modify:
      case State.Idle() =>
        (promise, true) -> State.InFlight(Queue(key), Map(key -> (in, promise)))
      case State.InFlight(order, byKey) =>
        byKey.get(key) match
          case Some((_, existing)) => (existing, false) -> State.InFlight(order, byKey)
          case None                => (promise, false)  -> State.InFlight(order.enqueue(key), byKey.updated(key, (in, promise)))

  private def runDrain: URIO[R, Unit] =
    ref
      .modify:
        case State.Idle()                              => Nil -> State.Idle()
        case State.InFlight(order, _) if order.isEmpty => Nil -> State.Idle()
        case State.InFlight(order, byKey)              =>
          val (keys, rest) = order.splitAt(drainSize) // oldest `drainSize` distinct keys (FIFO)
          keys.toList.map(key => byKey(key)) -> State.InFlight(rest, byKey -- keys)
      .flatMap:
        case Nil   => ZIO.unit
        case taken => runBatch(taken) *> runDrain

  // Total by construction: every key's shared promise is completed on any outcome — a per-item `BE`, a
  // whole-batch `E`, a lineage mismatch, a defect, or interruption — waking all of that key's awaiters.
  private def runBatch(taken: List[(In, Promise[Error, Out])]): URIO[R, Unit] = {
    val inputs   = Batch.make(taken.map((in, _) => in))
    val promises = taken.map((_, promise) => promise)
    logic
      .run(inputs)
      .tap(result => ZIO.fromEither(inputs.verifyLineage(result)))
      .foldCauseZIO(
        cause => failAll(promises, cause),
        result => fulfil(promises, result),
      )
  }

  private def failAll(promises: List[Promise[Error, Out]], cause: Cause[Error]): UIO[Unit] =
    ZIO.foreachDiscard(promises)(_.failCause(cause).unit)

  // Same lineage (verified above) ⇒ result slot `i` is key `i`'s outcome; complete its one shared promise.
  private def fulfil(promises: List[Promise[Error, Out]], result: Batch[BE, Out]): UIO[Unit] =
    ZIO.foreachDiscard(result.toList.zip(promises)):
      case (Left(err), promise)  => promise.fail(err).unit
      case (Right(out), promise) => promise.succeed(out).unit
}


object SerialDeduplicated:
  enum State[Key, Err, In, Out]:
    case Idle()
    case InFlight(order: Queue[Key], byKey: Map[Key, (In, Promise[Err, Out])])
