package homelab.incubator.flow.v2


import scala.collection.immutable.Queue

import homelab.common.data.Batch
import homelab.common.data.Batch.LineageMismatch
import homelab.incubator.flow.v2.Serial.State
import zio.*


class Serial[R, E, BE, In, Out](
  batchSize: Int,
  capturedScope: Scope,
  ref: Ref[Serial.State[LineageMismatch | E | BE, In, Out]],
  logic: Batcher.Logic[R, E, BE, In, Out],
) extends Batcher[R, LineageMismatch | E | BE, In, Out] {
  private type Error = LineageMismatch | E | BE

  private val drainSize = math.max(1, batchSize)

  // Every caller enqueues its own promise and awaits it; the caller that found the batcher idle also forks
  // the drain. No one has a privileged inline path, so a failure can never skip the drain — and the drain
  // completes every enqueued promise (see `runBatch`), so a caller is never left awaiting forever.
  //
  // Enqueue-and-fork is uninterruptible: a leader (`started`) interrupted after enqueuing but before forking
  // the drain would leave its request queued with no drain ever started — and since the state is now
  // `InFlight`, no later caller forks one either, wedging the batcher. Only the wait is interruptible.
  override def run(in: In): ZIO[R, Error, Out] =
    ZIO.uninterruptibleMask: restore =>
      for
        promise <- Promise.make[Error, Out]
        started <- enqueue(in, promise)
        _       <- runDrain.forkIn(capturedScope).when(started)
        out     <- restore(promise.await)
      yield out

  private def enqueue(input: In, promise: Promise[Error, Out]): UIO[Boolean] =
    ref.modify:
      case State.Idle()          => true  -> State.InFlight(Queue((input, promise)))
      case State.InFlight(queue) => false -> State.InFlight(queue.enqueue((input, promise)))

  private def runDrain: URIO[R, Unit] =
    ref
      .modify:
        case State.Idle()                   => Nil -> State.Idle()
        case State.InFlight(q) if q.isEmpty => Nil -> State.Idle()
        case State.InFlight(q)              =>
          val (batch, rest) = q.splitAt(drainSize) // front = oldest batchSize (FIFO)
          batch.toList -> State.InFlight(rest)
      .flatMap:
        case Nil  => ZIO.unit
        case list => runBatch(Batch.make(list)) *> runDrain

  // Total by construction: whatever happens to the bulk call — a per-item `BE`, a whole-batch `E`, a lineage
  // mismatch, a defect, or interruption — is routed to the waiting promises, so a drained batch can never
  // leave a caller awaiting forever.
  private def runBatch(batch: Batch.Success[(In, Promise[Error, Out])]): URIO[R, Unit] = {
    val promises = batch.map((_, promise) => promise)
    logic
      .run(batch.map((in, _) => in))
      .tap(result => ZIO.fromEither(promises.verifyLineage(result)))
      .foldCauseZIO(
        cause => failAll(promises, cause),
        result => fulfil(promises, result),
      )
  }

  private def failAll(promises: Batch.Success[Promise[Error, Out]], cause: Cause[Error]): UIO[Unit] =
    ZIO.foreachDiscard(promises.values)(_.failCause(cause).unit)

  // Same lineage (verified above) ⇒ result and promises share the index set and are both complete, so the
  // positional zip pairs each item's outcome with the caller that submitted it.
  private def fulfil(promises: Batch.Success[Promise[Error, Out]], result: Batch[BE, Out]): UIO[Unit] =
    ZIO.foreachDiscard(result.toList.zip(promises.values)):
      case (Left(err), promise)  => promise.fail(err).unit
      case (Right(out), promise) => promise.succeed(out).unit
}


object Serial:
  enum State[Err, In, Out]:
    case Idle()
    case InFlight(requests: Queue[(In, Promise[Err, Out])])
