package homelab.common.flow


import scala.collection.immutable.Queue

import homelab.common.data.Batch
import homelab.common.data.Batch.LineageMismatch
import homelab.common.flow.Serial.State
import zio.*


/**
 * A FIFO batcher: every caller enqueues a promise and awaits it; the caller that finds the batcher idle also
 * forks the (interruptible) drain, which processes one batch at a time. The drain is *total* — every promise
 * is completed on any outcome (a per-item `BE`, a whole-batch `E`, a lineage mismatch, a defect, an interrupt,
 * or scope close) — so no caller ever hangs. Built via [[Batcher.serial]].
 *
 * @param batchSize     the maximum inputs per bulk call
 * @param capturedScope the scope the drain is forked into (closing it drops pending work)
 * @param ref           the FIFO state, `Idle` or `InFlight`
 * @param logic         the bulk operation
 */
private[flow] final class Serial[E, BE, In, Out](
  batchSize: Int,
  capturedScope: Scope,
  ref: Ref[Serial.State[LineageMismatch | E | BE, In, Out]],
  logic: Batcher.Logic[E, BE, In, Out],
) extends Batcher[LineageMismatch | E | BE, In, Out] {
  private type Err = LineageMismatch | E | BE

  private val drainSize = math.max(1, batchSize)

  /**
   * Enqueue this request, fork the drain if we found the batcher idle, then await our promise. Enqueue+fork is
   * uninterruptible (a leader interrupted before forking would wedge the batcher); the drain body is
   * `interruptible` so scope close can kill it; only the wait is interruptible.
   *
   * @param in the request
   * @return its result; aborts with `Err` or is interrupted on scope close
   */
  override def run(in: In): IO[Err, Out] =
    ZIO.uninterruptibleMask: restore =>
      for
        promise <- Promise.make[Err, Out]
        started <- enqueue(in, promise)
        _       <- runDrain.interruptible.forkIn(capturedScope).when(started)
        out     <- restore(promise.await)
      yield out

  /**
   * The unbatched one-shot: a size-1 bulk call, its single result taken from `toList.head` (a `Batch` is
   * complete) with the per-item `BE` surfaced via `fromEither`. No routing, so lineage is irrelevant.
   *
   * @param in the request
   * @return its result; aborts with the `E`/`BE` of a one-item `logic` call
   */
  override private[flow] def direct(in: In): IO[Err, Out] =
    logic.run(Batch.single(in)).flatMap(result => ZIO.fromEither(result.toList.head))

  /**
   * Append `(in, promise)` to the queue.
   *
   * @return `true` if the batcher was idle (so the caller must fork the drain), else `false`
   */
  private def enqueue(in: In, promise: Promise[Err, Out]): UIO[Boolean] =
    ref.modify:
      case State.Idle()          => true  -> State.InFlight(Queue((in, promise)))
      case State.InFlight(queue) => false -> State.InFlight(queue.enqueue((in, promise)))

  /**
   * Drain the queue one FIFO batch at a time until empty, then return to idle.
   *
   * @return unit; carries the drain's error/interrupt in its (forked) fiber, unobserved by callers
   */
  private def runDrain: IO[Err, Unit] =
    ref
      .modify:
        case State.Idle()                   => Nil -> State.Idle()
        case State.InFlight(q) if q.isEmpty => Nil -> State.Idle()
        case State.InFlight(q)              =>
          val (batch, rest) = q.splitAt(drainSize) // oldest batchSize (FIFO)
          batch.toList -> State.InFlight(rest)
      .flatMap:
        case Nil   => ZIO.unit
        case batch => runBatch(batch) *> runDrain

  /**
   * Run one bulk call and fan the results back to the batch's promises.
   *
   * Promises are completed from the exit in an *uninterruptible* finalizer, so any `BE` / `E` / lineage
   * mismatch / defect / interrupt (scope close killing the drain mid-flight) reaches the callers. An interrupt
   * then propagates to stop the drain, but a typed failure or defect is swallowed so the remaining queue is
   * still drained.
   *
   * @param batch the `(input, promise)` pairs to process
   * @return unit; never fails except by propagating an interrupt
   */
  private def runBatch(batch: List[(In, Promise[Err, Out])]): IO[Err, Unit] =
    val inputs   = Batch.make(batch.map((in, _) => in))
    val promises = batch.map((_, promise) => promise)
    logic
      .run(inputs)
      .tap(result => ZIO.fromEither(inputs.verifyLineage(result)))
      .onExit:
        case Exit.Success(result) => fulfil(promises, result)
        case Exit.Failure(cause)  => failAll(promises, cause)
      .catchAllCause(cause => if cause.isInterrupted then ZIO.refailCause(cause) else ZIO.unit)
      .unit

  /**
   * Fail every promise with `cause` (a whole-batch failure, defect, or interrupt).
   *
   * @return unit
   */
  private def failAll(promises: List[Promise[Err, Out]], cause: Cause[Err]): UIO[Unit] =
    ZIO.foreachDiscard(promises)(_.failCause(cause).unit)

  /**
   * Complete each promise from its result slot — same lineage (verified) ⇒ slot `i` is caller `i`'s outcome.
   *
   * @return unit
   */
  private def fulfil(promises: List[Promise[Err, Out]], result: Batch[BE, Out]): UIO[Unit] =
    ZIO.foreachDiscard(result.toList.zip(promises)):
      case (Left(err), promise)  => promise.fail(err).unit
      case (Right(out), promise) => promise.succeed(out).unit

  /**
   * Scope finalizer: interrupt every queued caller so close drops pending work without stranding anyone (the
   * in-flight batch is handled by [[runBatch]]'s `onExit`).
   *
   * @return unit
   */
  private[flow] def abandon: UIO[Unit] =
    ref.get.flatMap:
      case State.InFlight(queue) => ZIO.foreachDiscard(queue)((_, promise) => promise.interrupt.unit)
      case State.Idle()          => ZIO.unit
}


object Serial:

  /**
   * The FIFO state: `Idle`, or `InFlight` holding the pending `(input, promise)` queue.
   *
   * @tparam Err the error each promise carries
   */
  enum State[Err, In, Out]:
    case Idle()
    case InFlight(requests: Queue[(In, Promise[Err, Out])])
