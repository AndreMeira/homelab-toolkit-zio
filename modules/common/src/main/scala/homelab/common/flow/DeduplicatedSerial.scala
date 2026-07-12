package homelab.common.flow


import scala.collection.immutable.Queue
import scala.util.chaining.scalaUtilChainingOps

import homelab.common.data.Batch
import homelab.common.data.Batch.LineageMismatch
import homelab.common.flow.DeduplicatedSerial.State
import zio.*


/**
 * A [[Serial]] batcher that also coalesces by key: requests sharing a `requestKey` collapse to one downstream
 * computation *and* one shared promise, so a duplicate allocates nothing, enqueues nothing, and only reads
 * state. `order` owns FIFO over distinct keys; `byKey` owns dedup + fan-in. Built via [[Batcher.deduplicated]].
 *
 * @param batchSize     the maximum distinct keys per bulk call
 * @param capturedScope the scope the drain is forked into
 * @param ref           the dedup state, `Idle` or `InFlight`
 * @param requestKey    the coalescing key of a request
 * @param logic         the bulk operation
 */
final private[flow] class DeduplicatedSerial[E, BE, Key, In, Out](
  batchSize: Int,
  capturedScope: Scope,
  ref: Ref[DeduplicatedSerial.State[Key, LineageMismatch | E | BE, In, Out]],
  requestKey: In => Key,
  logic: Batcher.Logic[E, BE, In, Out],
) extends Batcher[LineageMismatch | E | BE, In, Out] {
  private type Err = LineageMismatch | E | BE

  private val drainSize = math.max(1, batchSize)

  /**
   * If the key is already pending, share its promise (fast path — no allocation, no enqueue); otherwise
   * allocate, install (or reuse on a race), fork the drain if we were the leader, and await. Uninterruptible
   * except the final wait.
   *
   * @param in the request
   * @return its result; aborts with `Err` or is interrupted on scope close
   */
  def run(in: In): IO[Err, Out] = {
    val key = requestKey(in)
    ZIO.uninterruptibleMask: restore =>
      ref.get.flatMap: state =>
        state.getPromise(key) match
          case Some(promise) => restore(promise.await) // fast path
          case _             => createAndStorePromise(in, key).flatMap(promise => restore(promise.await))
  }

  /**
   * The unbatched one-shot (no dedup): a size-1 bulk call, its single result taken from `toList.head` with the
   * per-item `BE` surfaced via `fromEither`. No routing, so lineage is irrelevant.
   *
   * @param in the request
   * @return its result; aborts with the `E`/`BE` of a one-item `logic` call
   */
  override private[flow] def direct(in: In): IO[Err, Out] = {
    val batch = Batch.single(in)
    logic
      .run(batch)
      .tap(result => ZIO.fromEither(batch.verifyLineage(result)))
      .flatMap(result => ZIO.fromEither(result.toList.head))
  }

  /**
   * Allocate a fresh promise, install it for `key` (or reuse a racing fiber's), and fork the drain if leader.
   *
   * @return the promise the caller should await (ours, or the one already pending for `key`)
   */
  private def createAndStorePromise(in: In, key: Key): UIO[Promise[Err, Out]] =
    for
      fresh            <- Promise.make[Err, Out]
      sharedAndStarted <- store(key, in, fresh)
      (shared, started) = sharedAndStarted
      _                <- runDrain.interruptible.forkIn(capturedScope).when(started)
    yield shared

  /**
   * One `modify` that installs the key's shared promise (or reuses one a racing first-of-key fiber installed
   * since our `ref.get`, dropping `fresh`).
   *
   * @return the shared promise and whether we were the leader (was idle → must fork the drain)
   */
  private def store(key: Key, in: In, fresh: Promise[Err, Out]): UIO[(Promise[Err, Out], Boolean)] = ref.modify:
    case current @ State.Idle() => current.add(key, in, fresh).pipe((promise, state) => (promise, true) -> state)
    case current                => current.add(key, in, fresh).pipe((promise, state) => (promise, false) -> state)

  /**
   * Drain one FIFO requests of distinct keys at a time until empty, then return to idle.
   *
   * @return unit; carries the drain's error/interrupt in its (forked) fiber, unobserved by callers
   */
  private def runDrain: IO[Err, Unit] =
    ref
      .modify:
        case State.Idle()                              => Nil -> State.Idle()
        case State.InFlight(order, _) if order.isEmpty => Nil -> State.Idle()
        case State.InFlight(order, byKey)              =>
          val (keys, rest) = order.splitAt(drainSize) // oldest distinct keys (FIFO)
          keys.toList.map(byKey) -> State.InFlight(rest, byKey -- keys)
      .flatMap:
        case Nil   => ZIO.unit
        case batch => runBatch(batch) *> runDrain

  /**
   * Run one bulk call and fan the results back to each key's shared promise.
   *
   * Promises are completed from the exit in an *uninterruptible* finalizer, so any `BE` / `E` / lineage
   * mismatch / defect / interrupt reaches all of a key's awaiters. An interrupt then propagates to stop the
   * drain, but a typed failure or defect is swallowed so the remaining queue is still drained.
   *
   * @param requests the `(representative input, shared promise)` pairs to process
   * @return unit; never fails except by propagating an interrupt
   */
  private def runBatch(requests: List[(In, Promise[Err, Out])]): IO[Err, Unit] =
    val batch              = Batch.make(requests)
    val (inputs, promises) = batch.unzip
    logic
      .run(inputs)
      .tap(result => ZIO.fromEither(batch.verifyLineage(result)))
      .onExit:
        case Exit.Success(result) => fulfil(promises, result)
        case Exit.Failure(cause)  => failAll(promises, cause)
      .catchAllCause(cause => if cause.isInterrupted then ZIO.refailCause(cause) else ZIO.unit)
      .unit

  /**
   * Fail every key's shared promise with `cause` (a whole-requests failure, defect, or interrupt).
   *
   * @return unit
   */
  private def failAll(promises: Batch.Success[Promise[Err, Out]], cause: Cause[Err]): UIO[Unit] =
    ZIO.foreachDiscard(promises.values)(_.failCause(cause).unit)

  /**
   * Complete each key's shared promise from its result slot — same lineage (verified) ⇒ slot `i` is key `i`'s
   * outcome, waking all of that key's awaiters.
   *
   * @return unit
   */
  private def fulfil(promises: Batch.Success[Promise[Err, Out]], result: Batch[BE, Out]): UIO[Unit] =
    ZIO.foreachDiscard(result.toList.zip(promises.values)):
      case (Left(err), promise)  => promise.fail(err).unit
      case (Right(out), promise) => promise.succeed(out).unit

  /**
   * Scope finalizer: interrupt every queued key's shared promise so close drops pending work without
   * stranding anyone (the in-flight requests is handled by [[runBatch]]'s `onExit`).
   *
   * @return unit
   */
  private[flow] def abandon: UIO[Unit] =
    ref.get.flatMap:
      case State.InFlight(_, byKey) => ZIO.foreachDiscard(byKey.values)((_, promise) => promise.interrupt.unit)
      case State.Idle()             => ZIO.unit
}


object DeduplicatedSerial:

  /**
   * The dedup state: `Idle`, or `InFlight` holding the FIFO `order` of distinct keys and, per key, its
   * representative input and one shared promise.
   *
   * @tparam Err the error each promise carries
   */
  enum State[Key, Err, In, Out]:
    case Idle()
    case InFlight(order: Queue[Key], byKey: Map[Key, (In, Promise[Err, Out])])

    /**
     * The promise already pending for `key`, if any — the duplicate fast path.
     *
     * @return the shared promise, or `None` if `key` is not pending
     */
    def getPromise(key: Key): Option[Promise[Err, Out]] = this match
      case State.Idle()             => None
      case State.InFlight(_, byKey) => byKey.get(key).map((_, promise) => promise)

    /**
     * Admit `key`, reusing its pending promise if present, else installing `promise`.
     *
     * @return the promise to await (existing or `promise`) and the new state
     */
    def add(key: Key, in: In, promise: Promise[Err, Out]): (Promise[Err, Out], State[Key, Err, In, Out]) =
      getPromise(key) match
        case Some(existing) => existing -> this
        case None           => promise  -> forceAdd(key, in, promise)

    /**
     * Append `key` (new to the state) with its input and promise.
     *
     * @return the state with `key` appended to `order` and `byKey`
     */
    private def forceAdd(key: Key, in: In, promise: Promise[Err, Out]): State[Key, Err, In, Out] = this match
      case Idle()                 => State.InFlight(Queue(key), Map(key -> (in, promise)))
      case InFlight(order, byKey) => State.InFlight(order.enqueue(key), byKey.updated(key, (in, promise)))
