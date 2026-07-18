package homelab.inmemory.messaging


import homelab.common.error.ApplicationError
import zio.*
import zio.stm.TSemaphore


/**
 * An effectful keyed work queue: [[offer]] enqueues a value under its key; [[takeWith]] blocks until
 * some key is claimable, claims its head value, runs the given logic on it while holding the key (so at
 * most one value per key is in flight — per-key FIFO), and frees the key once the logic settles.
 *
 * The wake mechanism is a ZIO [[Queue]] of *ready keys*: a key is published exactly once per claimable
 * period (on [[KeyedState]]'s ready transitions) and `ready.take` wakes exactly one blocked taker per
 * published key — no broadcast, no thundering herd, and the woken-then-interrupted races are the ZIO
 * queue's problem, not ours. Fairness is the queue's FIFO: a released key with remaining backlog
 * re-publishes at the tail, behind every other waiting key — round-robin.
 *
 * A `permit` bounds the *pending* backlog: [[offer]] acquires one permit per value (suspending while
 * the buffer is full) and each value releases its permit when claimed, so the bound caps how many
 * un-claimed values may queue, not in-flight ones. An unbounded permit disables backpressure.
 *
 * @param permit backlog bound — one permit acquired per `offer`, released when a value is claimed
 * @param state  the pure keyed-scheduling core: per-key backlog + running set
 * @param ready  the published claimable keys — the wake mechanism and the fairness order
 * @tparam K the partition key
 * @tparam A the value queued
 */
final class KeyedQueue[K, A](
  permit: KeyedQueue.Permit,
  state: Ref[KeyedQueue.KeyedState[K, A]],
  ready: Queue[K],
) {

  /**
   * Acquire a buffer permit (suspending while the buffer is full), enqueue `value` under `key`, and
   * publish the key if it just became claimable. Only the acquire is interruptible: a value that takes
   * a permit is always reflected in the backlog, and a ready transition is always published.
   *
   * @param key   the value's partition key
   * @param value the value to enqueue
   * @return unit once the value is queued and any ready transition published
   */
  def offer(key: K, value: A): UIO[Unit] = ZIO.uninterruptibleMask { restore =>
    restore(permit.acquire) *> state.modify(_.enqueue(key, value)).flatMap(publish(key, _))
  }

  /**
   * Block until a key is claimable, claim its head value, run `logic` on it while holding the key, and
   * free the key once `logic` settles — on success, failure, or interruption. The bracket is the
   * primitive (rather than separate take/release) so there is no window where a claimed key can escape
   * its release: only the park (`ready.take`) and `logic` itself are interruptible.
   *
   * @param logic processes the claimed key and value
   * @tparam R the environment `logic` needs
   * @tparam E the error `logic` aborts with
   * @return unit once one value is processed and its key freed; aborts with `E` when `logic` fails
   */
  def takeWith[R, E](logic: (K, A) => ZIO[R, E, Unit]): ZIO[R, E, Unit] =
    ZIO.uninterruptibleMask { restore =>
      restore(ready.take).flatMap { key =>
        // From here to the `ensuring` attachment we are uninterruptible: the claimed key cannot leak.
        state.modify(_.claim(key)).flatMap { value =>
          permit.release *> restore(logic(key, value)).ensuring(releaseKey(key))
        }
      }
    }

  /**
   * Free a finished key and re-publish it when it still has backlog, so its next value is claimable
   * immediately — at the tail of the ready order, which is what makes scheduling round-robin.
   *
   * @param key the key whose value just finished
   * @return unit once the key is freed and any re-ready transition published
   */
  private def releaseKey(key: K): UIO[Unit] =
    state.modify(_.release(key)).flatMap(publish(key, _))

  /**
   * Publish `key` to the ready queue when `transition` signals it became claimable.
   *
   * @param key        the key to publish
   * @param transition the ready-transition flag from the pure state
   * @return unit once published (or immediately when there was no transition)
   */
  private def publish(key: K, transition: Boolean): UIO[Unit] =
    ZIO.when(transition)(ready.offer(key)).unit
}


object KeyedQueue {

  /**
   * The pure core of the queue's keyed, per-key-serial scheduling: a per-key FIFO backlog (`pending`)
   * and the keys currently being processed (`running`). Readiness — "this key just became claimable" —
   * is *signalled*, not stored: [[enqueue]] and [[release]] report the transition and [[KeyedQueue]]
   * publishes the key to its `ready` queue (whose ordering also decides fairness). The invariants, held
   * here once:
   *
   *   - `enqueue` signals ready exactly on the no-backlog → backlog transition of a non-running key, so
   *     a key is published at most once per claimable period.
   *   - A claimed key is running until [[release]]; [[claim]] on one key never touches another — the
   *     per-key serial gate.
   *   - Values within a key are delivered in emission order (per-key FIFO).
   *
   * All transitions are pure; [[KeyedQueue]] makes them atomic by running each inside a `Ref.modify`.
   *
   * @param pending per-key FIFO backlog; a key maps only while it has queued values
   * @param running keys with a value currently being processed — the per-key serial gate
   * @tparam K the partition key
   * @tparam A the value queued
   */
  final case class KeyedState[K, A](pending: Map[K, Vector[A]], running: Set[K]) {

    /**
     * Append `value` to its key's backlog. The returned flag signals the ready transition: `true`
     * exactly when this is the key's first queued value and it is not running — the driver publishes
     * the key on `true` and does nothing on `false` (already published, or re-published via [[release]]).
     *
     * @param key   the value's partition key
     * @param value the value to enqueue
     * @return whether the key became ready, and the state with the value queued
     */
    def enqueue(key: K, value: A): (Boolean, KeyedState[K, A]) = {
      val becameReady = !pending.contains(key) && !running.contains(key)
      becameReady -> KeyedState(pending.updated(key, pending.getOrElse(key, Vector.empty) :+ value), running)
    }

    /**
     * Claim a ready key: remove its head value from the backlog and mark the key running.
     *
     * Precondition: `key` was published by a ready transition and not claimed since — it has backlog
     * and is not running. The publish-once discipline of [[enqueue]]/[[release]] guarantees this;
     * violating it is a driver bug and throws (a defect, not an error).
     *
     * @param key the key to claim
     * @return the key's head value and the state with it removed and the key marked running
     */
    def claim(key: K): (A, KeyedState[K, A]) = {
      val values     = pending(key)
      val newPending = if values.tail.isEmpty then pending - key else pending.updated(key, values.tail)
      values.head -> KeyedState(newPending, running + key)
    }

    /**
     * Clear a finished key's running mark. The returned flag signals the re-ready transition: `true`
     * exactly when the key still has backlog — the driver re-publishes it (at the back of its wake
     * order, which is what makes scheduling round-robin).
     *
     * @param key the key whose value just finished
     * @return whether the key is ready again, and the state with the key freed
     */
    def release(key: K): (Boolean, KeyedState[K, A]) =
      pending.contains(key) -> KeyedState(pending, running - key)
  }

  object KeyedState:

    /**
     * The empty state — no backlog, nothing running.
     *
     * @tparam K the partition key
     * @tparam A the value queued
     * @return an empty state
     */
    def empty[K, A]: KeyedState[K, A] = KeyedState(Map.empty, Set.empty)

  /**
   * A failure constructing a [[KeyedQueue]]. An [[ApplicationError.ImplementationError]] because it
   * signals a misused API (a violated construction invariant), not a runtime or domain condition.
   */
  enum Error extends ApplicationError.ImplementationError:

    /** `maxBuffer` was set to a non-positive value, which cannot bound a backlog. */
    case NonPositiveBuffer(value: Int)

    override def message: String = this match
      case NonPositiveBuffer(value) => s"maxBuffer must be positive, was $value"

  /**
   * A backlog bound for the queue. `offer` takes one permit per value, a claimed value gives one back;
   * when none remain, `acquire` suspends until a release. An unbounded permit never blocks.
   */
  trait Permit:

    /**
     * Take a permit, suspending while none are available.
     *
     * @return unit once a permit is held
     */
    def acquire: UIO[Unit]

    /**
     * Return a permit, admitting a suspended [[acquire]].
     *
     * @return unit once the permit is released
     */
    def release: UIO[Unit]

  object Permit:

    /**
     * A permit bounded to `maxBuffer` values, or an unbounded (never-blocking) one when `None`.
     *
     * @param maxBuffer the backlog cap; must be positive when set, unbounded when `None`
     * @return the permit; aborts with `Error.NonPositiveBuffer` when `maxBuffer` is non-positive
     */
    def make(maxBuffer: Option[Int]): IO[Error, Permit] = maxBuffer match {
      case Some(n) if n <= 0 => ZIO.fail(Error.NonPositiveBuffer(n))
      case Some(n)           => TSemaphore.makeCommit(n).map(Permit.bounded)
      case None              => ZIO.succeed(Permit.unbounded)
    }

    /** A permit whose `acquire`/`release` are no-ops — no backpressure. */
    def unbounded: Permit = new Permit:
      override def acquire: UIO[Unit] = ZIO.unit
      override def release: UIO[Unit] = ZIO.unit

    /**
     * A permit backed by a semaphore, bounding the backlog to the semaphore's permit count.
     *
     * @param sem the semaphore gating the backlog
     * @return the permit
     */
    def bounded(sem: TSemaphore): Permit = new Permit:
      override def acquire: UIO[Unit] = sem.acquire.commit
      override def release: UIO[Unit] = sem.release.commit

  /**
   * A keyed queue with an optional backlog bound. The ready-key queue is unbounded by design: it holds
   * at most one entry per distinct claimable key, and bounding it could deadlock a key release.
   *
   * @param maxBuffer optional backlog cap; `offer` suspends when full, unbounded when `None`
   * @return the new queue; aborts with `Error` when `maxBuffer` is non-positive
   */
  def make[K, A](maxBuffer: Option[Int]): IO[Error, KeyedQueue[K, A]] =
    for {
      permit <- Permit.make(maxBuffer)
      state  <- Ref.make(KeyedState.empty[K, A])
      ready  <- Queue.unbounded[K]
    } yield new KeyedQueue(permit, state, ready)
}
