package homelab.inmemory.messaging


import homelab.common.error.ApplicationError
import homelab.common.messaging.{ Consumer, Producer }
import zio.*
import zio.stm.TSemaphore


/**
 * A keyed in-memory channel: `emit` enqueues each value under its partition key, and one `consume`
 * call processes one *round* — at most one element per key — applying `logic` to each in parallel
 * (optionally bounded by `parallelism`). Per-key ordering is preserved (FIFO within a key); distinct
 * keys make progress concurrently. It is both a [[Producer]] and a [[Consumer]]. Never fails.
 *
 * A `permit` bounds the *pending* backlog: `emit` acquires one permit per value (suspending while the
 * buffer is full) and each value releases its permit when dequeued for processing — so the bound caps
 * how many un-consumed values may queue, not in-flight ones. An unbounded permit disables backpressure.
 *
 * @param parallelism optional cap on concurrent per-round processing; unbounded when `None`
 * @param permit      backlog bound — one permit acquired per `emit`, released when a value is dequeued
 * @param partitioner derives a value's partition key
 * @param pending     per-key FIFO backlog
 * @param signal      the wake token a blocked consumer parks on
 */
final class Distributer[K, A](
  parallelism: Option[Int],
  permit: Distributer.Permit,
  partitioner: Distributer.Partitioner[K, A],
  pending: Ref[Map[K, Vector[A]]],
  signal: Ref[Promise[Nothing, Unit]],
) extends Producer[Nothing, A], Consumer[Nothing, A] {

  /**
   * Process one round — at most one element per key, in parallel. If nothing is pending, park on the
   * wake token until an `emit` fulfils it, then process the round it woke for.
   *
   * Dequeuing (`poll`) and releasing the dequeued elements' permits run uninterruptibly as a single
   * step, so exactly one permit is freed for each value removed from the backlog. Permits are released
   * at dequeue, not after processing, so `emit` is throttled by backlog depth rather than by `logic`.
   *
   * @param logic processes a single value
   * @tparam E2 the widened error, admitting `logic`'s failures
   * @return unit once the round is processed; aborts with `E2` on failure
   */
  override def consume[E2 >: Nothing](logic: A => IO[E2, Unit]): IO[E2, Unit] =
    for {
      // Load-bearing: capture `sig` BEFORE `poll`. If reordered, an `emit` landing between the poll
      // and the get completes an uncaptured token, and we park on the fresh one — lost wake-up.
      sig   <- signal.get
      elems <- poll.tap(ZIO.foreachDiscard(_)(_ => permit.release)).uninterruptible
      _     <- if elems.isEmpty then awaitSignal(sig, logic) else processElems(elems, logic)
    } yield ()

  /**
   * Acquire a buffer permit (suspending while the buffer is full), enqueue `value` under its key, and
   * wake a parked consumer, if any. Only the acquire is interruptible: a value that takes a permit is
   * always reflected in the backlog.
   *
   * @param value the value to emit
   * @return unit once the permit is held, the value enqueued, and the signal raised
   */
  override def emit(value: A): IO[Nothing, Unit] = ZIO.uninterruptibleMask { restore =>
    // Load-bearing: `addToPending` BEFORE `wakeUpSignal`. A woken consumer's re-poll must see this
    // value; if the signal were raised first, the poll could miss it and park despite a full backlog.
    restore(permit.acquire) *> addToPending(value) *> wakeUpSignal()
  }

  /**
   * Park on the token captured *before* `poll` and, once an `emit` fulfils it, re-enter `consume`.
   * Capturing before polling is what prevents a lost wake-up: an `emit` landing after our `poll` but
   * before our `await` fulfils the token current at swap time, so `await` returns rather than parking
   * on the new token.
   *
   * @param signal the wake token captured before polling
   * @param logic  processes a single value
   * @tparam E2 the widened error, admitting `logic`'s failures
   * @return unit once a woken round is processed; aborts with `E2` on failure
   */
  private def awaitSignal[E2 >: Nothing](
    signal: Promise[Nothing, Unit],
    logic: A => IO[E2, Unit],
  ): IO[E2, Unit] = signal.await *> consume(logic)

  /**
   * Apply `logic` to each element of the round in parallel, honouring `parallelism`. Permits are
   * already released at dequeue, so processing itself holds none.
   *
   * @param elems the round's elements (one per key)
   * @param logic processes a single value
   * @tparam E2 the widened error, admitting `logic`'s failures
   * @return unit once every element is processed; aborts with `E2` on the first failure
   */
  private def processElems[E2 >: Nothing](elems: List[A], logic: A => IO[E2, Unit]): IO[E2, Unit] =
    parallelism match
      case None    => ZIO.foreachParDiscard(elems)(logic)
      case Some(n) => ZIO.foreachParDiscard(elems)(logic).withParallelism(n)

  /**
   * Take one element per non-empty key (the round), leaving each key's tail behind.
   *
   * @return the round's elements, one per key that had backlog
   */
  private def poll: UIO[List[A]] =
    pending.modify: pending =>
      val heads = pending.values.flatMap(_.headOption).toList
      val tails = pending.collect { case (key, _ +: tail) if tail.nonEmpty => key -> tail }
      heads -> tails

  /**
   * Fulfil the current wake token and install a fresh one, so parked consumers proceed.
   *
   * @return unit once the token is swapped and the old one fulfilled
   */
  private def wakeUpSignal(): UIO[Unit] =
    for {
      promise <- Promise.make[Nothing, Unit]
      old     <- signal.modify(_ -> promise)
      _       <- old.succeed(())
    } yield ()

  /**
   * Append `value` to its key's FIFO backlog.
   *
   * @param value the value to enqueue
   * @return unit once enqueued
   */
  private def addToPending(value: A): UIO[Unit] =
    partitioner.partition(value).flatMap { key =>
      pending.update: pending =>
        pending.updatedWith(key):
          case Some(values) => Some(values :+ value)
          case None         => Some(Vector(value))
    }
}


object Distributer {

  /**
   * A failure constructing a [[Distributer]]. An [[ApplicationError.ImplementationError]] because it
   * signals a misused API (a violated construction invariant), not a runtime or domain condition.
   */
  enum Error extends ApplicationError.ImplementationError:

    /** `maxBuffer` was set to a non-positive value, which cannot bound a backlog. */
    case NonPositiveBuffer(value: Int)

    /** `parallelism` was set to a non-positive value, which cannot run a round. */
    case NonPositiveParallelism(value: Int)

    override def message: String = this match
      case NonPositiveBuffer(value)      => s"maxBuffer must be positive, was $value"
      case NonPositiveParallelism(value) => s"parallelism must be positive, was $value"

  /**
   * Derives a value's partition key — the routing identity that co-locates related messages.
   *
   * @tparam K the partition key
   * @tparam V the value partitioned
   */
  trait Partitioner[K, V] {

    /**
     * The partition key of `value`.
     *
     * @param value the value to key
     * @return its partition key
     */
    def partition(value: V): UIO[K]
  }

  object Partitioner {

    /**
     * A partitioner from an effectful key function.
     *
     * @param fn computes a value's key
     * @return the partitioner
     */
    def fromFunction[K, V](fn: V => UIO[K]): Partitioner[K, V] = fn(_)

    /**
     * A partitioner from a pure key function.
     *
     * @param fn computes a value's key
     * @return the partitioner
     */
    def pure[K, V](fn: V => K): Partitioner[K, V] = value => ZIO.succeed(fn(value))
  }

  /**
   * A backlog bound for the distributer. `emit` takes one permit per value, a dequeued value gives
   * one back; when none remain, `acquire` suspends until a release. An unbounded permit never blocks.
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
   * Reject a non-positive `parallelism`, which cannot run a round.
   *
   * @param parallelism the configured concurrency cap, or `None` for unbounded
   * @return unit when `parallelism` is absent or positive; aborts with `Error.NonPositiveParallelism`
   *         when it is non-positive
   */
  private def validateParallelism(parallelism: Option[Int]): IO[Error, Unit] =
    parallelism match
      case Some(n) if n <= 0 => ZIO.fail(Error.NonPositiveParallelism(n))
      case _                 => ZIO.unit

  /**
   * A distributer with an effectful partitioner.
   *
   * @param parallelism optional cap on concurrent per-round processing; unbounded when `None`
   * @param maxBuffer   optional backlog cap; `emit` suspends when full, unbounded when `None`
   * @param partitioner computes a value's partition key
   * @return the new distributer; aborts with `Error` when `parallelism` or `maxBuffer` is non-positive
   */
  def makeZIO[K, A](parallelism: Option[Int], maxBuffer: Option[Int])(partitioner: A => UIO[K]): IO[Error, Distributer[K, A]] =
    for {
      _       <- validateParallelism(parallelism)
      permit  <- Permit.make(maxBuffer)
      promise <- Promise.make[Nothing, Unit]
      pending <- Ref.make(Map.empty[K, Vector[A]])
      signal  <- Ref.make(promise)
    } yield new Distributer(parallelism, permit, Partitioner.fromFunction(partitioner), pending, signal)

  /**
   * A distributer with a pure partitioner.
   *
   * @param parallelism optional cap on concurrent per-round processing; unbounded when `None`
   * @param maxBuffer   optional backlog cap; `emit` suspends when full, unbounded when `None`
   * @param partitioner computes a value's partition key
   * @return the new distributer; aborts with `Error` when `parallelism` or `maxBuffer` is non-positive
   */
  def make[K, A](parallelism: Option[Int], maxBuffer: Option[Int])(partitioner: A => K): IO[Error, Distributer[K, A]] =
    for {
      _       <- validateParallelism(parallelism)
      permit  <- Permit.make(maxBuffer)
      promise <- Promise.make[Nothing, Unit]
      pending <- Ref.make(Map.empty[K, Vector[A]])
      signal  <- Ref.make(promise)
    } yield new Distributer(parallelism, permit, Partitioner.pure(partitioner), pending, signal)

}
