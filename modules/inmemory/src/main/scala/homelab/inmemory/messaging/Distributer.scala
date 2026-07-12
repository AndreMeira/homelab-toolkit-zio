package homelab.inmemory.messaging


import homelab.common.messaging.{ Consumer, Producer }
import zio.*


/**
 * A keyed in-memory channel: `emit` enqueues each value under its partition key, and one `consume`
 * call processes one *round* — at most one element per key — applying `logic` to each in parallel
 * (optionally bounded by `parallelism`). Per-key ordering is preserved (FIFO within a key); distinct
 * keys make progress concurrently. It is both a [[Producer]] and a [[Consumer]]. Never fails.
 *
 * @param parallelism optional cap on concurrent per-round processing; unbounded when `None`
 * @param partitioner derives a value's partition key
 * @param pending     per-key FIFO backlog
 * @param signal      the wake token a blocked consumer parks on
 */
final class Distributer[K, A](
  parallelism: Option[Int],
  partitioner: Distributer.Partitioner[K, A],
  pending: Ref[Map[K, Vector[A]]],
  signal: Ref[Promise[Nothing, Unit]],
) extends Producer[Nothing, A],
    Consumer[Nothing, A] {

  /**
   * Process one round — at most one element per key, in parallel. If nothing is pending, park on the
   * wake token until an `emit` fulfils it, then process the round it woke for.
   *
   * @param logic processes a single value
   * @tparam E2 the widened error, admitting `logic`'s failures
   * @return unit once the round is processed; aborts with `E2` on failure
   */
  override def consume[E2 >: Nothing](logic: A => IO[E2, Unit]): IO[E2, Unit] =
    for {
      sig   <- signal.get
      elems <- poll
      _     <- if elems.isEmpty then awaitSignal(sig, logic) else processElems(elems, logic)
    } yield ()

  /**
   * Enqueue `value` under its key and wake a parked consumer, if any.
   *
   * @param value the value to emit
   * @return unit once enqueued and the signal raised
   */
  override def emit(value: A): IO[Nothing, Unit] =
    addToPending(value) *> wakeUpSignal()

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
   * Apply `logic` to each element of the round in parallel, honouring `parallelism`.
   *
   * @param elems the round's elements (one per key)
   * @param logic processes a single value
   * @tparam E2 the widened error, admitting `logic`'s failures
   * @return unit once every element is processed; aborts with `E2` on the first failure
   */
  private def processElems[E2 >: Nothing](elems: List[A], logic: A => IO[E2, Unit]): IO[E2, Unit] =
    parallelism match
      case Some(n) => ZIO.foreachParDiscard(elems)(logic).withParallelism(n)
      case None    => ZIO.foreachParDiscard(elems)(logic)

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
    partitioner
      .partition(value)
      .flatMap: key =>
        pending.update: pending =>
          pending.updatedWith(key):
            case Some(values) => Some(values :+ value)
            case None         => Some(Vector(value))
}


object Distributer {

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
   * A distributer with an effectful partitioner.
   *
   * @param parallelism optional cap on concurrent per-round processing; unbounded when `None`
   * @param partitioner computes a value's partition key
   * @return the new distributer
   */
  def makeZIO[K, A](parallelism: Option[Int])(partitioner: A => UIO[K]): UIO[Distributer[K, A]] =
    for {
      promise <- Promise.make[Nothing, Unit]
      pending <- Ref.make(Map.empty[K, Vector[A]])
      signal  <- Ref.make(promise)
    } yield new Distributer(parallelism, Partitioner.fromFunction(partitioner), pending, signal)

  /**
   * A distributer with a pure partitioner.
   *
   * @param parallelism optional cap on concurrent per-round processing; unbounded when `None`
   * @param partitioner computes a value's partition key
   * @return the new distributer
   */
  def make[K, A](parallelism: Option[Int])(partitioner: A => K): UIO[Distributer[K, A]] =
    for {
      promise <- Promise.make[Nothing, Unit]
      pending <- Ref.make(Map.empty[K, Vector[A]])
      signal  <- Ref.make(promise)
    } yield new Distributer(parallelism, Partitioner.pure(partitioner), pending, signal)
}
