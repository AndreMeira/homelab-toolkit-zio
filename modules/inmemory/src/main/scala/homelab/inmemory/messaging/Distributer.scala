package homelab.inmemory.messaging


import homelab.common.messaging.{ Consumer, Producer }
import zio.*


/**
 * A keyed in-memory channel — the [[Producer]]/[[Consumer]] face of a [[KeyedQueue]]. `emit` enqueues
 * each value under its partition key (backpressured by the queue's `maxBuffer`); one `consume` call
 * claims one value — the head of one claimable key — runs `logic` on it while holding the key, and
 * frees the key once `logic` settles. At most one value per key is in flight (per-key FIFO), keys are
 * scheduled round-robin, and a key's next value is claimable the instant its previous one finishes.
 *
 * Concurrency is the caller's: `consume` serves one value per call, so fork N `consume` loops (e.g.
 * [[homelab.common.messaging.Processor.Parallel]]) to process N keys at once — an idle loop parks
 * cheaply and the queue wakes exactly one loop per newly claimable key, so large pools carry no
 * thundering-herd cost. `consume` returns once the value is processed; `logic` failures abort the very
 * call that ran them.
 *
 * @param partitioner derives a value's partition key
 * @param queue       the keyed work queue holding all scheduling and blocking semantics
 * @tparam K the partition key
 * @tparam A the value carried
 */
final class Distributer[K, A](
  partitioner: Distributer.Partitioner[K, A],
  queue: KeyedQueue[K, A],
) extends Producer[Nothing, A], Consumer[Nothing, A] {

  /**
   * Claim one value from one claimable key and run `logic` on it, holding the key for the duration; if
   * nothing is claimable, block until a key release or an `emit` publishes one.
   *
   * @param logic processes a single value
   * @tparam E2 the widened error, admitting `logic`'s failures
   * @return unit once one value is processed; aborts with `E2` when `logic` fails
   */
  override def consume[E2 >: Nothing](logic: A => IO[E2, Unit]): IO[E2, Unit] =
    queue.takeWith((_, value) => logic(value))

  /**
   * Enqueue `value` under its partition key, suspending while the queue's buffer is full.
   *
   * @param value the value to emit
   * @return unit once the value is queued
   */
  override def emit(value: A): IO[Nothing, Unit] =
    partitioner.partition(value).flatMap(key => queue.offer(key, value))
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
   * @param maxBuffer   optional backlog cap; `emit` suspends when full, unbounded when `None`
   * @param partitioner computes a value's partition key
   * @return the new distributer; aborts with `KeyedQueue.Error` when `maxBuffer` is non-positive
   */
  def makeZIO[K, A](maxBuffer: Option[Int])(partitioner: A => UIO[K]): IO[KeyedQueue.Error, Distributer[K, A]] =
    KeyedQueue.make[K, A](maxBuffer).map(new Distributer(Partitioner.fromFunction(partitioner), _))

  /**
   * A distributer with a pure partitioner.
   *
   * @param maxBuffer   optional backlog cap; `emit` suspends when full, unbounded when `None`
   * @param partitioner computes a value's partition key
   * @return the new distributer; aborts with `KeyedQueue.Error` when `maxBuffer` is non-positive
   */
  def make[K, A](maxBuffer: Option[Int])(partitioner: A => K): IO[KeyedQueue.Error, Distributer[K, A]] =
    KeyedQueue.make[K, A](maxBuffer).map(new Distributer(Partitioner.pure(partitioner), _))
}
