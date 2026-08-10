package homelab.common.messaging


import homelab.common.flow.KeyedQueue
import zio.*


/**
 * The intake side of a topology: a source of values of type `A` that hands each to caller-supplied
 * `logic`. The adapter wraps `logic` with the substrate's commit/ack boundary, so offset and
 * acknowledgement handling never surface here. Because a message may be redelivered on failure,
 * '''`logic` must be idempotent'''.
 *
 * @tparam E the error consuming aborts with
 * @tparam A the value consumed
 */
trait Consumer[+E, +A] { self =>

  /**
   * Take the next value (or batch, for [[Consumer.Batched]]) and run `logic` on it, within the
   * adapter's commit boundary. One call processes one noop; a run loop calls it repeatedly.
   *
   * @param logic processes one consumed value
   * @tparam E2 the widened error, admitting `logic`'s failures
   * @return noop once the value is processed and committed; aborts with `E2` on failure
   */
  def consume[E2 >: E](logic: A => IO[E2, Unit]): IO[E2, Unit]

  /**
   * Adapt this consumer to deliver `B` by mapping each consumed `A`.
   *
   * @param fn maps a consumed `A` to the delivered `B`
   * @tparam B the adapted output type
   * @return a consumer that delivers `f(a)` to its logic
   */
  def map[B](fn: A => B): Consumer[E, B] = new Consumer[E, B]:
    def consume[E2 >: E](logic: B => IO[E2, Unit]): IO[E2, Unit] =
      self.consume(a => logic(fn(a)))

  /**
   * Adapt this consumer to deliver `B` by effectfully mapping each consumed `A`.
   *
   * @param fn maps a consumed `A` to `B` inside `IO`
   * @tparam B the adapted output type
   * @tparam E1 the widened error type, admitting failures from `fn`
   * @return a consumer that runs `fn` for each consumed value before passing it to its logic
   */
  def mapZIO[B, E1 >: E](fn: A => IO[E1, B]): Consumer[E1, B] = new Consumer[E1, B]:
    def consume[E2 >: E1](logic: B => IO[E2, Unit]): IO[E2, Unit] =
      self.consume(a => fn(a).flatMap(logic))

  /**
   * Wraps this consumer so that concurrent calls to `consume` are serialised behind a
   * [[zio.Semaphore]]. Useful when the underlying adapter is not thread-safe or when downstream
   * `logic` must never overlap with itself.
   *
   * The permit is held for the whole `consume` call, including any blocking inside it — over a
   * parking source (one that suspends until data arrives) a parked call holds the lock and blocks
   * every other `consume` until it completes. This serialises, it does not fan out: it is not a way
   * to run several consumers concurrently.
   *
   * @return a new consumer that processes one `consume` call at a time
   */
  def serial: UIO[Consumer[E, A]] =
    for lock <- Semaphore.make(1)
    yield new Consumer[E, A]:
      def consume[E2 >: E](logic: A => IO[E2, Unit]): IO[E2, Unit] =
        lock.withPermit(self.consume(logic))
}


object Consumer {

  /**
   * A consumer that delivers messages in batches — one `consume` call processes up to a whole
   * [[List]] of `A`. The batch size is fixed where the adapter constructs it, not at the call site;
   * the batching shape is carried by the type, not by a parameter.
   *
   * @tparam E the error consuming aborts with
   * @tparam A the element type of each delivered batch
   */
  trait Batched[+E, +A] extends Consumer[E, List[A]]:
    def aggregate[B](fn: List[A] => B): Consumer[E, B] = map(fn)

  /**
   * A consumer that runs its logic once with `()` and never fails — a no-op intake.
   *
   * @return a consumer that delivers a single `Unit`
   */
  val unit: Consumer[Nothing, Unit] = new Consumer[Nothing, Unit] {
    def consume[E2 >: Nothing](logic: Unit => IO[E2, Unit]): IO[E2, Unit] = logic(())
  }

  /**
   * A [[Consumer]] over a ZIO [[Queue]]: each `consume` takes the next value — suspending until one is
   * offered — and runs `logic` on it. Never fails.
   *
   * @param queue the queue to pull values from
   * @tparam A the value consumed
   * @return a consumer delivering `queue`'s values, one per `consume` call
   */
  def fromQueue[A](queue: Queue[A]): Consumer[Nothing, A] = new Consumer[Nothing, A] {
    def consume[E2 >: Nothing](logic: A => IO[E2, Unit]): IO[E2, Unit] = queue.take.flatMap(logic)
  }

  /**
   * A [[Consumer]] over a [[KeyedQueue]]: each `consume` claims the next ready key's head value and runs
   * `logic` on it while that key is held — so per-key serialisation and fairness stay the queue's, and this
   * consumer merely delivers the values (the partition key is not surfaced to `logic`). Never fails.
   *
   * @param queue the keyed queue to pull values from
   * @tparam K the queue's partition key, not surfaced to `logic`
   * @tparam A the value consumed
   * @return a consumer delivering `queue`'s values, one per `consume` call
   */
  def fromKeyedQueue[K, A](queue: KeyedQueue[K, A]): Consumer[Nothing, A] = new Consumer[Nothing, A] {
    def consume[E2 >: Nothing](logic: A => IO[E2, Unit]): IO[E2, Unit] =
      queue.takeWith((_, request) => logic(request))
  }
}
