package homelab.common.messaging


import homelab.common.flow.{ KeyedQueue, Shutdown }
import zio.*


/**
 * The intake side of a topology: a source of values of type `A` that hands each to caller-supplied
 * `logic`. The adapter wraps `logic` with the substrate's commit/ack boundary, so offset and
 * acknowledgement handling never surface here. Because a message may be redelivered on failure,
 * '''`logic` must be idempotent'''.
 *
 * '''A `consume` call may never run `logic`.''' An adapter that fetches with a timeout returns without
 * calling it when the fetch comes back empty, and a wrapper may refuse a value it was handed (a draining
 * shutdown does, handing it back). A
 * caller must not count calls as deliveries, nor rely on `logic` to signal that a call has started.
 *
 * '''Interruption is not a failure.''' An adapter that can hand a message back settles an interrupted `logic`
 * by handing its message back at once, whatever its failure policy says: nobody finished the work, so it must
 * neither be dropped nor wait out a backoff or a lease. Failure handling (retry after a delay, discard,
 * surface) is for work that ran and failed: typed failures and defects. A `logic` that completed just as the
 * interrupt arrived is still settled by its outcome. This is what lets a closing scope interrupt in-flight
 * work and have it redelivered promptly, on every restart. An adapter with no acknowledgement (Core NATS) has nothing to hand back: an
 * interrupted message is lost, as it would be on a failure, since that substrate delivers once and forgets.
 *
 * @tparam E the error consuming aborts with
 * @tparam A the value consumed
 */
trait Consumer[+E, +A] { self =>

  /**
   * Take the next value (or batch, for [[Consumer.Batched]]) and run `logic` on it, within the
   * adapter's commit boundary. One call processes at most one value; a run loop calls it repeatedly. An
   * adapter that fetches with a timeout may return without calling `logic` when its fetch comes back empty,
   * so a caller must not assume a returned call delivered anything.
   *
   * @param logic processes one consumed value
   * @tparam E2 the widened error, admitting `logic`'s failures
   * @return noop once the value is processed and committed, or once an empty fetch has returned; aborts with
   *         `E2` on failure; when interrupted,
   *         hands the value back (where the adapter can) before the interruption carries on
   */
  def consume[E2 >: E](logic: A => IO[E2, Unit]): IO[E2, Unit]

  /**
   * Consume until one value has been delivered: call [[consume]], and call it again for as long as it
   * returns without having handed `logic` a value (an empty fetch). A run loop that means "one value per
   * step" calls this rather than [[consume]], so an empty fetch is retried rather than counted as a step.
   *
   * The delivery is recorded '''before''' `logic` runs, for two reasons. A step after `logic` would be an
   * interruptible gap between its end and the adapter's settlement: a `logic` that completes while an
   * interrupt is pending would be interrupted there, and the adapter would hand back work that was done. And
   * a value whose failure the adapter settles itself (a redelivery or discard policy) has been delivered: it
   * must end this call, not be followed by another fetch.
   *
   * @param logic processes the one consumed value
   * @tparam E2 the widened error, admitting `logic`'s failures
   * @return noop once one value has been delivered and settled; aborts with `E2` when the call delivering it
   *         does, without retrying
   */
  def once[E2 >: E](logic: A => IO[E2, Unit]): IO[E2, Unit] =
    for
      signal <- Promise.make[Nothing, Unit]
      _      <- consume(value => signal.succeed(()) *> logic(value))
                  .repeatUntilZIO(_ => signal.isDone)
    yield ()

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

  /**
   * Wraps this consumer so that a shutdown waits for its in-process work, and it stops taking work once
   * shutdown has begun. Each delivered value runs through `shutdown.graceful`, so:
   *
   *   - a value whose `logic` is already running when `shutdown.await` is called is waited for, and
   *     completes;
   *   - a `consume` call started after `await` parks without reaching this consumer, so nothing more is
   *     pulled or claimed, and the closing scope interrupts it holding nothing;
   *   - a `consume` call that had already reached this consumer, and receives a value after `await`, is
   *     refused: `logic` never runs, the call parks, and the closing scope interrupts it, so the adapter hands
   *     the value back as it does any interrupted one (or loses it, where it cannot hand back).
   *
   * A value is refused where the drain sits in the chain, so anything layered '''inside''' it (a decoding
   * `mapZIO`) runs first. To have every late value refused, decode on top of the drain:
   * `raw.drainLocal(shutdown).mapZIO(decode)`.
   *
   * `logic` must not itself run a section through `shutdown.graceful`: it is one already, and the inner one
   * would park once shutdown began, holding up `await` forever.
   *
   * @param shutdown the shutdown that waits for this consumer's work
   * @return the draining consumer
   */
  def drainLocal(shutdown: Shutdown): Consumer[E, A] = new Consumer[E, A]:
    def consume[E2 >: E](logic: A => IO[E2, Unit]): IO[E2, Unit] =
      shutdown.closing.flatMap: stopped =>
        if stopped then ZIO.never
        else self.consume(value => shutdown.graceful(logic(value)))

  /**
   * Wraps this consumer so that a shutdown waits for its whole `consume` call, the fetch included, and it
   * stops taking work once shutdown has begun. Where [[drainLocal]] waits only for `logic`, this also waits
   * for a fetch that is still open, so a message the server sends while shutdown begins is not lost on its
   * way but delivered, refused and handed back:
   *
   *   - a `consume` call started before `shutdown.await` is waited for, and if its value arrives before
   *     `await`, `logic` runs and completes;
   *   - a value that arrives after `await` is refused (`shutdown.gated`), by interrupting `logic` before it
   *     starts, so the adapter hands it back at once, as it does any interrupted value (or loses it, where it
   *     cannot hand back). The section then ends with the refusal and parks outside its count, so `await` is
   *     not held up and a fail-fast processor does not see it as a failure;
   *   - a `consume` call started after `await` parks without reaching this consumer.
   *
   * `await` waits for every open fetch, so this consumer must make '''one bounded fetch per call''' and
   * return without calling `logic` when it comes back empty, as the JetStream consumers do. A consumer that
   * blocks until a value arrives (an in-memory queue, a fetch loop) would hold up `await` for as long as its
   * intake stays idle; drain those with [[drainLocal]], which loses nothing there.
   *
   * A value is refused where the drain sits in the chain, so anything layered '''inside''' it runs first: a
   * late value that a decoding `mapZIO` below the drain cannot read reaches the adapter's failure policy, as
   * a poison message does at any other time. To have every late value refused, decode on top of the drain:
   * `raw.globalDrain(shutdown).mapZIO(decode)`.
   *
   * `logic` must not itself run a section through `shutdown.graceful`: it runs inside one, and the inner one
   * would park once shutdown began, holding up `await` forever.
   *
   * @param shutdown the shutdown that waits for this consumer's calls
   * @return the draining consumer
   */
  def globalDrain(shutdown: Shutdown): Consumer[E, A] = new Consumer[E, A]:
    def consume[E2 >: E](logic: A => IO[E2, Unit]): IO[E2, Unit] =
      shutdown.graceful:
        self.consume(value => shutdown.gated(logic(value)))
}


object Consumer {

  /**
   * A consumer that delivers batches: one `consume` call processes a whole `Chunk` of `A`. The batch size
   * is fixed where the adapter constructs it, not at the call site — the batching is carried by the type.
   *
   * A name for the plain form, not a subtype: a `map` or `mapZIO` from one chunk to another yields a
   * `Batched` again, since there is no subtype to lose.
   *
   * @tparam E the error consuming aborts with
   * @tparam A the element type of each delivered batch
   */
  type Batched[+E, +A] = Consumer[E, Chunk[A]]

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

  /**
   * A [[Consumer]] over a [[KeyedQueue]] that keeps the key, for `logic` that needs to know which partition
   * a value came from — to load that key's state, or to confine what it does to that key.
   *
   * The key is held for the length of `logic`, as in [[fromKeyedQueue]], so a value and the key it arrived
   * under are the same claim. Never fails.
   *
   * @param queue the keyed queue to pull values from
   * @tparam K the queue's partition key, delivered alongside the value
   * @tparam A the value consumed
   * @return a consumer delivering `queue`'s values paired with their keys, one pair per `consume` call
   */
  def fromKeyedQueueWithKeys[K, A](queue: KeyedQueue[K, A]): Consumer[Nothing, (K, A)] =
    new Consumer[Nothing, (K, A)] {
      def consume[E2 >: Nothing](logic: ((K, A)) => IO[E2, Unit]): IO[E2, Unit] =
        queue.takeWith((key, value) => logic((key, value)))
    }
}
