package homelab.incubator.processing.actor.v4


import homelab.common.error.ApplicationError.AdapterError
import homelab.common.flow.KeyedQueue
import homelab.common.messaging.{ Consumer, Partitioner, Producer }
import homelab.common.processing.Processor
import homelab.common.store.KeyValueStore
import zio.*


/**
 * SKETCH — v4, the **in-band reply** variant of the v3 actor. There is no `Mailbox` and no `Envelope`: the
 * queue carries bare `I`, and a request/reply message *carries its own* reply [[zio.Promise]] (Akka-style).
 * The handle is the produce half of a channel — a [[Producer]] — with `ask`/`tell` as extensions; the
 * consume half stays inside the runtime (and is keyed, for a pool). Compared to v3 this drops the `O` type
 * parameter, the reply from [[Step]], and all reply-routing from the runtime — at the cost that a transition
 * must complete its reply promises itself (a failure has nowhere to be routed).
 *
 * @param runtime how the mailbox is drained and how a message routes to a key
 * @param state   where each entity's state is persisted, keyed by `K`
 * @param logic   the behaviour: how a fresh entity seeds and how it steps
 * @tparam E the error a transition may fail with
 * @tparam I the message accepted (carries its own reply promises where it wants a reply)
 * @tparam S the entity state
 * @tparam K the entity key — inferred from `state`
 */
final case class Actor[E, I, S, K](
  runtime: Actor.Runtime[I, K],
  state: KeyValueStore[K, S],
  logic: Actor.Logic[E, I, S],
):

  /**
   * Fork the drain and return the produce side of the mailbox. Each message loads its entity's state
   * (seeding via [[Actor.Logic.initial]]), steps it, and persists ([[Actor.Step.Continue]]) or evicts
   * ([[Actor.Step.Passivate]]). Any reply is the transition's own doing — it completes the promise the
   * message carried.
   *
   * @return the mailbox's produce side (`tell`/`ask` via the extensions below); aborts with `AdapterError`
   *         if the intake cannot be set up
   */
  def spawn: ZIO[Scope, AdapterError, Producer[E | AdapterError, I]] =
    runtime.spawn[E | AdapterError] { (k, input, self) =>
      for
        stored <- state.get(k)
        step   <- logic.next(self)(input, stored.getOrElse(logic.initial(input)))
        _      <- step match
                    case Actor.Step.Continue(next) => state.set(k, next)
                    case Actor.Step.Passivate      => state.delete(k).unit
      yield ()
    }


object Actor:

  /** Send-to-self — unchanged from v3. */
  trait Self[-I]:
    /** Enqueue `input` onto this entity's own mailbox (fire-and-forget). */
    def send(input: I): UIO[Unit]

    /** Run `message` in the background; deliver its result to this entity when it completes. */
    def pipeToSelf(message: UIO[I]): UIO[Unit]

  /**
   * One transition's outcome — no reply here, unlike v3. A reply, if any, is delivered in-band by the
   * transition completing a promise the message carried.
   *
   * @tparam S the entity state
   */
  enum Step[+S]:
    /** Persist `state`. */
    case Continue(state: S)

    /** Delete the state — the entity re-seeds on its next message. */
    case Passivate

  /**
   * The actor behaviour. Like v3 minus `O`: a transition returns only the lifecycle [[Step]], and delivers
   * any reply itself by completing the promise its message carried (see [[ask]]).
   *
   * @tparam E the error a transition may fail with
   * @tparam I the message accepted
   * @tparam S the entity state
   */
  trait Logic[E, I, S]:

    /** The starting state for an entity not yet in the store. */
    def initial(input: I): S

    /**
     * Apply one transition: continue with a new state or passivate, completing any reply promise the message
     * carried along the way.
     *
     * @param self  a handle to enqueue follow-up messages onto this same entity
     * @param input the message to process
     * @param state the entity's current state
     * @return the next [[Step]]; fails with `E` if the transition fails
     */
    def next(self: Self[I])(input: I, state: S): IO[E, Step[S]]

  /**
   * The execution strategy. Its `process` no longer returns a reply — the runtime just drains, steps, and
   * moves on; replies travel in-band.
   *
   * @tparam I the message accepted
   * @tparam K the entity key this runtime routes messages to
   */
  trait Runtime[I, K]:

    /**
     * Stand up a running mailbox and hand back its produce side. Per-message failures are swallowed so the
     * drain survives — an unreplied `ask` is the transition's own bug, not the loop's.
     *
     * @param process the per-message step: `(key, input, self) => unit`
     * @tparam E the error a step may fail with (also the emit error of the returned producer)
     * @return the mailbox's produce side; aborts with `AdapterError` if the intake cannot be set up
     */
    def spawn[E](process: (K, I, Self[I]) => IO[E, Unit]): ZIO[Scope, AdapterError, Producer[E, I]]

  object Runtime:

    /** Same pipe-tracked [[Self]] factory as v3: daemon-forked pipes, pruned in flight, cut off at shutdown. */
    private def selfFactory[I]: ZIO[Scope, Nothing, (I => UIO[Unit]) => Self[I]] =
      for
        pipes <- Ref.make(Map.empty[Long, Fiber.Runtime[Nothing, Unit]])
        ids   <- Ref.make(0L)
        _     <- ZIO.addFinalizer(pipes.get.flatMap(inFlight => Fiber.interruptAll(inFlight.values)))
      yield enqueue =>
        new Self[I]:
          def send(input: I): UIO[Unit] = enqueue(input)

          def pipeToSelf(message: UIO[I]): UIO[Unit] =
            ids
              .getAndUpdate(_ + 1)
              .flatMap { id =>
                message.flatMap(enqueue).ensuring(pipes.update(_ - id)).forkDaemon.flatMap(f => pipes.update(_ + (id -> f)))
              }
              .unit

    /**
     * A single entity: one unbounded queue drained by one fiber, under the unit key.
     *
     * @tparam I the message accepted
     * @return a single-entity runtime keyed by `Unit`
     */
    def local[I]: Runtime[I, Unit] =
      new Runtime[I, Unit]:
        def spawn[E](process: (Unit, I, Self[I]) => IO[E, Unit]): ZIO[Scope, AdapterError, Producer[E, I]] =
          for
            queue  <- Queue.unbounded[I]
            mkSelf <- selfFactory[I]
            self    = mkSelf(input => queue.offer(input).unit)
            _      <- Processor
                        // A failed transition should have replied in-band; swallow so the drain keeps going.
                        .serial(Consumer.fromQueue(queue))(input => process((), input, self).exit.unit)
                        .forkScoped
          yield new Producer[E, I]:
            def emit(value: I): IO[E, Unit] = queue.offer(value).unit

    /**
     * A keyed pool: one entity per partition key of `I`, drained by `parallelism` consumers over a
     * [[KeyedQueue]]. The produce side partitions on the way in; the keyed consume side stays internal.
     *
     * @param parallelism how many entities may step at once; must be positive
     * @param maxBuffer   optional per-key backpressure bound
     * @param key         extracts the partition (entity) key from a message
     * @tparam I the message accepted
     * @return a pool runtime keyed by `key.Type`
     */
    def pool[I](
      parallelism: Int,
      maxBuffer: Option[Int] = None,
    )(using
      key: Partitioner.Key[I]
    ): Runtime[I, key.Type] =
      new Runtime[I, key.Type]:
        def spawn[E](process: (key.Type, I, Self[I]) => IO[E, Unit]): ZIO[Scope, AdapterError, Producer[E, I]] =
          for
            queue  <- KeyedQueue
                        .make[key.Type, I](maxBuffer)
                        .mapError(cause => new AdapterError { override def message: String = cause.message })
            mkSelf <- selfFactory[I]
            keyed   = new Consumer[Nothing, (key.Type, I)]:
                        def consume[E2 >: Nothing](run: ((key.Type, I)) => IO[E2, Unit]): IO[E2, Unit] =
                          queue.takeWith((k, input) => run((k, input)))
            _      <- Processor
                        .parallel(keyed, parallelism) {
                          case (k, input) =>
                            val self = mkSelf(message => queue.offer(k, message).unit)
                            process(k, input, self).exit.unit
                        }
                        .forkScoped
          yield new Producer[E, I]:
            def emit(value: I): IO[E, Unit] = queue.offer(key.get(value), value).unit

  /**
   * The two ways to address the mailbox, over its produce side. `tell` is fire-and-forget; `ask` mints a
   * reply promise, wraps it in a message via `reply`, sends it, and awaits — the transition completes the
   * promise. The reply's error `E2` is the protocol's own (often `Nothing`), independent of the mailbox's
   * emit error `E`.
   */
  extension [E, I](mailbox: Producer[E, I])
    /** Fire-and-forget. */
    def tell(message: I): IO[E, Unit] = mailbox.emit(message)

    /**
     * Request/reply: send a reply-carrying message and await the promise the transition completes.
     *
     * @param reply builds the message to send from the reply promise it must complete
     * @tparam E2 the reply's own error
     * @tparam A  the reply value
     * @return the reply; aborts with `E` if the send fails or `E2` if the transition fails the promise
     */
    def ask[E2, A](reply: Promise[E2, A] => I): IO[E | E2, A] =
      for
        promise <- Promise.make[E2, A]
        _       <- mailbox.emit(reply(promise))
        answer  <- promise.await
      yield answer
