package homelab.incubator.processing.v3


import homelab.common.error.ApplicationError.AdapterError
import homelab.common.flow.KeyedQueue
import homelab.common.messaging.{ Consumer, Partitioner }
import homelab.common.processing.Processor
import homelab.common.store.KeyValueStore
import zio.*


/**
 * SKETCH — an **addressable mailbox**: something you `ask` (request/reply) or `tell` (fire-and-forget). A
 * single entity and a keyed pool of entities present the same interface, so the caller never cares whether
 * there is one entity behind it or a whole shard region.
 *
 * @tparam E the error an interaction aborts with
 * @tparam I the message accepted
 * @tparam O the reply an `ask` produces
 */
trait Mailbox[+E, -I, +O]:

  /** Request/reply: enqueue `input` and await its transition's reply. */
  def ask(input: I): IO[E, O]

  /** Fire-and-forget: enqueue `input`; its reply and any error are dropped. */
  def tell(input: I): IO[E, Unit]


object Mailbox:

  /** A queued message: the input, plus a reply channel when it is an `ask` (absent for a `tell`). */
  private[processing] type Envelope[E, I, O] = (I, Option[Promise[E, O]])

  /** Deliver a transition's outcome to its reply channel — or drop it, for a `tell`. */
  private[processing] def reply[E, O](replyTo: Option[Promise[E, O]], result: Exit[E, O]): UIO[Unit] =
    ZIO.foreachDiscard(replyTo)(_.done(result))

  /** The `ask`/`tell` facade over an `offer` that enqueues one envelope onto the backing mailbox. */
  private[processing] def facade[E, I, O](offer: Envelope[E, I, O] => UIO[Any]): Mailbox[E, I, O] =
    new Mailbox[E, I, O]:
      def ask(input: I): IO[E, O] =
        for
          promise <- Promise.make[E, O]
          _       <- offer((input, Some(promise)))
          output  <- promise.await
        yield output

      def tell(input: I): IO[E, Unit] = offer((input, None)).unit


/**
 * SKETCH — the assembled actor, split three ways: pure [[Actor.Logic behaviour]], a
 * [[homelab.common.store.KeyValueStore persistence]] port, and an [[Actor.Runtime execution]] strategy.
 * [[spawn]] wires them into a running [[Mailbox]]. Swap any axis independently: the same logic over an
 * in-memory vs. Postgres store, or the same store under a single entity vs. a keyed pool.
 *
 * The transition core — *load state → step with self → persist or evict → reply* — lives here once, so the
 * runtimes differ only in mailbox, drain concurrency, and keying.
 *
 * @param runtime how the mailbox is drained (single fiber vs. keyed pool) and how a message routes to a key
 * @param state   where each entity's state is persisted, keyed by `K`
 * @param logic   the behaviour: how a fresh entity seeds and how it steps
 * @tparam E the error a transition may fail with
 * @tparam I the message accepted
 * @tparam S the entity state
 * @tparam O the reply produced
 * @tparam K the entity key — `Unit` for a single entity, a partition key for a pool; inferred from `state`
 */
final case class Actor[E, I, S, O, K](
  runtime: Actor.Runtime[I, K],
  state: KeyValueStore[K, S],
  logic: Actor.Logic[E, I, S, O],
):

  /**
   * Wire the three axes into a running mailbox. Each message loads its entity's state (seeding a fresh one
   * via [[Actor.Logic.initial]]), applies [[Actor.Logic.next]] with a self-handle in scope, then persists
   * the new state ([[Actor.Step.Continue]]) or evicts it ([[Actor.Step.Passivate]]), and replies.
   *
   * @return a live mailbox, torn down when the enclosing scope closes; aborts with `AdapterError` if the
   *         runtime cannot be set up. Interactions abort with `E` or `AdapterError`.
   */
  def spawn: ZIO[Scope, AdapterError, Mailbox[E | AdapterError, I, O]] =
    runtime.spawn[E | AdapterError, O] { (k, input, self) =>
      for
        stored <- state.get(k)
        step   <- logic.next(self)(input, stored.getOrElse(logic.initial(input)))
        reply  <- step match
                    case Actor.Step.Continue(next, out) => state.set(k, next).as(out) // persist
                    case Actor.Step.Passivate(out)      => state.delete(k).as(out)    // evict → re-seed
      yield reply
    }


object Actor:

  /**
   * The send-to-self capability handed to a transition. Never fails — enqueuing onto the entity's own
   * mailbox is non-blocking — so there is no error channel and no variance to fight.
   *
   * @tparam I the message accepted
   */
  trait Self[-I]:
    /** Enqueue `input` onto this entity's own mailbox (fire-and-forget). */
    def send(input: I): UIO[Unit]

    /**
     * Run `message` as a background fiber that outlives the current turn, and [[send]] its result to this
     * entity when it completes — the **pipe-to-self** pattern. `message` is total: fold both outcomes of any
     * fallible work into the message yourself, so a failure comes back as *data* (a message) rather than a
     * lost fiber. Because the result is delivered through the mailbox, it is handled in the entity's serial
     * context, not in the background fiber. The fiber is bound to the runtime's scope, so it is interrupted
     * if the actor stops before it completes.
     *
     * @param message the background effect whose result becomes the next message
     * @return noop once the fiber is forked
     */
    def pipeToSelf(message: UIO[I]): UIO[Unit]

  /**
   * One transition's outcome. Both branches reply an `O`; the choice is whether the entity's state lives on
   * or is evicted. There is no self-termination — an entity's lifetime is bound by the [[spawn]] scope, not
   * a step it fires at itself.
   *
   * @tparam S the entity state
   * @tparam O the reply produced
   */
  enum Step[+S, +O]:
    /** Persist `state` and reply `reply`. */
    case Continue[+S, +O](state: S, reply: O) extends Step[S, O]

    /** Reply `reply`, then delete the state — the entity re-seeds on its next message. */
    case Passivate[+O](reply: O) extends Step[Nothing, O]

  /**
   * The actor **behaviour** (pure): how a fresh entity seeds, and how it steps per message. `self` arrives as
   * a plain parameter and the transition is a bare [[zio.IO]] — dependencies are closed over when the logic
   * is built, not threaded through a `ZIO` environment. Background work that must outlive the turn goes
   * through [[Self.pipeToSelf]]. A plain state machine is exactly "always [[Step.Continue]], ignore `self`".
   *
   * @tparam E the error a transition may fail with
   * @tparam I the message accepted
   * @tparam S the entity state
   * @tparam O the reply produced
   */
  trait Logic[E, I, S, +O]:

    /**
     * The starting state for an entity not yet in the store, from the message that first addressed it.
     *
     * @param input the first message seen for this entity
     * @return the seed state
     */
    def initial(input: I): S

    /**
     * Apply one transition: given the current state (and a self-handle to message this entity again),
     * continue with a new state or passivate — replying in either case.
     *
     * @param self  a handle to enqueue follow-up messages onto this same entity
     * @param input the message to process
     * @param state the entity's current state
     * @return the next [[Step]]; fails with `E` if the transition fails
     */
    def next(self: Self[I])(input: I, state: S): IO[E, Step[S, O]]

  /**
   * The **execution** strategy: owns the mailbox, its drain concurrency, message-to-key routing, and reply
   * delivery. Given a `process` step (keyed, with a self-handle), it stands up a running [[Mailbox]]. This is
   * exactly what `StateMachine.serial`/`distributed` do inline — reified as a value you inject.
   *
   * @tparam I the message accepted
   * @tparam K the entity key this runtime routes messages to
   */
  trait Runtime[I, K]:

    /**
     * Stand up a running mailbox: create the intake, drain it under this runtime's concurrency policy, and
     * for each message run `process` with its key and a self-handle, replying with the outcome.
     *
     * @param process the per-message step: `(key, input, self) => reply`
     * @tparam Err the error a step (and thus an interaction) aborts with
     * @tparam O   the reply produced
     * @return a live mailbox; aborts with `AdapterError` if the intake cannot be set up
     */
    def spawn[Err, O](process: (K, I, Self[I]) => IO[Err, O]): ZIO[Scope, AdapterError, Mailbox[Err, I, O]]

  object Runtime:

    /**
     * A [[Self]] factory backed by one shared pipe registry. Every [[Self.pipeToSelf]] it builds forks its
     * work as a daemon — so a finished pipe is pruned by ZIO's supervisor rather than accumulating a
     * finalizer on the runtime's scope — records the fiber while it is in flight, and drops it again when it
     * finishes; a single scope finalizer interrupts whatever is still in flight when the runtime stops.
     * `enqueue` is how the built entity puts a message on its own mailbox — both `send` and a pipe's delivery
     * go through it, so a pool passes a key-targeting `enqueue` per entity while all pipes share one registry.
     *
     * @tparam I the message accepted
     * @return a factory turning an entity's `enqueue` into its [[Self]]; needs a [[Scope]] for the one
     *         shutdown finalizer
     */
    private def selfFactory[I]: ZIO[Scope, Nothing, (I => UIO[Unit]) => Self[I]] =
      for
        pipes <- Ref.make(Map.empty[Long, Fiber.Runtime[Nothing, Unit]])
        ids   <- Ref.make(0L)
        _     <- ZIO.addFinalizer(pipes.get.flatMap(inFlight => Fiber.interruptAll(inFlight.values)))
      yield enqueue =>
        new Self[I]:
          def send(input: I): UIO[Unit] = enqueue(input)

          def pipeToSelf(message: UIO[I]): UIO[Unit] =
            ids.getAndUpdate(_ + 1).flatMap { id =>
              // Daemon-fork so a finished pipe is pruned by ZIO's supervisor; track it only while in flight.
              // (A pipe that finishes before the add re-inserts a dead entry — harmless, cleared at shutdown.)
              message.flatMap(enqueue).ensuring(pipes.update(_ - id)).forkDaemon.flatMap(f => pipes.update(_ + (id -> f)))
            }.unit

    /**
     * A single entity: one unbounded queue drained by one fiber, under the unit key. Setup never fails.
     *
     * @tparam I the message accepted
     * @return a single-entity runtime keyed by `Unit`
     */
    def local[I]: Runtime[I, Unit] =
      new Runtime[I, Unit]:
        def spawn[Err, O](process: (Unit, I, Self[I]) => IO[Err, O]): ZIO[Scope, AdapterError, Mailbox[Err, I, O]] =
          for
            queue  <- Queue.unbounded[Mailbox.Envelope[Err, I, O]]
            mkSelf <- selfFactory[I]
            self    = mkSelf(input => queue.offer((input, None)).unit)
            _      <- Processor
                        .serial(Consumer.fromQueue(queue)) { (input, replyTo) =>
                          process((), input, self).exit.flatMap(Mailbox.reply(replyTo, _))
                        }
                        .forkScoped
          yield Mailbox.facade(queue.offer)

    /**
     * A keyed pool: one entity per partition key of `I`, drained by `parallelism` consumers over a
     * [[KeyedQueue]] — concurrent across keys, never overlapping within one. Each entity's [[Self]] targets
     * its *own* key, so a self-message stays on the same entity.
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
        def spawn[Err, O](
          process: (key.Type, I, Self[I]) => IO[Err, O]
        ): ZIO[Scope, AdapterError, Mailbox[Err, I, O]] =
          for
            queue  <- KeyedQueue
                        .make[key.Type, Mailbox.Envelope[Err, I, O]](maxBuffer)
                        .mapError(cause => new AdapterError { override def message: String = cause.message })
            mkSelf <- selfFactory[I]
            // A key-preserving consumer: Consumer.fromKeyedQueue drops the key, but the pool needs it to
            // route to the right entity state and to build that entity's own Self.
            keyed   = new Consumer[Nothing, (key.Type, Mailbox.Envelope[Err, I, O])]:
                        def consume[E2 >: Nothing](
                          run: ((key.Type, Mailbox.Envelope[Err, I, O])) => IO[E2, Unit]
                        ): IO[E2, Unit] =
                          queue.takeWith((k, envelope) => run((k, envelope)))
            _      <- Processor
                        .parallel(keyed, parallelism) {
                          case (k, (input, replyTo)) =>
                            val self = mkSelf(message => queue.offer(k, (message, None)).unit)
                            process(k, input, self).exit.flatMap(Mailbox.reply(replyTo, _))
                        }
                        .forkScoped
          yield Mailbox.facade(envelope => queue.offer(key.get(envelope._1), envelope))
