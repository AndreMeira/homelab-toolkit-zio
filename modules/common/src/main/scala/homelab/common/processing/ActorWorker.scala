package homelab.common.processing


import homelab.common.error.ApplicationError.AdapterError
import homelab.common.flow.KeyedQueue
import homelab.common.messaging.{ Partitioner, Pipe, Producer }
import homelab.common.store.{ Bucket, KeyValueStore }
import zio.*


/**
 * A single stateful actor as a real [[Worker.PerItem]]: it inherits the serial drain
 * ([[homelab.common.processing.Through.PerItem.run run]]), the owned [[Pipe]] intake, and `send` from
 * `Worker`, and adds persisted state plus **out-of-band** replies. The mailbox carries an
 * [[ActorWorker.Envelope]] `(I, Option[Promise])`; the reply is the transition's returned `O`, which
 * [[process]] routes to that promise — so a failed step fails the `ask`, not the drain loop. One entity,
 * `Unit` key; the keyed many-entity variant is [[ActorWorker.Pool]]. Built only via [[ActorWorker.make]].
 *
 * @tparam E the error a transition may fail with
 * @tparam I the message accepted
 * @tparam S the entity state
 * @tparam O the reply produced
 */
class ActorWorker[E, I, S, O] private (
  store: Bucket[S],
  logic: ActorWorker.Logic[E, I, S, O],
  self: ActorWorker.Self[I],
  val input: Pipe[E | AdapterError, ActorWorker.Envelope[E | AdapterError, I, O]],
) extends Worker.PerItem[E | AdapterError, ActorWorker.Envelope[E | AdapterError, I, O]]:

  // Bind `self` once — the transition is reused for every message.
  private val transition = logic.next(self)

  /** Fire-and-forget: enqueue with no reply channel. */
  def fire(message: I): IO[E | AdapterError, Unit] = send(message -> None)

  /** Request/reply: enqueue with a fresh reply promise and await it. */
  def ask(message: I): IO[E | AdapterError, O] =
    for
      promise <- Promise.make[E | AdapterError, O]
      _       <- send(message -> Some(promise))
      reply   <- promise.await
    yield reply

  /**
   * Step the single entity for one message: load state (seeding via [[ActorWorker.Logic.initial]]), run the
   * transition, persist ([[ActorWorker.Step.Continue]]) or evict ([[ActorWorker.Step.Passivate]]), then route
   * the outcome to the message's reply promise. Never fails — a failed step is delivered to the `ask`, so the
   * drain keeps going.
   *
   * @param value the envelope to process
   * @return noop; the outcome goes to the reply promise, not the error channel
   */
  override def process(value: ActorWorker.Envelope[E | AdapterError, I, O]): IO[E | AdapterError, Unit] = {
    for
      state <- store.get.someOrElseZIO(logic.initial(value.message))
      reply <- transition(value.message, state).flatMap:
                 case ActorWorker.Step.Continue(next, out) => store.set(next).as(out)
                 case ActorWorker.Step.Passivate(out)      => store.empty.as(out)
    yield reply
  }.exit.flatMap(exit => ZIO.foreachDiscard(value.replyTo)(_.done(exit)))


object ActorWorker:

  /** A queued message: the input, plus a reply channel when it is an `ask` (absent for a `fire`). */
  type Envelope[E, I, O] = (message: I, replyTo: Option[Promise[E, O]])

  /**
   * The transition's window into the pool it lives in: enqueue a follow-up (`send`), or run background work
   * whose result is delivered back as a message (`pipeToSelf`). Both are non-blocking and route through the
   * mailbox, so a message addresses whatever entity its key designates — for a self-message, this one.
   *
   * @tparam I the message accepted
   */
  trait Self[-I]:
    /** Enqueue `input` onto the mailbox (fire-and-forget). */
    def send(input: I): UIO[Unit]

    /** Run `message` in the background; deliver its result to the mailbox when it completes. */
    def pipeToSelf(message: UIO[I]): UIO[Unit]

  /** One transition's outcome — both branches reply an `O`; `Passivate` also evicts the state. */
  enum Step[+S, +O]:
    case Continue[+S, +O](state: S, reply: O) extends Step[S, O]
    case Passivate[+O](reply: O)              extends Step[Nothing, O]

  /**
   * The behaviour: seed a fresh entity, and step it — replying an `O` and choosing its lifecycle.
   *
   * @tparam E the error a transition may fail with
   * @tparam I the message accepted
   * @tparam S the entity state
   * @tparam O the reply produced
   */
  trait Logic[E, I, S, O]:
    /** The starting state for an entity not yet in the store. */
    def initial(input: I): IO[E, S]

    /** Apply one transition: reply an `O`, and continue with a new state or passivate. */
    def next(self: Self[I]): (input: I, state: S) => IO[E, Step[S, O]]

  /**
   * Build a [[Self]] over `sender` — the send side of an entity's mailbox, over bare messages (the caller's
   * `contramap` wraps each into a fire envelope). `send` emits through it; `pipeToSelf` runs each background
   * pipe in a *child* of the entity's scope: it survives the current turn (`forkIn`), is interrupted if the
   * entity stops (the child is bounded by the parent scope), and closes its child on completion — which
   * self-removes the child from the parent, so a long-lived entity never accumulates finalizers. Built once
   * per entity.
   *
   * @param sender the send side of the mailbox, over bare messages
   * @tparam I the message accepted
   * @return the self handle; needs a [[Scope]] — the parent each pipe forks a child from
   */
  private def makeSelf[I](sender: Producer[Nothing, I]): ZIO[Scope, Nothing, Self[I]] =
    ZIO.scope.map { scope =>
      new Self[I]:
        def send(input: I): UIO[Unit] = sender.emit(input)

        def pipeToSelf(message: UIO[I]): UIO[Unit] =
          scope.fork.flatMap { pipe =>
            message.flatMap(sender.emit).ensuring(pipe.close(Exit.unit)).forkIn(pipe).unit
          }
    }

  /**
   * Build a single [[ActorWorker]] over an unbounded queue. Does not start it — the caller forks
   * [[homelab.common.processing.Through.PerItem.run run]] (`worker.run.forkScoped`) and then `fire`s/`ask`s.
   *
   * @param store the entity's state bucket
   * @param logic the behaviour
   * @return an un-started actor worker; needs a [[Scope]] for the pipe-tracking finalizer
   */
  def make[E, I, S, O](
    store: Bucket[S],
    logic: Logic[E, I, S, O],
  ): ZIO[Scope, Nothing, ActorWorker[E, I, S, O]] =
    for
      queue  <- Queue.unbounded[Envelope[E | AdapterError, I, O]]
      mailbox = Pipe.fromQueue(queue)
      self   <- makeSelf(mailbox.contramap((message: I) => message -> None))
    yield new ActorWorker(store, logic, self, mailbox)

  /**
   * The keyed pool as a real [[Worker.Parallel]] made key-safe *by its intake*: its `input` is a
   * [[Pipe.fromKeyedQueue]], so the `parallelism` concurrent `process` runs land on distinct keys and never
   * race within one. The private constructor + [[Pool.make]] are what guarantee that — a `Pool` can only ever
   * be built over the key-safe pipe. Same out-of-band replies as [[ActorWorker]]; `fire`/`ask` and `self` all
   * emit through that partitioned intake, so a self-message routes by *its own* key (which, for a genuine
   * self-message, is this entity's).
   *
   * @tparam E the error a transition may fail with
   * @tparam I the message accepted
   * @tparam K the partition (entity) key
   * @tparam S the entity state
   * @tparam O the reply produced
   */
  class Pool[E, I, K, S, O] private (
    store: KeyValueStore[K, S],
    logic: Logic[E, I, S, O],
    val input: Pipe[E | AdapterError, Envelope[E | AdapterError, I, O]],
    self: Self[I],
    partition: I => K,
    override val parallelism: Int,
  ) extends Worker.Parallel[E | AdapterError, Envelope[E | AdapterError, I, O]]:

    // Bind `self` once — the transition is reused for every message, of every key.
    private val transition = logic.next(self)

    /** Fire-and-forget onto this message's entity. */
    def fire(message: I): IO[E | AdapterError, Unit] = send(message -> None)

    /** Request/reply onto this message's entity. */
    def ask(message: I): IO[E | AdapterError, O] =
      for
        promise <- Promise.make[E | AdapterError, O]
        _       <- send(message -> Some(promise))
        reply   <- promise.await
      yield reply

    /**
     * A copy that drains with a different `parallelism` — how many *distinct* entities may step at once.
     * Cheap: it shares this pool's mailbox and self, changing only the drain concurrency, so run the copy,
     * not this original (running both would double-drain the one queue).
     *
     * @param n the new concurrency cap; must be positive
     * @return a pool identical to this but for its parallelism
     */
    def withParallelism(n: Int): Pool[E, I, K, S, O] =
      new Pool(store, logic, input, self, partition, n)

    /**
     * Step the addressed entity: the key-safe intake guarantees no other run for the same key is in flight,
     * so `store.get(k)`/`set(k)` are race-free. `self` routes back through the same partitioned intake, so a
     * self-message must carry this entity's key.
     *
     * @param value the envelope to process
     * @return noop; the outcome goes to the reply promise, not the error channel
     */
    override def process(value: Envelope[E | AdapterError, I, O]): IO[E | AdapterError, Unit] = {
      for
        key    = partition(value.message)
        state <- store.get(key).someOrElseZIO(logic.initial(value.message))
        reply <- transition(value.message, state).flatMap:
                   case Step.Continue(next, out) => store.set(key, next).as(out)
                   case Step.Passivate(out)      => store.delete(key).as(out)
      yield reply
    }.exit.flatMap(exit => ZIO.foreachDiscard(value.replyTo)(_.done(exit)))

  object Pool:

    /**
     * Build a keyed [[Pool]] over a [[KeyedQueue]]. Does not start it — the caller forks
     * [[homelab.common.processing.Through.Parallel.run run]] (`pool.run.forkScoped`).
     *
     * @param store       the per-entity state store, keyed by the same `K` the messages partition to
     * @param logic       the behaviour
     * @param parallelism how many entities may step at once (default 1 — serial across keys; raise it, or use
     *                    [[Pool.withParallelism]], for cross-key concurrency); must be positive
     * @param maxBuffer   optional per-key backpressure bound
     * @param key         extracts the partition (entity) key from a message
     * @return an un-started pool; aborts with `AdapterError` if the intake cannot be set up
     */
    def make[E, I, K, S, O](
      store: KeyValueStore[K, S],
      logic: Logic[E, I, S, O],
      parallelism: Int = 1,
      maxBuffer: Option[Int] = None,
    )(using
      key: Partitioner.Key[I] { type Type = K }
    ): ZIO[Scope, AdapterError, Pool[E, I, K, S, O]] =
      for
        queue  <- KeyedQueue
                    .make[K, Envelope[E | AdapterError, I, O]](maxBuffer)
                    .mapError(cause => new AdapterError { override def message: String = cause.message })
        mailbox = Pipe.fromKeyedQueue(queue)(envelope => key.get(envelope.message))
        self   <- makeSelf(mailbox.contramap((message: I) => message -> None))
      yield new Pool(store, logic, mailbox, self, key.get, parallelism)
