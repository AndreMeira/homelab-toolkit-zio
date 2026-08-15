package homelab.incubator.processing.v5


import homelab.common.error.ApplicationError.AdapterError
import homelab.common.store.Bucket
// Explicit: `zio.Runtime` would otherwise win over this package's own via the wildcard below.
import homelab.incubator.processing.v5.Runtime
import zio.*


trait Actor[E, I, S, O] {

  /**
   * The starting state for an entity the store has never seen (or has since passivated).
   *
   * @param message the first message routed to this entity — the one that triggered seeding
   * @return the initial state; aborts with `E` if seeding fails
   */
  def init(message: I): IO[E, S]

  /**
   * Build this entity's transition, binding [[Actor.Self]] once so it can be closed over. The returned
   * function is invoked per message: it replies an `O` and chooses the entity's lifecycle
   * ([[Actor.Next.Continue]] to persist a new state, [[Actor.Next.Passivate]] to evict it).
   *
   * @param self this entity's handle onto its own mailbox — `send` a follow-up, or `pipeToSelf` background work
   * @return the transition `(message, state) => Next`; aborts with `E` when the step fails
   */
  def next(self: Actor.Self[I]): (input: I, state: S) => IO[E, Actor.Next[S, O]]
}


object Actor {

  /**
   * The refusal a handle gives once its entity is [[Next.Done]] — the entity it named is finished, and the
   * message was not processed. It is not a failure of the behaviour (nothing went wrong) but a fact about the
   * handle, which is why it rides in the handle's error channel and never in an [[Actor]]'s `E`.
   *
   * A caller that can obtain a fresh entity should do so and retry; a caller holding the only handle there
   * ever was has simply reached the end of it.
   */
  case object Terminated
  type Terminated = Terminated.type

  /**
   * Start `behaviour` over `state` on a [[Runtime]] of its own, provisioned in the calling scope — the root
   * case, for an entity that answers to nobody. Nothing runs until the first message: the entity is dormant,
   * and closing the scope takes it and its runtime down together.
   *
   * Where one owner runs several entities, give them a single runtime and [[spawnIn]] instead: a runtime is
   * built to carry many, and a private one per entity spends a whole keyed executor on a single queue.
   *
   * `E` must admit `AdapterError` because the state store's failures surface through the same channel as
   * the transition's own — the entity cannot tell a caller "the step was fine but the state did not save".
   *
   * `state` is where the entity keeps itself between messages. Omit it and it gets a private in-process
   * slot, which is what you want unless the state must outlive the process or be shared — pass `Some(slot)`
   * to put it in a database, or to hand the slot to a replacement entity later. Note a slot is a *location*,
   * not a saved copy: [[Next.Done]] empties it, so a replacement over the same slot seeds afresh through
   * [[Actor.init]] unless the behaviour wrote what it wants to keep somewhere else first (or the store's
   * `delete` is deliberately a no-op — what `set`/`delete` mean is the store's business, not ours).
   *
   * @param behaviour the transition to step this entity with
   * @param state     the slot this entity's state lives in; `None` allocates a private in-memory one
   * @tparam E the error a transition may fail with — must admit the store's `AdapterError`
   * @tparam I the message accepted
   * @tparam S the entity state
   * @tparam O the reply produced
   * @return the live entity, inert until its first message
   */
  def spawn[E >: AdapterError, I, S, O](
    behaviour: Actor[E, I, S, O],
    state: Option[Bucket[S]] = None,
  ): ZIO[Scope, Nothing, Running[E, I, O]] =
    Runtime.make.flatMap(runtime => spawnIn(runtime)(behaviour, state)(ZIO.unit))

  /**
   * [[spawn]], with an owner watching the entity's lifecycle: `onDone` runs when it finishes, so the watcher
   * can release what it held or forget it entirely. One watcher, fixed at spawn — this is a hook, not a
   * subscription.
   *
   * It runs once, because an entity finishes once: after the state slot is emptied, before the reply is
   * delivered — so a returning `ask` means the entity is already finished and cleaned up — and on the
   * entity's own drain fiber, so nothing else is processed while it is in flight.
   *
   * @param behaviour the transition to step this entity with
   * @param state     the slot this entity's state lives in; `None` allocates a private in-memory one
   * @param onDone    run when the entity finishes, before its last reply is delivered
   * @tparam E the error a transition may fail with — must admit the store's `AdapterError`
   * @tparam I the message accepted
   * @tparam S the entity state
   * @tparam O the reply produced
   * @return the live entity, inert until its first message
   */
  def spawnWatched[E >: AdapterError, I, S, O](
    behaviour: Actor[E, I, S, O],
    state: Option[Bucket[S]] = None,
  )(
    onDone: UIO[Unit]
  ): ZIO[Scope, Nothing, Running[E, I, O]] =
    Runtime.make.flatMap(runtime => spawnIn(runtime)(behaviour, state)(onDone))

  /**
   * Start `behaviour` on `runtime`, sharing it with whatever else that runtime carries. The entity takes a
   * serialisation key of its own and a scope of its own, and **retires itself**: finishing closes that scope,
   * releasing its background work and its finalizers without anybody having to stop it.
   *
   * This is the form for a pool, or any owner running many entities: one runtime, one key and one scope
   * each, and a footprint proportional to the entities that are live rather than to every one ever started.
   *
   * @param runtime   the substrate to run on
   * @param behaviour the transition to step this entity with
   * @param state     the slot this entity's state lives in; `None` allocates a private in-memory one
   * @param onDone    run when the entity finishes, before its last reply is delivered
   * @tparam E the error a transition may fail with — must admit the store's `AdapterError`
   * @tparam I the message accepted
   * @tparam S the entity state
   * @tparam O the reply produced
   * @return the live entity, inert until its first message
   */
  def spawnIn[E >: AdapterError, I, S, O](
    runtime: Runtime
  )(
    behaviour: Actor[E, I, S, O],
    state: Option[Bucket[S]] = None,
  )(
    onDone: UIO[Unit]
  ): UIO[Running[E, I, O]] =
    for
      owned    <- runtime.child
      held     <- state.fold(Bucket.inmemory[S])(ZIO.succeed(_))
      finished <- Promise.make[Nothing, O]
      // An entity torn down with the runtime never finishes, so release anyone waiting on it rather than
      // leaving them parked for the life of their fiber. Finishing normally closes this scope itself.
      _        <- owned.addFinalizer(finished.interrupt)
    yield new Running.Live(runtime, new Runtime.Key, owned, behaviour, held, onDone, finished)


  /**
   * A live entity: an [[Actor]] bound to its state and to the [[Runtime]] that serialises it. This is the
   * handle a caller holds — messages go in, replies come out, and the behaviour, the state and the scheduling
   * are all closed over. Every message routed through one `Running` is processed strictly in order.
   *
   * **A handle is alive or it is not.** When a transition returns [[Next.Done]] the entity finishes: its state
   * is dropped and this handle stops accepting work, failing every later message with [[Terminated]] rather
   * than quietly starting a new life. Getting another entity means spawning one — which is what lets a
   * pool such as [[Distributed]] hand out work safely, since a finished entity can never silently start a
   * second life for a key that has already moved on.
   *
   * @tparam E the error a transition may fail with — also carries the state store's `AdapterError`
   * @tparam I the message accepted
   * @tparam O the reply produced
   */
  trait Running[+E, -I, +O]:

    /**
     * Send `message` and await this entity's reply.
     *
     * @param message the message to process
     * @return the transition's reply; aborts with `E` if the step, its seeding, or its persistence fails,
     *         with [[Terminated]] if the entity had already finished, and is interrupted if the runtime shuts
     *         down while this message is still queued
     */
    def ask(message: I): IO[E | Terminated, O]

    /**
     * Send `message` without awaiting the step — fire-and-forget. A failing step is swallowed rather than
     * surfaced here; it fails nothing and leaves the entity's backlog intact.
     *
     * Refusal is reported *best-effort*: an entity already finished when the call is made aborts with
     * [[Terminated]], but one that finishes in the moment between queueing and claiming drops the message
     * silently, because nothing is waiting to be told. Where that matters, [[ask]] and discard the reply.
     *
     * @param message the message to process
     * @return noop once queued, not once processed; aborts with [[Terminated]] if the entity had finished
     */
    def send(message: I): IO[E | Terminated, Unit]

    /**
     * Send a message carrying a reply promise, and await *that* promise rather than the step's own reply.
     * `fn` receives a fresh promise to embed in the message; the entity completes it whenever the answer is
     * ready — possibly several steps later, or from `pipeToSelf` work. The reply-to pattern, for answers that
     * do not line up one-to-one with the message that asked for them.
     *
     * The carrying message is [[ask]]ed rather than [[send]]t, so an entity that is finished (or that fails
     * on this step) fails the caller here instead of leaving it waiting on a promise nobody will ever
     * complete.
     *
     * @param fn builds the message around the promise the caller will await
     * @tparam E2 the promise's error type — a supertype of `E`, since a covariant `E` cannot sit in
     *            `Promise`'s invariant slot; callers normally let it infer to `E`
     * @tparam A the value the entity promises to deliver
     * @return whatever the entity completes the promise with; aborts with `E` if it fails it, or with
     *         [[Terminated]] if the entity had already finished
     */
    def expect[E2 >: E, A](fn: Promise[E2, A] => I): IO[E2 | Terminated, A]

    /**
     * Wait for this entity to finish, and take the reply its final step produced — the [[Next.Done]] value.
     * Never fails: a failing step does not end an entity, so [[Actor.Next.Done]] is the only way here. It is
     * interrupted if the runtime's scope closes first, since an entity torn down that way never finishes.
     *
     * This is the observer's hook, and it runs *after* the world can see the entity as gone. Anything that
     * must act before that — a routing table evicting the key — belongs in [[Actor.spawnWatched]] instead.
     *
     * @return the final reply, once the entity has finished
     */
    def await: UIO[O]

    /**
     * Whether this entity has already finished — a snapshot taken now, not a subscription. Useful to skip
     * work for an entity known to be gone; racy as a guard, since it can become true a moment later. The
     * reliable signal is [[Terminated]] coming back from [[ask]] or [[send]].
     *
     * @return `true` once the entity has finished
     */
    def terminated: UIO[Boolean]

  object Running:

    /**
     * A single entity — the degenerate one-key case of a keyed pool. Its `Runtime` key is a fresh token
     * created per instance rather than anything derived from the message, so *every* message this entity
     * receives shares one serialisation key and they queue behind each other.
     *
     * `finished` is completed only by the step that ends the entity and read only on the drain fiber, so the
     * check and the completion are serialised by the very machinery being guarded: a message queued a moment
     * before the end is refused, one queued a moment after is refused, and there is no window in between. A
     * promise rather than a flag so the same latch that refuses work can also be awaited.
     *
     * @param runtime   the substrate this entity's steps are serialised on
     * @param key       this entity's serialisation key — allocated for it, never shared
     * @param owned     this entity's own scope: its background work, its finalizers, closed when it finishes
     * @param behaviour the transition to step with
     * @param state     the slot the entity's state lives in — empty means "not yet seeded"
     * @param onDone    the watcher's hook, run once when the entity finishes
     * @param finished  completed with the final reply when the entity ends; every message claimed afterwards
     *                  is refused
     */
    private[Actor] final class Live[E >: AdapterError, I, S, O](
      runtime: Runtime,
      key: Runtime.Key,
      owned: Scope.Closeable,
      behaviour: Actor[E, I, S, O],
      state: Bucket[S],
      onDone: UIO[Unit],
      finished: Promise[Nothing, O],
    ) extends Running[E, I, O]:
      private type Err = E | Terminated

      def ask(message: I): IO[Err, O] =
        for
          promise <- Promise.make[Err, O]
          _       <- submit(message, Some(promise))
          reply   <- promise.await
        yield reply

      // Best-effort refusal: reads `finished` on the caller's fiber so `send` stays fire-and-forget. A
      // message that races the ending step is queued, then refused by the drain with no one to tell — the
      // price of not awaiting. Use `ask` where losing that message would matter.
      def send(message: I): IO[Err, Unit] =
        finished.isDone.flatMap:
          case true  => ZIO.fail(Terminated)
          case false => submit(message, None)

      def expect[E2 >: E, A](fn: Promise[E2, A] => I): IO[E2 | Terminated, A] =
        for
          promise <- Promise.make[E2, A]
          _       <- ask(fn(promise)) // fails here rather than waiting on a promise nobody will complete
          result  <- promise.await
        yield result

      /**
       * Queue one message on this entity's runtime key, with a reply channel when the caller wants one.
       * Queueing is unconditional even once [[finished]] is set — the refusal is issued by the drain, where
       * it cannot race the step that ends the entity.
       *
       * @param message the message to process
       * @param replyTo the promise to settle with the step's outcome, or `None` for fire-and-forget
       * @return noop once queued
       */
      private def submit(message: I, replyTo: Option[Promise[Err, O]]): UIO[Unit] =
        runtime.submit(key)(task(message, replyTo))

      /**
       * The [[Runtime.Task]] for one message: refuse it outright if the entity has finished, else [[step]]
       * wrapped so it can never fail the drain. The reply is routed from an *uninterruptible* `onExit`, so a
       * failed, defective or interrupted step settles the caller's promise rather than the drain; a typed
       * failure is then swallowed to keep the backlog moving, and only an interrupt is re-raised, to stop the
       * drain as the scope closes. `drop` covers the message that never ran at all.
       *
       * @param message the message to process
       * @param replyTo the promise to settle, if any
       * @return the task the runtime will run — or drop — for this message
       */
      private def task(message: I, replyTo: Option[Promise[Err, O]]): Runtime.Task =
        new Runtime.Task:
          def run: UIO[Unit] =
            finished.isDone.flatMap:
              case true  => ZIO.foreachDiscard(replyTo)(_.fail(Terminated).unit)
              case false =>
                step(message)
                  .onExit(exit => ZIO.foreachDiscard(replyTo)(_.done(exit)))
                  // `stripFailures` keeps the interrupt but drops the typed failure, which is what makes this
                  // a `UIO`: the step's `E` has already gone to the promise and must not reach the drain.
                  .catchAllCause(cause =>
                    if cause.isInterrupted then ZIO.refailCause(cause.stripFailures) else ZIO.unit
                  )
                  // Retire *after* the reply is out, never during the step: closing the scope stops this
                  // entity's background work, and a caller owed the final answer must have it first. The
                  // drain fiber survives closing its own scope — ZIO's fork finalizer skips self-interrupt —
                  // so it lives on to refuse whatever is queued behind.
                  .zipRight(ZIO.whenZIO(finished.isDone)(owned.close(Exit.unit)))
                  .unit

          def drop: UIO[Unit] = ZIO.foreachDiscard(replyTo)(_.interrupt.unit)

      /**
       * Step the entity once: load its state (seeding via [[Actor.init]] when the slot is empty), run the
       * transition, then either persist the new state ([[Next.Continue]]) or end the entity
       * ([[Next.Done]]) — emptying the slot, marking it [[finished]], and running `onDone`.
       *
       * The hook runs on this drain fiber, so no message is processed while it is in flight and the last
       * reply waits on it; work that must not hold the entity up belongs in [[Runtime.background]].
       *
       * @param message the message to process
       * @return the transition's reply; aborts with `E` if seeding, the step, or the store fails
       */
      private def step(message: I): IO[E, O] =
        for
          current <- state.get.someOrElseZIO(behaviour.init(message))
          next    <- behaviour.next(self)(message, current)
          reply   <- next match
                       case Next.Continue(updated, out) => state.set(updated).as(out)
                       case Next.Done(out)              => state.empty *> finished.succeed(out) *> onDone.as(out)
        yield reply

      /**
       * The handle handed to the transition. A self-`send` queues on the same key, so it lands behind
       * whatever this entity is already doing rather than re-entering the current step; `pipeToSelf` forks
       * into the runtime's scope and queues its result the same way. Both ignore refusal: a transition that
       * ends its entity and had already queued itself a follow-up meant the follow-up to be moot.
       */
      def await: UIO[O] = finished.await

      def terminated: UIO[Boolean] = finished.isDone

      private val self: Self[I] = new Self[I]:
        def send(input: I): UIO[Unit]              = submit(input, None)
        def pipeToSelf(message: UIO[I]): UIO[Unit] = message.flatMap(send).forkIn(owned).unit

  trait Self[-I]:
    /** Enqueue `input` onto this entity's own mailbox (fire-and-forget). */
    def send(input: I): UIO[Unit]

    /** Run `message` in the background; deliver its result to this entity when it completes. */
    def pipeToSelf(message: UIO[I]): UIO[Unit]

  /**
   * One transition's outcome. Both branches carry the [[reply]] the caller receives; they differ only in what
   * becomes of the entity's state — [[Next.Continue]] keeps it, [[Next.Done]] drops it and ends the entity.
   *
   * @tparam S the entity state, carried only by [[Next.Continue]]
   * @tparam O the reply produced, carried by both
   */
  sealed trait Next[+S, +O]:
    /** The value sent back to whoever asked — produced whether or not the entity survives the step. */
    def reply: O

  object Next:

    /**
     * Stay alive with `state` — the ordinary step. The next message for this entity is handed exactly this
     * state, with no call to [[Actor.init]].
     *
     * @param state the state the entity carries into its next message
     * @param reply the value sent back for the message just handled
     * @tparam S the entity state
     * @tparam O the reply produced
     */
    case class Continue[+S, +O](state: S, reply: O) extends Next[S, O]

    /**
     * Finish: drop the entity's state and end it. The slot is emptied, the watcher's `onDone` runs, and the
     * handle refuses everything afterwards with [[Terminated]] — it does not quietly seed a second life on
     * the next message. Another entity for the same subject is a new [[Actor.spawn]], which is what keeps
     * "one entity per key" true even while handles are being passed around.
     *
     * @param reply the value sent back for the message just handled
     * @tparam O the reply produced
     */
    case class Done[+O](reply: O) extends Next[Nothing, O]
}
