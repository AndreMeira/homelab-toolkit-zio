package homelab.incubator.processing.v5


import homelab.common.error.ApplicationError.AdapterError
import homelab.common.flow.Loop
import homelab.common.messaging.Partitioner
import homelab.common.store.KeyValueStore
import zio.*

import scala.collection.immutable.Queue


/**
 * The behaviour of a keyed, stateful entity — *just* the behaviour: seed a fresh entity ([[init]]) and step it
 * ([[next]]). No node, no queue, no run loop; those live in [[Stateful.Runner]], which drives a `Stateful`
 * over many keys. Following the [[homelab.common.processing.Workflow]] model, the behaviour is decoupled from
 * the machinery that runs it: a `Stateful` value is handed to a runner per call
 * (`runner.fire(actor, message)`), never bound into it.
 *
 * @tparam E the error a transition may fail with
 * @tparam I the message accepted
 * @tparam S the entity state
 * @tparam O the reply produced
 */
trait Stateful[E, I, S, O] {

  /**
   * The starting state for an entity the store has never seen (or has since passivated).
   *
   * @param message the first message routed to this entity — the one that triggered seeding
   * @return the initial state; aborts with `E` if seeding fails
   */
  def init(message: I): IO[E, S]

  /**
   * Build this entity's transition, binding [[Stateful.Self]] once so it can be closed over. The returned
   * function is invoked per message: it replies an `O` and chooses the entity's lifecycle
   * ([[Stateful.Step.Continue]] to persist a new state, [[Stateful.Step.Done]] to evict it).
   *
   * @param self this entity's handle onto its own mailbox — `send` a follow-up, or `pipeToSelf` background work
   * @return the transition `(message, state) => Next`; aborts with `E` when the step fails
   */
  def next(self: Stateful.Self[I]): (input: I, state: S) => IO[E, Stateful.Step[S, O]]
}


object Stateful {

  /**
   * A transition's window onto its own entity: enqueue a follow-up (`send`), or run background work whose
   * result is delivered back as a message (`pipeToSelf`). Both are non-blocking and route through the same
   * mailbox the entity drains — tagged with the *same* behaviour that produced this handle — so a self-message
   * serialises behind whatever the entity is currently doing.
   *
   * @tparam I the message accepted
   */
  trait Self[-I]:
    /** Enqueue `input` onto this entity's own mailbox (fire-and-forget). */
    def send(input: I): UIO[Unit]

    /** Run `message` in the background; deliver its result to this entity when it completes. */
    def pipeToSelf(message: UIO[I]): UIO[Unit]

  /** One transition's outcome — both branches reply an `O`; `Done` also evicts the state. */
  type Step[+S, O] = Step.Continue[S, O] | Step.Done[O]

  object Step:
    type Continue[+S, O] = Loop.Next.Continue[(S, O), O]
    type Done[+O]        = Loop.Next.Done[Nothing, O]

  /**
   * The substrate that runs a [[Stateful]] behaviour over many keys: the runner holds the *machinery* (state
   * store + per-key scheduling + scope), and the behaviour is passed to each call
   * (`fire(actor, message)` / `ask(actor, message)`), never bound in. This is why a runner exists here and
   * not for [[homelab.common.processing.Workflow]], where durability and locking are combinators over the
   * behaviour value (`persisted`, `serialised`) because there is no live machinery to own. It is *specific to a
   * behaviour* only in its type parameters, so state needs no serialisation — [[KeyValueStore]] holds a native
   * `S`, and what `set`/`delete` mean (drop a row, release a handle, nothing) is the store's business, not the
   * runner's.
   *
   * There is no single/pool split: every entity is a key, and a "single actor" is the degenerate one-key case.
   *
   * **Fork-on-send, no start.** Like [[homelab.common.flow.batching.Serial]], there is no standing drain to
   * boot: construction is inert, and the *first* message to a dormant key forks that key's drain into the
   * captured scope. A key is "in flight" exactly while its backlog is non-empty; the drain processes it one
   * message at a time (per-key FIFO — the actor guarantee) and releases the key when the backlog empties.
   * Distinct keys drain on independent fibers, so cross-key work runs in serial for free.
   *
   * **Behaviour travels in the envelope.** Because the behaviour is not bound at construction, each queued
   * message carries the `Stateful` that should process it; a self-`send` re-tags with the same one. So a drain
   * always has a behaviour to apply, whatever call (or self-send) enqueued the message it just claimed.
   *
   * **Out-of-band replies.** A step's reply `O` is routed to the caller's promise from an uninterruptible
   * finalizer, so a failed or interrupted step fails the `ask` rather than the drain — one bad message never
   * wedges a key's backlog. Built only via [[Runner.make]].
   *
   * @param scope     the captured scope drains and `pipeToSelf` work are forked into (closing it drops pending)
   * @param ref       per-key backlog: a key present with a queue is in flight, absent is idle
   * @param store     the entity state, `K -> S`
   * @param partition extracts an entity key from a message
   * @tparam E the error a transition may fail with
   * @tparam I the message accepted
   * @tparam K the entity key
   * @tparam S the entity state
   * @tparam O the reply produced
   */
  final class Runner[E, I, K, S, O] private (
    scope: Scope,
    ref: Ref[Map[K, Queue[Runner.Envelope[E, I, S, O]]]],
    store: KeyValueStore[K, S],
    partition: I => K,
  ):
    private type Err     = E | AdapterError
    private type Payload = Runner.Envelope[E, I, S, O]

    /** Fire-and-forget: run `actor` for `message`'s entity, discarding the reply. */
    def fire(actor: Stateful[E, I, S, O], message: I): UIO[Unit] =
      submit((actor, message, None))

    /**
     * Request/reply: run `actor` for `message`'s entity, enqueueing with a fresh promise and awaiting it.
     *
     * @param actor   the behaviour to step the entity with
     * @param message the message to send
     * @return the entity's reply; aborts with `Err` if the step (or its persistence) fails
     */
    def ask(actor: Stateful[E, I, S, O], message: I): IO[Err, O] =
      for
        promise <- Promise.make[Err, O]
        _       <- submit((actor, message, Some(promise)))
        reply   <- promise.await
      yield reply

    /**
     * Enqueue `payload` under its key and, if that key was idle, fork its drain. Enqueue+fork is uninterruptible so
     * an interrupt cannot land *between* them and strand a freshly non-idle key with no drain; the drain body
     * itself is `interruptible` so scope close can kill it.
     *
     * @param payload the envelope to enqueue
     * @return noop once queued and any drain forked
     */
    private def submit(payload: Payload): UIO[Unit] = {
      val key = partition(payload.message)
      ZIO.uninterruptible:
        enqueue(key, payload).flatMap: started =>
          ZIO.when(started)(drainLoop(key).interruptible.forkIn(scope)).unit
    }

    /**
     * Append `payload` to its key's backlog.
     *
     * @param key the entity key
     * @param payload the envelope to enqueue
     * @return `true` if the key was idle (so the caller must fork its drain), else `false`
     */
    private def enqueue(key: K, payload: Payload): UIO[Boolean] =
      ref.modify: map =>
        map.get(key) match
          case None    => true  -> map.updated(key, Queue(payload))
          case Some(q) => false -> map.updated(key, q.enqueue(payload))

    /**
     * Drain one key to exhaustion, one message at a time, then stop (its backlog now released).
     *
     * @param key the entity key to drain
     * @return noop; carried on the forked drain fiber, its error/interrupt unobserved by callers
     */
    private def drainLoop(key: K): IO[Err, Unit] =
      claimNext(key).flatMap:
        case Some(env) => process(key, env) *> drainLoop(key)
        case None      => ZIO.unit

    /**
     * Claim the key's head message. The key stays present (in flight) even once its queue empties — it is only
     * *released* on a later step that finds it already empty. That extra empty step is what makes the drain
     * self-serialising: while the last message is still processing the key is present, so a concurrent
     * [[enqueue]] appends (`started == false`, no second drain forked); only after the drain observes the empty
     * queue and removes the key does a fresh message re-fork. So at most one drain per key ever runs.
     *
     * @param key the entity key
     * @return the next envelope, or `None` when the backlog is empty (the key is released in the same step)
     */
    private def claimNext(key: K): UIO[Option[Payload]] = ref.modify: map =>
      map.get(key).fold(None -> map) { queue =>
        queue.dequeueOption match
          case Some((env, rest)) => Some(env) -> map.updated(key, rest) // stay present, even if rest empty
          case None              => None      -> (map - key)            // empty → release the key
      }

    /**
     * Next one entity for one message with the envelope's behaviour: load state (seeding via
     * [[Stateful.init]]), run the transition, persist ([[Step.Continue]]) or evict ([[Step.Done]]), then
     * route the outcome to the message's reply promise. The promise is completed from an *uninterruptible*
     * `onExit`, so a per-step `E`, a store `AdapterError`, a defect, or an interrupt (scope close mid-step) all
     * reach the `ask`. A typed failure/defect is then swallowed so the drain keeps going; only an interrupt is
     * re-raised, to stop the drain as the scope closes.
     *
     * @param key the entity key
     * @param payload the envelope to process — its `actor` supplies `init`/`next`
     * @return noop; the outcome goes to the reply promise, never the drain's error channel (bar interrupt)
     */
    private def process(key: K, payload: Payload): IO[Err, Unit] = {
      for
        actor  = payload.actor
        state <- store.get(key).someOrElseZIO(actor.init(payload.message))
        next  <- actor.next(selfFor(actor))(payload.message, state)
        reply <- next match
                   case Loop.Next.Continue(nextState, out) => store.set(key, nextState).as(out)
                   case Loop.Next.Done(out)                => store.delete(key).as(out)
      yield reply
    }
      .onExit(exit => ZIO.foreachDiscard(payload.replyTo)(_.done(exit)))
      .catchAllCause(cause => if cause.isInterrupted then ZIO.refailCause(cause) else ZIO.unit)
      .unit

    /**
     * The [[Self]] handed to `actor`'s transition: `send`/`pipeToSelf` route back through [[submit]] tagged
     * with the *same* `actor`, so a self-message is stepped by the same behaviour and wakes a dormant key like
     * any other send. `pipeToSelf` forks its background work into the runner scope.
     *
     * @param actor the behaviour a self-message must be re-tagged with
     * @return the self handle for this step
     */
    private def selfFor(actor: Stateful[E, I, S, O]): Self[I] = new Self[I]:
      def send(input: I): UIO[Unit]              = submit((actor, input, None))
      def pipeToSelf(message: UIO[I]): UIO[Unit] = message.flatMap(send).forkIn(scope).unit

    /**
     * Scope finalizer: interrupt every *queued* caller across all keys so close drops pending work without
     * stranding anyone. A message already in flight is handled by [[process]]'s `onExit` (the drain fiber is
     * interrupted by the same scope close).
     *
     * @return noop
     */
    private def abandon: UIO[Unit] =
      ref.get.flatMap: map =>
        ZIO.foreachDiscard(map.values.flatten)(payload => ZIO.foreachDiscard(payload.replyTo)(_.interrupt.unit))

  object Runner:

    /** A queued message: the behaviour to run it, the input, and a reply channel when it is an `ask`. */
    type Envelope[E, I, S, O] =
      (actor: Stateful[E, I, S, O], message: I, replyTo: Option[Promise[E | AdapterError, O]])

    /**
     * Build a [[Runner]] over `store`. Does not start anything — the first `fire`/`ask` per key forks that
     * key's drain into the runner's scope. A scope finalizer interrupts any still-queued callers on close.
     *
     * The type parameters are not all inferable from `store` alone (`E`, `O` are free), so a call site
     * annotates them: `Runner.inmemory[E, I, K, S, O](store)`.
     *
     * @param store the entity state store, keyed by the same `K` messages partition to
     * @param key   derives the entity key from a message
     * @tparam E the error a transition may fail with
     * @tparam I the message accepted
     * @tparam K the entity key
     * @tparam S the entity state
     * @tparam O the reply produced
     * @return a runner ready to `fire`/`ask`; needs a [[Scope]] — the parent drains are forked into
     */
    def make[E, I, K, S, O](
      store: KeyValueStore[K, S]
    )(using
      key: Partitioner.Key[I] { type Type = K }
    ): ZIO[Scope, Nothing, Runner[E, I, K, S, O]] =
      for
        scope <- ZIO.scope
        ref   <- Ref.make(Map.empty[K, Queue[Envelope[E, I, S, O]]])
        runner = new Runner[E, I, K, S, O](scope, ref, store, key.get)
        _     <- ZIO.addFinalizer(runner.abandon)
      yield runner
}
