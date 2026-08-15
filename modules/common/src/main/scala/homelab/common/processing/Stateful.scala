package homelab.common.processing


import homelab.common.error.ApplicationError
import homelab.common.error.ApplicationError.AdapterError
import homelab.common.messaging.Pipe
import homelab.common.store.KeyValueStore
import zio.*


/**
 * A stateful entity per key, built out of a [[Worker]] and a [[KeyValueStore]] and nothing else: the worker
 * supplies the mailbox, the request/reply plumbing and the run loop; the store supplies the state; this adds
 * only the step — seed on first sight ([[init]]), advance on every message ([[next]]).
 *
 * Deliberately not called an actor. It offers none of what that word implies — no address to pass around, no
 * supervision, no lifecycle to watch, no self-messaging — and naming it so would promise all of them.
 *
 * **There are no entity handles, so there is no entity lifetime.** An entity is a key with a row in the
 * store, and [[Stateful.Next.Done]] deletes that row — after which the next message for the key seeds a fresh
 * one. Nothing can hold a reference to something that has ended, so nothing needs refusing, retrying, or
 * replacing.
 *
 * **Serialisation is the pipe's job**, not this trait's. A plain [[Worker]] handles one message at a time,
 * which is enough on its own. [[Stateful.Parallel]] handles several at once, and takes a
 * [[homelab.common.messaging.Pipe.KeySafe]] so that the pipe it runs over has said it keeps a key in flight
 * once at a time. Over a pipe that does not, two messages for one entity read the same state and both write
 * it back — a failure that loses writes rather than failing, which is why the requirement is in the type.
 *
 * @tparam E the error a step may fail with — between the store's `AdapterError` and `ApplicationError`
 * @tparam K the entity key
 * @tparam S the entity state
 * @tparam I the message accepted
 * @tparam O the reply produced
 */
trait Stateful[E >: AdapterError <: ApplicationError, K, S, I, O] extends Worker[E, I, O] {

  /** Where entity state lives — one row per key, absent meaning "not yet seeded, or finished". */
  def store: KeyValueStore[K, S]

  /**
   * The entity a message belongs to.
   *
   * @param message the message to route
   * @return its entity key
   */
  def key(message: I): K

  /**
   * The starting state for a key the store has no row for.
   *
   * @param message the message that triggered seeding
   * @return the initial state; aborts with `E` if seeding fails
   */
  def init(message: I): IO[E, S]

  /**
   * Advance an entity: given its state and a message, reply and say what becomes of the state.
   *
   * @param state   the entity's current state
   * @param message the message to handle
   * @return the outcome; aborts with `E` when the step fails, which fails that message's caller alone
   */
  def next(state: S, message: I): IO[E, Stateful.Next[S, O]]

  /**
   * Load, step, store — the whole of what this adds to a [[Worker]].
   *
   * `final` for the same reason [[Worker.process]] is: this is the protocol rather than the work. An override
   * that forgot to write the new state back would leave an entity silently stuck on its old one.
   *
   * @return the handler the worker runs per message
   */
  final override def receive: I => IO[E, O] = message =>
    val entity = key(message)
    for
      current <- store.get(entity).someOrElseZIO(init(message))
      outcome <- next(current, message)
      reply   <- outcome match
                   case Stateful.Next.Continue(updated, out) => store.set(entity, updated).as(out)
                   case Stateful.Next.Done(out)              => store.delete(entity).as(out)
    yield reply
}


object Stateful {

  /**
   * One step's outcome. Both branches carry the reply; they differ only in what becomes of the state —
   * [[Continue]] keeps it, [[Done]] drops the row.
   *
   * @tparam S the entity state, carried only by [[Continue]]
   * @tparam O the reply produced, carried by both
   */
  sealed trait Next[+S, +O]:
    /** The value sent back to whoever asked. */
    def reply: O

  object Next:

    /**
     * Carry on with `state`.
     *
     * @param state the state the entity keeps
     * @param reply the value sent back for this message
     */
    case class Continue[+S, +O](state: S, reply: O) extends Next[S, O]

    /**
     * Finish: delete the entity's row. Not a terminal event for anything holding a reference — nothing holds
     * one — so the next message for this key simply seeds a new entity through [[Stateful.init]].
     *
     * @param reply the value sent back for this message
     */
    case class Done[+O](reply: O) extends Next[Nothing, O]

  /**
   * A [[Stateful]] that handles messages concurrently, up to `parallelism` at a time — the [[Worker.Parallel]]
   * run loop over the same load-step-store logic.
   *
   * **The input pipe must serialise per key**, or this is unsafe: two messages for one entity handled at once
   * both read the state, both step from it, and the second write wins, losing the first. The pipe is the only
   * thing that can prevent it. `Pipe.fromKeyedQueue(queue)(key)` — built with the *same* key function this
   * trait stores under — is the pipe that holds a key while one of its values is in flight.
   *
   * @tparam E the error a step may fail with — between the store's `AdapterError` and `ApplicationError`
   * @tparam K the entity key
   * @tparam S the entity state
   * @tparam I the message accepted
   * @tparam O the reply produced
   */
  trait Parallel[E >: AdapterError <: ApplicationError, K, S, I, O] extends Stateful[E, K, S, I, O], Worker.Parallel[E, I, O] {

    /**
     * The mailbox, narrowed to one that keeps a key in flight once at a time — which is what makes running
     * several messages at once safe here. A plain [[Pipe]] does not typecheck, so the requirement is met at
     * the construction site rather than remembered.
     *
     * What the type cannot say is that the pipe partitions by *this* trait's [[key]]: a keyed pipe built with
     * a different key function satisfies the marker and still interleaves one entity's messages.
     */
    override def input: Pipe.KeySafe[E, (I, Promise[E, O])]
  }
}
