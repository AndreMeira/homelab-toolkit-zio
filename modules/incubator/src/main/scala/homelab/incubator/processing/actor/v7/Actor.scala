package homelab.incubator.processing.actor.v7


import homelab.common.error.ApplicationError.AdapterError
import homelab.common.flow.Permit
import homelab.common.store.Bucket
import zio.*


/**
 * The behaviour of a stateful entity — *just* the behaviour: seed a fresh entity ([[init]]) and step it
 * ([[next]]). No queue, no run loop, no lifetime; those belong to whatever runs it.
 *
 * @tparam E the error a transition may fail with
 * @tparam I the message accepted
 * @tparam S the entity state
 * @tparam O the reply produced
 */
trait Actor[E, I, S, O] {

  /**
   * The starting state for an entity that has none yet.
   *
   * @param message the first message routed to this entity — the one that triggered seeding
   * @return the initial state; aborts with `E` if seeding fails
   */
  def init(message: I): IO[E, S]

  /**
   * Build this entity's transition, binding [[Actor.Self]] once so it can be closed over. The returned
   * function is invoked per message: it replies an `O` and chooses the entity's fate — [[Actor.Next.Continue]]
   * to carry on with a new state, [[Actor.Next.Done]] to finish.
   *
   * @param self this entity's handle onto its own mailbox — `send` a follow-up, or `pipeToSelf` background work
   * @return the transition `(message, state) => Next`; aborts with `E` when the step fails
   */
  def next(self: Actor.Self[I]): (input: I, state: S) => IO[E, Actor.Next[S, O]]
}


object Actor {

  /**
   * The refusal a handle gives once its entity is [[Next.Done]]: the entity is finished and the message was
   * not processed. Not a failure of the behaviour — a fact about the handle — which is why it rides in the
   * handle's error channel and never in an [[Actor]]'s `E`.
   */
  case object Terminated
  type Terminated = Terminated.type

  /**
   * One transition's outcome. Both branches carry the [[reply]] the caller receives; they differ only in
   * whether the entity carries on.
   *
   * @tparam S the entity state, carried only by [[Next.Continue]]
   * @tparam O the reply produced, carried by both
   */
  sealed trait Next[+S, +O]:
    /** The value sent back to whoever asked — produced whether or not the entity survives the step. */
    def reply: O

  object Next:

    /**
     * Carry on with `state` — the ordinary step. The next message is handed exactly this state, with no call
     * to [[Actor.init]].
     *
     * @param state the state the entity carries into its next message
     * @param reply the value sent back for the message just handled
     */
    case class Continue[+S, +O](state: S, reply: O) extends Next[S, O]

    /**
     * Finish: drop the state and end the entity. Its handle refuses everything afterwards with [[Terminated]]
     * rather than quietly seeding a second life, so "one entity per handle" stays true however widely the
     * handle is passed around.
     *
     * @param reply the value sent back for the message just handled
     */
    case class Done[+O](reply: O) extends Next[Nothing, O]

  /**
   * A transition's window onto its own entity: queue a follow-up ([[send]]), or run background work whose
   * result comes back as a message ([[pipeToSelf]]). Both are non-blocking and both land in the same mailbox
   * the entity drains, so a self-message serialises behind whatever the entity is currently doing rather than
   * re-entering the step that sent it.
   *
   * @tparam I the message accepted
   */
  trait Self[-I]:

    /**
     * Queue `input` onto this entity's own mailbox, fire-and-forget.
     *
     * @param input the message to queue
     * @return noop once queued
     */
    def send(input: I): UIO[Unit]

    /**
     * Run `message` in the background and queue its result when it completes. The work outlives the step that
     * started it and dies with the entity.
     *
     * @param message the background work producing the follow-up message
     * @return noop once forked
     */
    def pipeToSelf(message: UIO[I]): UIO[Unit]

  /**
   * A live entity: an [[Actor]] bound to its state and to the mailbox that serialises it. Messages go in,
   * replies come out, and every message is processed in order, one at a time.
   *
   * A handle is alive or it is not. When a transition returns [[Next.Done]] the entity finishes, and this
   * handle refuses everything afterwards with [[Terminated]]. Another entity for the same subject is a new
   * [[Actor.spawn]].
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
     * @return the transition's reply; aborts with `E` if the step fails, with [[Terminated]] if the entity had
     *         already finished, and is interrupted if the entity is torn down while this message is queued
     */
    def ask(message: I): IO[E | Terminated, O]

    /**
     * Send `message` without awaiting the step. The reply, and any failure, are discarded — this is the only
     * channel either travels on.
     *
     * @param message the message to process
     * @return noop once queued, not once processed
     */
    def send(message: I): IO[E | Terminated, Unit]

    /**
     * Wait for this entity to finish, and take the reply its final step produced. Never fails: a failing step
     * does not end an entity, so [[Next.Done]] is the only way here. Interrupted if the entity is torn down
     * first, since one torn down that way never finishes.
     *
     * @return the final reply, once the entity has finished
     */
    def await: UIO[O]

  /**
   * Start `behaviour` in the calling scope, with a mailbox of its own. Nothing runs until the first message.
   *
   * The entity owns a scope: its mailbox, its background work, and its lifetime all live there, and it closes
   * when the entity finishes or when the caller's scope does — whichever comes first.
   *
   * `state` is where the entity keeps itself between messages; omit it for a private in-process slot, or pass
   * one to put the state in a database. Note a slot is a *location*, not a saved copy: [[Next.Done]] empties
   * it, so a replacement over the same slot seeds afresh.
   *
   * `mailbox` bounds how many messages may be accepted-but-unanswered at once, and a producer beyond that
   * waits. It bounds *ingress only by consequence*: a transition's own [[Self.send]] takes a slot like any
   * other message, so a bounded entity that messages itself while saturated waits for room its own step must
   * finish to make. Bound the mailbox of an entity that talks to itself, and you are choosing that risk
   * deliberately.
   *
   * @param behaviour the transition to step this entity with
   * @param state     the slot this entity's state lives in; `None` allocates a private in-memory one
   * @param mailbox   accepted-but-unanswered messages allowed at once; `None` for no bound
   * @tparam E the error a transition may fail with — must admit the store's `AdapterError`
   * @tparam I the message accepted
   * @tparam S the entity state
   * @tparam O the reply produced
   * @return the live entity, inert until its first message
   */
  def spawn[E >: AdapterError, I, S, O](
    behaviour: Actor[E, I, S, O],
    state: Option[Bucket[S]] = None,
    mailbox: Option[Int] = None,
  ): ZIO[Scope, Permit.Error, Running[E, I, O]] =
    for
      parent   <- ZIO.scope
      own      <- parent.fork
      worker   <- own.extend(Worker.bounded(mailbox))
      held     <- state.fold(Bucket.inmemory[S])(ZIO.succeed(_))
      finished <- Promise.make[Nothing, O]
      // Torn down without finishing, nothing ever completes this, so release anyone waiting on it.
      _        <- own.addFinalizer(finished.interrupt)
      // Finishing retires the entity: its mailbox and any background work go with the scope. The `flush` is
      // load-bearing — `finished` completes *inside* the final step, before its reply has been routed, so
      // closing on that signal alone would interrupt the caller waiting for the last answer. Flushing waits
      // for the mailbox to drain past that step, which also lets everything queued behind it be refused
      // properly rather than dropped. Forked into the scope it closes, which is safe because a fiber is not
      // interrupted by its own fork finalizer.
      _        <- (finished.await *> worker.flush *> own.close(Exit.unit)).forkIn(own)
    yield new Live(worker, held, behaviour, finished, own)

  /**
   * The one [[Running]]: a [[Worker]] for order, a [[Bucket]] for state, a [[Promise]] for the end of life.
   *
   * Every message is submitted to the worker as an effect, so the worker's guarantees are the entity's:
   * one at a time in arrival order, a reply routed to the caller, a failure confined to its own caller, and
   * queued callers released when the entity goes away.
   *
   * @param worker   the mailbox — serialises every message, including the entity's own
   * @param state    the slot this entity's state lives in — empty means "not yet seeded"
   * @param behaviour the transition to step with
   * @param finished completed with the final reply when the entity ends; every later message is refused
   * @param own      the entity's scope: its worker, its background work, its lifetime
   */
  final private class Live[E >: AdapterError, I, S, O](
    worker: Worker,
    state: Bucket[S],
    behaviour: Actor[E, I, S, O],
    finished: Promise[Nothing, O],
    own: Scope,
  ) extends Running[E, I, O] {

    def ask(message: I): IO[E | Terminated, O] = refusing(worker.submit(step(message)).flatMap(_.await))

    def send(message: I): IO[E | Terminated, Unit] = refusing(worker.submit(step(message)).unit)

    def await: UIO[O] = finished.await

    /**
     * One message: refuse it if the entity has finished, else load the state (seeding when the slot is
     * empty), run the transition, and either keep the new state or end the entity.
     *
     * The refusal check runs *inside* the submission, so it is serialised with the step that ends the entity
     * — a message queued a moment before the end is refused, one queued a moment after is refused, and there
     * is no window between.
     *
     * @param message the message to process
     * @return the transition's reply; aborts with `E` if the step fails, or [[Terminated]] if the entity had
     *         already finished
     */
    private def step(message: I): IO[E | Terminated, O] =
      finished.isDone.flatMap:
        case true  => ZIO.fail(Terminated)
        case false =>
          for
            current <- state.get.someOrElseZIO(behaviour.init(message))
            outcome <- behaviour.next(self)(message, current)
            reply   <- outcome match
                         case Next.Continue(updated, out) => state.set(updated).as(out)
                         case Next.Done(out)              => state.empty *> finished.succeed(out).as(out)
          yield reply

    /**
     * Hold the "a finished entity refuses" contract across retirement.
     *
     * The refusal normally comes from [[step]], which is only reached while the mailbox is alive. Once the
     * entity has finished, its mailbox is released — and a message submitted to a released mailbox is
     * *dropped*, so its caller is interrupted rather than refused. That would make the answer to "what does a
     * finished entity do with my message?" depend on whether retirement had got there yet.
     *
     * So: refuse up front when the entity is already known to be finished, and read an interruption as a
     * refusal when it turns out the entity finished underneath us. A genuine interruption — the caller's own,
     * or the teardown of a *live* entity — still propagates, because `finished` is only complete in the one
     * case this is covering.
     *
     * @param submission the delivery to guard
     * @tparam A the delivery's result
     * @return the delivery's result; aborts with [[Terminated]] if the entity was, or has just become, finished
     */
    private def refusing[A](submission: IO[E | Terminated, A]): IO[E | Terminated, A] =
      finished.isDone.flatMap:
        case true  => ZIO.fail(Terminated)
        case false =>
          submission.catchAllCause: cause =>
            if cause.isInterrupted then
              finished.isDone.flatMap:
                case true  => ZIO.fail(Terminated)
                case false => ZIO.refailCause(cause)
            else ZIO.refailCause(cause)

    /** The handle handed to the transition: same mailbox, so a self-message queues rather than re-enters. */
    private val self: Self[I] = new Self[I]:
      def send(input: I): UIO[Unit]              = worker.submit(step(input)).unit
      def pipeToSelf(message: UIO[I]): UIO[Unit] = message.flatMap(send).forkIn(own).unit
  }
}
