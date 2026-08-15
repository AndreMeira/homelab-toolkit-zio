package homelab.incubator.processing.v5


import homelab.common.error.ApplicationError.AdapterError
import homelab.common.flow.KeyedQueue
import homelab.common.messaging.Partitioner
import homelab.common.store.Bucket
import homelab.incubator.processing.v5.Actor.{ Next, Running, Self, Terminated }
// Explicit: `zio.Runtime` would otherwise win over this package's own via the wildcard below.
import homelab.incubator.processing.v5.Runtime
import zio.*

import scala.collection.immutable.Queue


/**
 * A keyed pool of entities behind one [[Running]] façade: messages are partitioned, each key gets its own
 * entity, and entities are spawned and replaced as they finish. What a caller holds looks exactly like a
 * single entity.
 *
 * **The pool is itself an actor.** Routing state — which entity serves which key, which keys have a delivery
 * in flight, and what is queued behind them — is the loop state of one [[Distributed.Distributor]] entity, so
 * every read and write of it happens in a serialised step. Nothing else can resolve a key while a key is
 * being resolved, evicted, or replaced.
 *
 * **One delivery per key at a time.** A key with a delivery in flight is `running`; messages arriving for it
 * are queued in the pool's own state rather than forwarded, and the next one goes out only when the previous
 * outcome comes home. That is what makes a stale handle impossible: the pool is the sole holder of every
 * handle, and it never decides to replace an entity while a delivery to the old one is outstanding.
 *
 * **The loop never waits.** A delivery is handed to [[Self.pipeToSelf]], so the distributor's own step ends
 * immediately and the outcome returns as another message. Callers are answered through a promise handed back
 * at accept time, not by the distributor's reply.
 *
 * **Entities are scoped individually.** Each is spawned into a scope of its own, closed as soon as the pool
 * learns it has finished, so what the pool holds is proportional to the entities that are *live* rather than
 * to every entity it has ever spawned.
 */
object Distributed {

  /**
   * Start a pool over `behaviour` in the calling scope. Nothing runs until the first message; entities are
   * spawned on demand, and closing the scope takes down the pool and everything in it — releasing every
   * caller still waiting for an answer rather than leaving them parked.
   *
   * `maxInFlight` bounds *accepted but unanswered* messages across the whole pool, and it is the only thing
   * standing between an overwhelmed pool and unbounded memory. A caller takes a permit before its message is
   * accepted and gets it back when the message is answered, so a saturated pool makes producers wait rather
   * than piling work up behind entities that cannot keep up. Bounding the pool's own queues instead would
   * achieve nothing: callers enqueue on the distributor's mailbox first, which has no bound, so the pile
   * would simply move one queue upstream. Only the producer can be made to wait.
   *
   * The bound is on messages *entering* the pool. An entity's own `Self.send` / `pipeToSelf` deliberately
   * bypass it: work that must complete before a permit can be released must never wait for one, or a
   * saturated pool would deadlock on itself.
   *
   * It bounds concurrency too, as a side effect — every delivery belongs to a message holding a permit — so
   * pick it thinking about how many entities may run at once, not only about memory. Deep queue with narrow
   * concurrency needs a second permit, taken by the delivery rather than by the caller.
   *
   * @param behaviour   the behaviour every entity is spawned with
   * @param maxInFlight accepted-but-unanswered messages allowed at once; `None` for no backpressure
   * @param key         partitions a message to its entity key
   * @tparam E the error a transition may fail with — must admit `AdapterError`
   * @tparam I the message accepted
   * @tparam K the entity key
   * @tparam S the entity state
   * @tparam O the reply produced
   * @return the pool, inert until its first message; aborts if `maxInFlight` is non-positive
   */
  def make[E >: AdapterError, I, K, S, O](
    behaviour: Actor[E, I, S, O],
    maxInFlight: Option[Int] = None,
  )(using
    key: Partitioner.Key[I] { type Type = K }
  ): ZIO[Scope, KeyedQueue.Error, Running[E, I, O]] =
    for
      permit  <- KeyedQueue.Permit.make(maxInFlight)
      // One runtime for the whole pool: the distributor takes a slot on it, and so does every entity.
      runtime <- Runtime.make
      // The routing table lives in a slot this factory owns rather than one the entity allocates for itself,
      // so the shutdown hook can read it. It is still the distributor's state; nothing else writes it.
      table   <- Bucket.inmemory[Table[E, I, K, O]]
      _       <- ZIO.addFinalizer(shutdown(table))
      distributor <- Actor.spawnIn(runtime)(new Distributor(runtime, behaviour, key.get, permit), Some(table))(ZIO.unit)
    yield new Handle(distributor, permit)

  /**
   * Tear the pool down by releasing everyone still waiting. Callers are interrupted rather than failed — a
   * pool that is going away has no answer to give and no error of its own to report — and without this they
   * would park forever, since nothing else holds their promises. The entities themselves need nothing here:
   * they run on the pool's runtime, whose scope is closing anyway.
   *
   * Best-effort by nature: it reads the last committed table, so a message accepted in the instant the scope
   * closes may not be represented there.
   *
   * @param table the slot the distributor keeps its routing state in
   * @return noop once everything reachable has been released
   */
  private def shutdown[E, I, K, O](table: Bucket[Table[E, I, K, O]]): UIO[Unit] =
    // A store failure here is not worth failing a teardown over: read what we can, release what we find.
    table.get.catchAll(_ => ZIO.none).flatMap {
      case None        => ZIO.unit
      case Some(state) =>
        val waiting = state.running.values ++ state.pending.values.flatten.map((_, replyTo) => replyTo)
        ZIO.foreachDiscard(waiting)(_.interrupt.unit)
    }

  /** What a caller is owed for one message: the reply, or the failure the step produced. */
  private type ReplyTo[E, O] = Promise[E | Terminated, O]

  /**
   * What the distributor entity accepts. A pool's traffic is not just user messages: a delivery reports back
   * when it settles, and reports separately when the entity it was sent to turned out to be finished. Both
   * come back as messages so they are handled in the same serialised loop as everything else.
   *
   * @tparam E the error a transition may fail with
   * @tparam I the user message
   * @tparam K the entity key
   * @tparam O the reply produced
   */
  private enum Command[E, I, K, O]:

    /** A caller's message, with the promise it is waiting on. */
    case Deliver(message: I, replyTo: ReplyTo[E, O])

    /** The delivery in flight for `key` is over, however it ended — the key is free to send its next. */
    case Settled(key: K)

    /** The entity for `key` was finished; the message never ran and must go to a replacement. */
    case Refused(key: K, message: I, replyTo: ReplyTo[E, O])

  /**
   * The distributor's loop state: who serves each key, what is queued for it, and which keys have a delivery
   * in flight. `pending` and `running` together are the keyed-scheduling core — a key in `running` holds its
   * queue back until its outstanding delivery settles.
   *
   * `running` maps each key to the promise its delivery will answer, rather than being a bare set, so that
   * everything the pool owes anybody is reachable from the state alone — which is what lets [[shutdown]]
   * release callers instead of leaving them known only to a delivery's closure.
   *
   * @param alive   the entity serving each key, with its scope
   * @param pending messages queued behind an in-flight delivery, per key, in arrival order
   * @param running the promise owed for each key's outstanding delivery
   */
  final private case class Table[E, I, K, O](
    alive: Map[K, Running[E, I, O]],
    pending: Map[K, Queue[(I, ReplyTo[E, O])]],
    running: Map[K, ReplyTo[E, O]],
  ) {

    /** Record `entity` as serving `key`, replacing whatever was there. */
    def serving(key: K, actor: Running[E, I, O]): Table[E, I, K, O] = copy(alive = alive.updated(key, actor))

    /** Forget the entity for `key` — a replacement is spawned on demand. */
    def evict(key: K): Table[E, I, K, O] = copy(alive = alive - key)

    /** Mark `key` as having a delivery in flight, owing `replyTo`. */
    def busy(key: K, replyTo: ReplyTo[E, O]): Table[E, I, K, O] = copy(running = running.updated(key, replyTo))

    /** Mark `key` idle — nothing queued, nothing outstanding. */
    def idle(key: K): Table[E, I, K, O] = copy(pending = pending - key, running = running - key)

    /** Queue `message` behind `key`'s in-flight delivery. */
    def queue(key: K, message: I, replyTo: ReplyTo[E, O]): Table[E, I, K, O] =
      copy(pending = pending.updated(key, pending.getOrElse(key, Queue.empty).enqueue(message -> replyTo)))

    /**
     * Take `key`'s next queued message, if any.
     *
     * @return the message and the table without it, or `None` when the key's queue is empty
     */
    def dequeue(key: K): Option[((I, ReplyTo[E, O]), Table[E, I, K, O])] =
      pending.get(key).flatMap(_.dequeueOption).map {
        case (head, rest) =>
          val queues = if rest.isEmpty then pending - key else pending.updated(key, rest)
          head -> copy(pending = queues)
      }
  }

  /**
   * The pool's routing entity. Its `Unit` reply is deliberate: a caller is answered through the promise it
   * handed in, never by this entity's step, so the loop is only ever a map lookup, a spawn, and a fork.
   *
   * @param runtime   the substrate every entity — this one included — takes a slot on
   * @param behaviour the behaviour every entity is spawned with
   * @param partition extracts an entity key from a message
   * @param permit    the in-flight bound; released as each message is answered
   */
  final private class Distributor[E >: AdapterError, I, K, S, O](
    runtime: Runtime,
    behaviour: Actor[E, I, S, O],
    partition: I => K,
    permit: KeyedQueue.Permit,
  ) extends Actor[E, Command[E, I, K, O], Table[E, I, K, O], Unit] {

    private type Cmd   = Command[E, I, K, O]
    private type State = Table[E, I, K, O]

    /**
     * @param message ignored — a pool starts empty whatever arrives first
     * @return the empty table
     */
    def init(message: Cmd): IO[E, State] =
      ZIO.succeed(Table(Map.empty, Map.empty, Map.empty))

    /**
     * Handle one command. A pool never finishes, so every branch continues.
     *
     * @param self this entity's own mailbox — deliveries report back through it
     * @return the transition; aborts with `E` only if spawning does
     */
    def next(self: Self[Cmd]): (input: Cmd, state: State) => IO[E, Next[State, Unit]] =
      (command, table) =>
        command match

          case Command.Deliver(message, replyTo) =>
            val key = partition(message)
            // A key already has a delivery out: queue behind it rather than forwarding beside it.
            if table.running.contains(key) then ZIO.succeed(Next.Continue(table.queue(key, message, replyTo), ()))
            else dispatch(self, table, key, message, replyTo)

          // The outstanding delivery is over: send the next queued message, or let the key go idle.
          case Command.Settled(key) =>
            table.dequeue(key) match
              case None                              => ZIO.succeed(Next.Continue(table.idle(key), ()))
              case Some((message, replyTo), drained) => dispatch(self, drained, key, message, replyTo)

          // The entity was finished before the message reached it: forget it and try a fresh one.
          case Command.Refused(key, message, replyTo) =>
            dispatch(self, table.evict(key), key, message, replyTo)

    /**
     * Resolve `key`'s entity — reusing the live one, or spawning a replacement when there is none or the
     * recorded one has finished — and put `message` on its way.
     *
     * @param self    the distributor's mailbox, for the delivery to report back to
     * @param table   the table to resolve against
     * @param key     the entity key
     * @param message the message to deliver
     * @param replyTo the promise the caller is waiting on
     * @return continue, with the resolved entity recorded and the key marked busy
     */
    private def dispatch(
      self: Self[Cmd],
      table: State,
      key: K,
      message: I,
      replyTo: ReplyTo[E, O],
    ): IO[E, Next[State, Unit]] =
      for
        actor <- resolve(table, key)
        _     <- forward(self, key, actor, message, replyTo)
      yield Next.Continue(table.serving(key, actor).busy(key, replyTo), ())

    /**
     * The entity for `key`: the recorded one if it is still alive, else a fresh one. A finished entity is
     * simply replaced — it has already released everything it held, so forgetting it is the whole of
     * retiring it.
     *
     * The liveness check is a snapshot and can go stale a moment later; that case is caught by the delivery
     * itself, which comes back as [[Command.Refused]]. Checking here only saves the wasted round trip.
     *
     * @param table the table to look in
     * @param key   the entity key
     * @return the entity to deliver to
     */
    private def resolve(table: State, key: K): IO[E, Running[E, I, O]] =
      table.alive.get(key) match
        case None        => spawn
        case Some(actor) => actor.terminated.flatMap(finished => if finished then spawn else ZIO.succeed(actor))

    /**
     * A fresh entity on the pool's runtime, with a key and a scope of its own. It retires itself when it
     * finishes, so the pool never has to stop one — only stop pointing at it.
     *
     * @return the new entity
     */
    private def spawn: UIO[Running[E, I, O]] =
      Actor.spawnIn(runtime)(behaviour)(ZIO.unit)

    /**
     * Hand `message` to `actor` off the loop, and turn whatever happens into a message back to the loop.
     *
     * Every outcome produces exactly one command, and that is the invariant the pool rests on: a key stays
     * `running` until its delivery reports back, so an outcome that goes missing wedges the key and strands
     * everything queued behind it. Hence [[ZIO.exit]] rather than a success path — a failed or defective step
     * still settles — and an interrupt hook, for the delivery that never gets to finish at all.
     *
     * A refusal is the one outcome the caller is not told about: the message never ran, so the promise is
     * left alone — and its permit stays held, because the message is still in flight — and the pool retries
     * it against a replacement.
     *
     * @param self    the mailbox the outcome is sent to
     * @param key     the key whose delivery this is
     * @param actor   the entity to deliver to
     * @param message the message to deliver
     * @param replyTo the promise the caller is waiting on
     * @return noop once the delivery is forked
     */
    private def forward(
      self: Self[Cmd],
      key: K,
      actor: Running[E, I, O],
      message: I,
      replyTo: ReplyTo[E, O],
    ): UIO[Unit] = {
      val delivery: UIO[Cmd] =
        actor.ask(message).exit.flatMap {
          case Exit.Failure(cause) if cause.failureOption.contains(Terminated) =>
            ZIO.succeed(Command.Refused(key, message, replyTo))
          case settled                                                         =>
            // The permit belongs to the message, not the delivery: it goes back exactly when the caller is
            // answered, so a message retried against a replacement keeps holding it.
            replyTo.done(settled) *> permit.release.as(Command.Settled(key))
        }

      self.pipeToSelf(delivery.onInterrupt(self.send(Command.Settled(key))))
    }
  }

  /**
   * The caller's view of the pool. Every call hands the distributor a promise and walks away with it; the
   * distributor answers it whenever the message is eventually delivered, however many spawns and retries
   * that takes.
   *
   * @param distributor the routing entity
   * @param permit      the in-flight bound; taken here, given back when the pool answers
   */
  final private class Handle[E, I, K, O](
    distributor: Running[E, Command[E, I, K, O], Unit],
    permit: KeyedQueue.Permit,
  ) extends Running[E, I, O] {

    def ask(message: I): IO[E | Terminated, O] =
      accept(message).flatMap(_.await)

    def send(message: I): IO[E | Terminated, Unit] =
      accept(message).unit // the caller waits for a permit, never for the answer

    def expect[E2 >: E, A](fn: Promise[E2, A] => I): IO[E2 | Terminated, A] =
      for
        promise <- Promise.make[E2, A]
        _       <- ask(fn(promise))
        result  <- promise.await
      yield result

    /**
     * Take a permit and get the message *into the pool's table* — an `ask` rather than a `send`, so this
     * returns only once the distributor's step has run and the message is somewhere teardown can find it.
     *
     * That handshake is what closes the seam between the two custodians. A message merely queued on the
     * distributor's mailbox is known to the runtime but not to the routing table: the runtime's `drop` would
     * see no promise to release (a `send` carries none) and the table would not know it existed, so a pool
     * closing with a backlog would strand exactly the callers it had most recently taken on. Waiting for the
     * step means the message is always owned by one of the two, never in between.
     *
     * The cost is that a producer waits for one map lookup, a possible spawn, and a fork — not for its
     * answer. Only the wait for a *permit* is interruptible: a producer parked there holds nothing, whereas
     * one interrupted mid-handshake could not tell "never accepted" from "accepted, and I stopped looking" —
     * and would return a permit the pool is still going to return itself, quietly inflating the bound. That
     * wait is bounded by the distributor's backlog, which is bounded by `maxInFlight` whenever one is set.
     *
     * @param message the message to submit
     * @return the promise the pool will answer on; aborts if the pool never took the message on
     */
    private def accept(message: I): IO[E | Terminated, ReplyTo[E, O]] =
      ZIO.uninterruptibleMask: restore =>
        for
          _       <- restore(permit.acquire)
          replyTo <- Promise.make[E | Terminated, O]
          _       <- distributor
                       .ask(Command.Deliver(message, replyTo))
                       // Uninterruptible, so a failure here means one thing only: the pool never took it.
                       .onExit(exit => permit.release.unless(exit.isSuccess).unit)
        yield replyTo

    /**
     * A pool has no single lifetime: its entities finish individually and are replaced on demand, so there is
     * no moment at which the pool is done. It ends when its scope closes, and that is what a caller holds.
     *
     * @return never completes
     */
    def await: UIO[O] = ZIO.never

    /**
     * @return always `false` — see [[await]]
     */
    def terminated: UIO[Boolean] = ZIO.succeed(false)
  }
}
