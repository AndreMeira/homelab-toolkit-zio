package homelab.incubator.processing.actor.v7

import homelab.common.error.ApplicationError.AdapterError
import homelab.common.flow.Permit
import homelab.common.messaging.Partitioner
import Actor.{ Next, Running, Self, Terminated }
import zio.*


/**
 * The same keyed pool as [[Distributed]], with the routing table kept as an actor's loop state instead of in
 * a `Ref.Synchronized`. Written to be compared against it, not to replace it.
 *
 * **Everything goes through one entity.** A router actor owns the table, so resolving, spawning and evicting
 * are steps in one serialised loop — the exclusion comes from the mailbox rather than from a lock. Nothing
 * else can touch the table, so no atomic read-modify-write primitive appears anywhere.
 *
 * **The router never waits for a child.** It forks the delivery and its step ends, or every entity in the
 * pool would queue behind one mailbox. That is why a caller cannot be answered by the router's reply, and why
 * messages travel as an [[Envelope]] carrying the promise the caller is waiting on.
 *
 * **A refusal is repaired by redelivery.** The forking fiber cannot touch the table — it is loop state — so
 * on [[Actor.Terminated]] it sends the envelope back to the router, tagged with the entity that refused it.
 * The router evicts that entity if the table still points at it and delivers again, which terminates because
 * the replacement is freshly spawned, and stays correct when two callers discover the same death: the second
 * one's tag no longer matches, so it does not evict the replacement the first installed.
 */
object Routed {

  /**
   * Start a pool over `behaviour` in the calling scope.
   *
   * @param behaviour the behaviour every entity is spawned with
   * @param mailbox   the bound each entity's mailbox gets; `None` for unbounded
   * @param key       partitions a message to its entity key
   * @tparam E the error a transition may fail with — must admit `AdapterError`
   * @tparam I the message accepted
   * @tparam K the entity key
   * @tparam S the entity state
   * @tparam O the reply produced
   * @return the pool, inert until its first message
   */
  def make[E >: AdapterError, I, K, S, O](
    behaviour: Actor[E, I, S, O],
    mailbox: Option[Int] = None,
  )(using
    key: Partitioner.Key[I] { type Type = K }
  ): ZIO[Scope, Permit.Error, Running[E, I, O]] =
    for
      scope  <- ZIO.scope
      _      <- Permit.make(mailbox) // reject a bad bound here, where a caller can still see it
      router <- Actor.spawn(new Router[E, I, K, S, O](scope, behaviour, key.get, mailbox))
    yield new Live(router)

  /**
   * A message on its way to an entity, with what the router needs to finish the job: the promise the caller is
   * waiting on, and — on a redelivery — the entity that refused it.
   *
   * Both fields exist because the router does not deliver synchronously. The promise is how a caller is
   * answered by something other than the router's own reply; `stale` is how the forking fiber tells the table
   * what it learned, since it cannot reach the table itself.
   *
   * @param message the caller's message
   * @param replyTo the promise its outcome is routed to
   * @param stale   the entity that refused this message, on a redelivery
   */
  final private case class Envelope[E, I, O](
    message: I,
    replyTo: Promise[E | Terminated, O],
    stale: Option[Running[E, I, O]],
  )

  /**
   * The router: one entity whose state is the whole table.
   *
   * @param scope     the scope entities and deliveries are forked into
   * @param behaviour the behaviour every entity is spawned with
   * @param partition extracts an entity key from a message
   * @param mailbox   the bound each entity's mailbox gets
   */
  final private class Router[E >: AdapterError, I, K, S, O](
    scope: Scope,
    behaviour: Actor[E, I, S, O],
    partition: I => K,
    mailbox: Option[Int],
  ) extends Actor[E, Envelope[E, I, O], Map[K, Running[E, I, O]], Unit] {

    private type Table = Map[K, Running[E, I, O]]

    /**
     * @param message ignored — a pool starts empty whatever arrives first
     * @return the empty table
     */
    def init(message: Envelope[E, I, O]): IO[E, Table] = ZIO.succeed(Map.empty)

    /**
     * Evict what the envelope reports dead, resolve the key, put the message on its way. Always continues: a
     * router has nothing to finish.
     *
     * @param self this router's own mailbox, where refused deliveries come back
     * @return the transition
     */
    def next(self: Self[Envelope[E, I, O]]): (input: Envelope[E, I, O], state: Table) => IO[E, Next[Table, Unit]] =
      (envelope, table) =>
        val key    = partition(envelope.message)
        val pruned = envelope.stale.fold(table): dead =>
          if table.get(key).exists(_ eq dead) then table - key else table

        for
          entity <- pruned.get(key).fold(spawn)(ZIO.succeed(_))
          _      <- forward(self, key, entity, envelope)
        yield Next.Continue(pruned.updated(key, entity), ())

    /**
     * Hand the message to `entity` off the loop, and turn a refusal into a redelivery.
     *
     * Forked into the pool's scope rather than awaited: the router must not wait for a child, or every key in
     * the pool would queue behind this one step.
     *
     * @param self     the router's mailbox, for a refused envelope to come back to
     * @param key      the entity key — unused by the delivery, carried for readability at the call site
     * @param entity   the entity to deliver to
     * @param envelope the message and the promise it answers
     * @return noop once the delivery is forked
     */
    private def forward(
      self: Self[Envelope[E, I, O]],
      key: K,
      entity: Running[E, I, O],
      envelope: Envelope[E, I, O],
    ): UIO[Unit] =
      entity
        .ask(envelope.message)
        .exit
        .flatMap:
          case Exit.Failure(cause) if isTerminated(cause) => self.send(envelope.copy(stale = Some(entity)))
          case settled                                    => envelope.replyTo.done(settled).unit
        .forkIn(scope)
        .unit

    /** A fresh entity in the pool's scope, so it outlives the step that spawned it. */
    private def spawn: IO[E, Running[E, I, O]] =
      scope
        .extend(Actor.spawn(behaviour, mailbox = mailbox))
        .orDieWith(invalid => new IllegalStateException(invalid.message))

    private def isTerminated(cause: Cause[E | Actor.Terminated.type]): Boolean =
      cause.failureOption.contains(Terminated)
  }

  /**
   * The caller's view: hand the router an envelope and walk away with its promise.
   *
   * @param router the routing entity
   */
  final private class Live[E, I, K, O](router: Running[E, Envelope[E, I, O], Unit]) extends Running[E, I, O] {

    def ask(message: I): IO[E | Terminated, O] = accept(message).flatMap(_.await)

    def send(message: I): IO[E | Terminated, Unit] = accept(message).unit

    /**
     * A pool has no single lifetime: its entities finish individually and are replaced on demand. It ends when
     * its scope closes, and that is what a caller holds.
     *
     * @return never completes
     */
    def await: UIO[O] = ZIO.never

    /**
     * Queue `message` at the router with a promise for its answer.
     *
     * @param message the message to route
     * @return the promise the pool will answer on
     */
    private def accept(message: I): IO[E | Terminated, Promise[E | Terminated, O]] =
      for
        replyTo <- Promise.make[E | Terminated, O]
        _       <- router.send(Envelope(message, replyTo, None))
      yield replyTo
  }
}
