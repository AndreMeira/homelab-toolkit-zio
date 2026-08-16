package homelab.incubator.processing.actor.v7

import homelab.common.error.ApplicationError.AdapterError
import homelab.common.flow.Permit
import homelab.common.messaging.Partitioner
import Actor.{ Running, Terminated }
import zio.*


/**
 * A keyed pool of entities behind one [[Running]] façade: messages are partitioned, each key gets its own
 * entity, and an entity that finishes is replaced by the next message for its key. What a caller holds looks
 * like a single entity.
 *
 * **It is a registry, not a scheduler.** Ordering and one-at-a-time are the entities' own doing — each has a
 * mailbox — so the pool keeps nothing but `key -> entity`. There is no in-flight tracking, no per-key queue,
 * and no delivery protocol, because there is nothing here for two messages to race over: whichever handle
 * they resolve, that entity serialises them.
 *
 * **Staleness is caught, not prevented.** Resolving and delivering are two steps, so an entity can finish in
 * between and refuse the message. That is what [[Actor.Terminated]] is for: the pool drops the dead handle,
 * spawns a replacement, and delivers again, all invisible to the caller. Because a finished entity stays
 * finished, this retries at most once per death rather than looping.
 *
 * **Backpressure is per entity.** A `mailbox` bound is passed to each one, so a slow entity makes its own
 * producers wait without a pool-wide ceiling that would couple unrelated keys.
 */
object Distributed {

  /**
   * Start a pool over `behaviour` in the calling scope. Nothing runs until the first message; entities are
   * spawned on demand and retire themselves when they finish.
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
      scope <- ZIO.scope
      // Validate the bound once, here, where a caller can still see the failure — spawning happens later,
      // on a message, where there is nowhere sensible to report it.
      _     <- Permit.make(mailbox)
      table <- Ref.Synchronized.make(Map.empty[K, Running[E, I, O]])
    yield new Live(scope, behaviour, key.get, mailbox, table)

  /**
   * The pool. `Ref.Synchronized` rather than a plain `Ref` because resolving is a read-then-*spawn*-then-write
   * and spawning is effectful: two callers racing on an unseen key would otherwise both spawn, and one of the
   * two entities would be dropped on the floor with its scope already registered.
   *
   * @param scope     the scope entities are spawned into — they outlive the message that created them
   * @param behaviour the behaviour every entity is spawned with
   * @param partition extracts an entity key from a message
   * @param mailbox   the bound each entity's mailbox gets
   * @param table     the registry, `key -> entity`
   */
  final private class Live[E >: AdapterError, I, K, S, O](
    scope: Scope,
    behaviour: Actor[E, I, S, O],
    partition: I => K,
    mailbox: Option[Int],
    table: Ref.Synchronized[Map[K, Running[E, I, O]]],
  ) extends Running[E, I, O] {

    def ask(message: I): IO[E | Terminated, O] = deliver(message)(_.ask(message))

    def send(message: I): IO[E | Terminated, Unit] = deliver(message)(_.send(message))

    /**
     * A pool has no single lifetime: its entities finish individually and are replaced on demand, so there is
     * no moment at which the pool is done. It ends when its scope closes, and that is what a caller holds.
     *
     * @return never completes
     */
    def await: UIO[O] = ZIO.never

    /**
     * Resolve `message`'s entity and run `delivery` against it, replacing the entity and trying again if it
     * turns out to have finished.
     *
     * Terminates: the replacement is freshly spawned, so it cannot already be finished, and a second refusal
     * would mean it finished on this very message — which is a step that ran, not a refusal.
     *
     * @param message  the message whose key selects the entity
     * @param delivery what to do with the resolved entity
     * @tparam A the delivery's result
     * @return the delivery's result; aborts with `E` if the entity's step fails
     */
    private def deliver[A](message: I)(delivery: Running[E, I, O] => IO[E | Terminated, A]): IO[E | Terminated, A] =
      val key = partition(message)
      resolve(key).flatMap: entity =>
        delivery(entity).catchAll:
          case Terminated => retire(key, entity) *> resolve(key).flatMap(delivery)
          case failure    => ZIO.fail(failure)

    /**
     * The entity for `key`, spawning one if the registry has never seen it. The spawn happens under the
     * registry's lock, so one key can never end up with two entities.
     *
     * @param key the entity key
     * @return the entity serving it
     */
    private def resolve(key: K): UIO[Running[E, I, O]] =
      table.modifyZIO: entities =>
        entities.get(key) match
          case Some(entity) => ZIO.succeed(entity -> entities)
          case None         => spawn.map(entity => entity -> entities.updated(key, entity))

    /**
     * Forget `stale`, unless the registry has already moved on to something else — two callers can discover
     * the same death, and the second must not evict the replacement the first installed.
     *
     * Nothing is stopped here: a finished entity has already released its mailbox and its background work.
     * Forgetting it *is* retiring it.
     *
     * @param key   the entity key
     * @param stale the entity found to be finished
     * @return noop once the registry no longer points at `stale`
     */
    private def retire(key: K, stale: Running[E, I, O]): UIO[Unit] =
      table.update(entities => if entities.get(key).exists(_ eq stale) then entities - key else entities)

    /**
     * A fresh entity in the pool's scope, so it outlives the message that caused it to be spawned.
     *
     * Spawning can only fail on a non-positive mailbox bound, which [[Distributed.make]] already rejected, so
     * reaching that here would mean the pool was built by some other route — a defect, not a failure.
     *
     * @return the new entity
     */
    private def spawn: UIO[Running[E, I, O]] =
      scope
        .extend(Actor.spawn(behaviour, mailbox = mailbox))
        .orDieWith(invalid => new IllegalStateException(invalid.message))
  }
}
