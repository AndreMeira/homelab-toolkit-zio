package homelab.incubator.processing.actor.v5

import zio.{ Ref, Scope, UIO, ZIO }

import scala.collection.immutable.Queue


/**
 * The scheduling substrate actors run on — a keyed serial executor, and nothing else. It owns a scope, a
 * per-key backlog and the drain fibers; it knows nothing of behaviours, entity state or replies, so one
 * `Runtime` carries as many actors as a node cares to run.
 *
 * Serialisation is by [[Runtime.Key]] — an identity token the caller allocates and keeps — so what a key
 * *means* stays the caller's business while the runtime is handed something it cannot confuse with a message
 * or a state.
 *
 * **Fork-on-send, no start.** Construction is inert: the first task submitted for a dormant key forks that
 * key's drain into the scope. A key is "in flight" exactly while its backlog is non-empty; its drain runs
 * one task at a time (per-key FIFO — the actor guarantee) and releases the key when the backlog empties.
 * Distinct keys drain on independent fibers, so cross-key work is concurrent for free.
 */
private[v5] trait Runtime:

  /**
   * Enqueue `task` under `key`, to run after everything already queued for that key and never beside it.
   *
   * @param key  the serialisation key — tasks sharing one run strictly in submission order
   * @param task the work to run when its turn comes
   * @return noop once queued (and any drain forked); does not await the task
   */
  def submit(key: Runtime.Key)(task: Runtime.Task): UIO[Unit]

  /**
   * A scope of an actor's own, forked from the runtime's, for what belongs to one actor rather than to the
   * runtime: background work it forks, and whatever must be released when it finishes.
   *
   * Forked rather than free-standing, so it closes with the runtime if the actor never finishes — and
   * deregisters from the runtime when closed early, so a long-lived runtime with heavy turnover accumulates
   * nothing.
   *
   * @return the new scope, closed by whoever asked for it
   */
  def child: UIO[Scope.Closeable]


private[v5] object Runtime:

  /**
   * A serialisation slot: everything submitted under one `Key` runs in submission order, one at a time.
   *
   * Deliberately empty and deliberately a `class` — a `Key` carries no data and compares by *identity*, so
   * two of them are the same slot only if they are literally the same object. The caller allocates one per
   * thing it wants serialised (an entity allocates one for itself; a keyed pool would hold a `Map[K, Key]`)
   * and holds onto it. Nothing else can be mistaken for a key, and no two callers can collide by having
   * built equal values.
   */
  final class Key

  /**
   * One unit of serialised work, with the two ways it can end: it runs, or the runtime shuts down while it
   * is still queued and it is dropped. Both are total — a `Task` never fails, because the runtime has no
   * error channel to put a failure in and no promise to route it to. Whoever builds the task owns that:
   * catching the step's failure and completing a caller's promise happens inside [[run]].
   */
  trait Task:

    /**
     * Perform the work. Runs on the key's drain fiber, so it must not fail — a failure has nowhere to go
     * but the drain, and would take the rest of the key's backlog with it.
     *
     * @return noop when the work has settled
     */
    def run: UIO[Unit]

    /**
     * Called *instead of* [[run]] when the runtime's scope closes while this task is still queued. The hook
     * exists because a task that never starts can never be interrupted: without it, a caller awaiting this
     * task's reply would hang for the life of its fiber.
     *
     * @return noop once the task's caller has been released
     */
    def drop: UIO[Unit]

  /**
   * Build a `Runtime` in the calling scope. Nothing is started; a finalizer drops whatever is still queued
   * when that scope closes (tasks already running are interrupted with their drain fibers, and actors' own
   * scopes close with it).
   *
   * @return the runtime, inert until its first [[Runtime.submit]]
   */
  def make: ZIO[Scope, Nothing, Runtime] =
    for
      scope   <- ZIO.scope
      backlog <- Ref.make(Map.empty[Key, Queue[Task]])
      runtime  = new Live(scope, backlog)
      _       <- ZIO.addFinalizer(runtime.abandon)
    yield runtime

  /**
   * The only [[Runtime]]. Split out from [[make]] so the drain machinery can be named and documented rather
   * than nested in a constructor.
   *
   * @param scope   the scope drains and actors' own scopes are forked from
   * @param backlog per-key queues: a key present is in flight, absent is idle
   */
  private final class Live(scope: Scope, backlog: Ref[Map[Key, Queue[Task]]]) extends Runtime:

    /**
     * Enqueue and, if the key was idle, fork its drain. The pair is uninterruptible so an interrupt cannot
     * land *between* them and strand a freshly non-idle key with no drain; the drain body itself is
     * `interruptible` so scope close can kill it.
     *
     * @param key  the serialisation key
     * @param task the work to enqueue
     * @return noop once queued and any drain forked
     */
    def submit(key: Key)(task: Task): UIO[Unit] =
      ZIO.uninterruptible:
        enqueue(key, task).flatMap: started =>
          ZIO.when(started)(drain(key).interruptible.forkIn(scope)).unit

    def child: UIO[Scope.Closeable] = scope.fork

    /**
     * Append `task` to its key's queue.
     *
     * @param key  the serialisation key
     * @param task the work to append
     * @return `true` if the key was idle, so the caller must fork its drain; else `false`
     */
    private def enqueue(key: Key, task: Task): UIO[Boolean] =
      backlog.modify: map =>
        map.get(key) match
          case None    => true  -> map.updated(key, Queue(task))
          case Some(q) => false -> map.updated(key, q.enqueue(task))

    /**
     * Claim the key's head task. The key stays present once its queue empties and is released only by a
     * *later* claim that finds it empty. That extra empty step is what makes the drain self-serialising:
     * while the last task is still running the key is present, so a concurrent [[enqueue]] appends
     * (`started == false`, no second drain) — only after the drain observes the empty queue and drops the
     * key does a fresh task re-fork. At most one drain per key ever runs.
     *
     * @param key the serialisation key
     * @return the next task, or `None` when the backlog is empty (releasing the key in the same step)
     */
    private def claim(key: Key): UIO[Option[Task]] =
      backlog.modify: map =>
        map.get(key).fold(Option.empty[Task] -> map): queue =>
          queue.dequeueOption match
            case Some((task, rest)) => Some(task) -> map.updated(key, rest) // stay present, even if rest empty
            case None               => None       -> (map - key)            // empty → release the key

    /**
     * Run one key's backlog to exhaustion, one task at a time, then stop.
     *
     * @param key the serialisation key to drain
     * @return noop; carried on the forked drain fiber
     */
    private def drain(key: Key): UIO[Unit] =
      claim(key).flatMap:
        case Some(task) => task.run *> drain(key)
        case None       => ZIO.unit

    /**
     * Scope finalizer: take the whole backlog and [[Task.drop]] everything still in it, so closing releases
     * queued callers instead of stranding them. Tasks already running are interrupted along with their drain
     * fibers, which is the running task's own business.
     *
     * @return noop once every queued task has been dropped
     */
    def abandon: UIO[Unit] =
      backlog.getAndSet(Map.empty).flatMap(map => ZIO.foreachDiscard(map.values.flatten)(_.drop))
