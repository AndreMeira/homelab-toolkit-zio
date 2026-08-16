package homelab.incubator.processing.actor.v7

import homelab.common.flow.Permit
import zio.*

import scala.collection.immutable


/**
 * A serial executor for scoped effects: everything submitted runs one at a time, in submission order, on a
 * fiber of the worker's rather than the submitter's. Submitting hands back the promise the outcome will be
 * routed to, so a caller chooses whether to wait.
 *
 * Each effect gets a scope of its own, closed once it settles — so a submission may acquire resources without
 * the worker outliving them.
 */
trait Worker:

  /**
   * Queue `effect` to run after everything already submitted, and never beside it.
   *
   * The returned promise is settled with whatever the effect produces — success, typed failure, defect, or
   * interruption — so a failing submission fails its caller and nobody else. It is also the *only* channel:
   * a caller that discards the promise discards the errors with it, since a worker neither logs nor reports
   * what its submissions do. Fire-and-forget here means forgetting the failures too.
   *
   * Interrupting the promise before the effect starts cancels it: the worker skips a submission whose promise
   * is already settled. Once it has started, the promise no longer controls it; interrupting then only
   * changes what the caller sees.
   *
   * @param effect the work to run, in a scope of its own
   * @tparam E the error the effect may fail with
   * @tparam A the value it produces
   * @return the promise its outcome will be routed to; queued, not yet run
   */
  def submit[E, A](effect: ZIO[Scope, E, A]): UIO[Promise[E, A]]

  /**
   * Wait until everything submitted before this call has finished — a barrier, obtained by queueing a marker
   * and awaiting it. Because a worker is serial and first-in-first-out, a marker that has run is proof that
   * everything ahead of it did, including the closing of each submission's scope. So this waits for prior
   * finalizers too, not merely for prior effects.
   *
   * "Caught up", not "idle": work submitted after this call is not waited for, and the worker may well be
   * busy again by the time it returns.
   *
   * On a bounded worker the marker needs a permit like any other submission, so a saturated worker makes this
   * wait for room before it even joins the queue — and it holds one of the bound's slots while it does.
   * Calling it from inside a submission deadlocks for the usual reason: the marker queues behind the
   * submission that is waiting for it.
   *
   * @return noop once everything submitted before this call has settled
   */
  def flush: UIO[Unit] = submit(ZIO.unit).flatMap(_.await)


object Worker {

  /**
   * Build an unbounded worker in the calling scope. Nothing is started; the first submission forks the drain,
   * and closing the scope stops it and releases every queued caller.
   *
   * Unbounded means a fast producer can grow the queue without limit — [[bounded]] is the same worker with a
   * ceiling on unanswered work.
   *
   * @return the worker, inert until its first submission
   */
  def make: ZIO[Scope, Nothing, Worker] = build(Permit.unbounded)

  /**
   * [[make]], with a ceiling: at most `maxInFlight` submissions may be accepted-but-unanswered at once, and a
   * producer that would exceed it waits rather than growing the queue.
   *
   * Because a worker is serial, at most one submission is ever *running*, so this bounds the backlog alone —
   * unlike a keyed pool, where the same number would also cap how much runs at once.
   *
   * It changes what [[Worker.submit]] costs. Unbounded, submitting is a handful of atomic writes and never
   * waits; bounded, it parks while the worker is saturated. That is the point of asking for a bound, but it
   * means a producer that must never block wants [[make]] and its own policy on top.
   *
   * @param maxInFlight accepted-but-unanswered submissions allowed at once; `None` for no bound
   * @return the worker, inert until its first submission; aborts if `maxInFlight` is non-positive
   */
  def bounded(maxInFlight: Option[Int]): ZIO[Scope, Permit.Error, Worker] =
    Permit.make(maxInFlight).flatMap(build)

  /**
   * The worker both factories build, differing only in what bounds it.
   *
   * @param permit the ceiling on unanswered submissions; [[Permit.unbounded]] for none
   * @return the worker, inert until its first submission
   */
  private def build(permit: Permit): ZIO[Scope, Nothing, Worker] =
    for {
      scope   <- ZIO.scope
      backlog <- Ref.make(Backlog(immutable.Queue.empty, draining = false))
    } yield new Live(scope, backlog, permit)

  /**
   * One queued submission, with the two ways it can end: it runs, or the worker goes away while it is still
   * queued and it is dropped. Both are total — a `Task` never fails, because the drain has no error channel
   * to put a failure in. Routing the outcome to the caller's promise happens inside [[run]].
   */
  private trait Task:

    /**
     * Run the effect and settle the caller's promise with whatever it produces. Skips the work entirely if
     * the promise is already settled — which is how a caller cancels a submission it has changed its mind
     * about.
     *
     * @return noop once the effect has settled
     */
    def run: UIO[Unit]

    /**
     * Called *instead of* [[run]] when the worker goes away while this task is still queued: work that will
     * never start cannot be interrupted, so its caller has to be released explicitly.
     *
     * @return noop once the caller has been released
     */
    def drop: UIO[Unit]

  private object Task:

    /**
     * A task over `effect`, answering on `promise`, running in the scope this is built with — which is the
     * submission's own scope, closed by the drain once the task settles.
     *
     * @param effect  the work to run
     * @param promise the caller's channel for its outcome
     * @tparam E the error the effect may fail with
     * @tparam A the value it produces
     * @return the task, bound to the ambient scope
     */
    def make[E, A](effect: ZIO[Scope, E, A], promise: Promise[E, A]): URIO[Scope, Task] =
      for {
        scope <- ZIO.scope
      } yield new Task:
        override def drop: UIO[Unit] = promise.interrupt.unit
        override def run: UIO[Unit]  = promise.isDone.flatMap:
          case true  => ZIO.unit
          case false =>
            scope
              .extend(effect.onExit(exit => promise.done(exit)))
              // The outcome has gone to the promise; what is left must not reach the drain. Interruption is
              // re-raised rather than swallowed, so a worker being torn down stops now instead of claiming
              // one more task first.
              .catchAllCause(cause => if cause.isInterrupted then ZIO.refailCause(cause.stripFailures) else ZIO.unit)
              .unit

  /**
   * The queue, plus whether a drain fiber is on duty. One cell, because "must I start a drain?" and "is there
   * anything left to run?" have to be decided against the same instant: a submission landing while the drain
   * sits between its last task and noticing an empty queue must join that drain, not fork a second one.
   *
   * @param pending  the tasks waiting, in submission order, each with the scope it runs in
   * @param draining whether a drain is on duty — true from the submission that started one until that drain
   *                 finds the queue empty, which is a longer window than "a task is running"
   */
  final private case class Backlog(pending: immutable.Queue[(Task, Scope.Closeable)], draining: Boolean)

  private class Live(
    scope: Scope,
    backlog: Ref[Backlog],
    permit: Permit,
  ) extends Worker {

    /**
     * Take the work on: give it a scope of its own, a promise to answer on, and a place in the queue —
     * starting a drain if none is on duty.
     *
     * Waiting for a permit is the only interruptible part: a producer parked there holds nothing, whereas one
     * interrupted after taking one would lose it to the only party that gives it back. Everything after it is
     * bookkeeping that must not be torn in half — an interrupt landing between [[enqueue]] and the fork would
     * leave the queue holding work with no drain and `draining` claiming otherwise, so no later submission
     * would ever start one and the worker would accept work and silently run none of it. An earlier one would
     * strand `child`, registered on the worker's scope with nothing left to close it. None of that costs
     * anything to guard, because none of it suspends: a scope fork, two finalizers, a promise, one `Ref`
     * update.
     *
     * The permit comes back through a finalizer on the submission's own scope, which closes exactly once on
     * every path — after the task runs, or when the worker closes if it never does. That is why the bound
     * lives in here rather than in a wrapper: from outside, the only observable end of a submission is its
     * promise, so a decorator would need a fiber parked on `await` for each one to learn what this gets for
     * free. An unbounded worker pays nothing for the same lines, since [[Permit.unbounded]]'s acquire and
     * release are no-ops.
     *
     * The drain body is put back to `interruptible` on the way out: a forked fiber inherits the interrupt
     * status of the region that forked it, so without this the drain — an unbounded loop — could never be
     * interrupted, and closing the worker's scope would block on it forever. Interrupting *this* fiber does
     * not touch the drain either way: [[ZIO.forkIn]] forks a daemon whose lifetime belongs to the scope, not
     * to whoever submitted.
     *
     * @param effect the work to run, in a scope closed once it settles
     * @return the promise its outcome will be routed to
     */
    override def submit[E, A](effect: ZIO[Scope, E, A]): UIO[Promise[E, A]] =
      ZIO.uninterruptibleMask { restore =>
        for {
          _       <- restore(permit.acquire)
          child   <- scope.fork
          _       <- child.addFinalizer(permit.release)
          promise <- Promise.make[E, A]
          task    <- child.extend(Task.make(effect, promise))
          _       <- child.addFinalizer(task.drop)
          start   <- enqueue(task, child)
          _       <- ZIO.when(start)(drain.interruptible.forkIn(scope)).unit
        } yield promise
      }

    /**
     * Append `task`, and report whether this submission is the one that has to start the drain.
     *
     * Sets `draining` unconditionally: after this step a drain is always on duty — either the one already
     * running, or the one this caller is about to fork.
     *
     * @param task  the work to append
     * @param owned the scope that task runs in
     * @return `true` if the caller must fork a drain, else `false`
     */
    private def enqueue(task: Task, owned: Scope.Closeable): UIO[Boolean] =
      backlog.modify: current =>
        !current.draining -> Backlog(current.pending.enqueue(task -> owned), draining = true)

    /**
     * Take the next task, or stand the drain down — decided in the same step that finds the queue empty, so
     * a concurrent [[enqueue]] either lands before it (and gets drained) or after it (and forks a fresh
     * drain), never in between.
     *
     * @return the next task and its scope, or `None` once the queue is empty
     */
    private def dequeue: UIO[Option[(Task, Scope.Closeable)]] =
      backlog.modify: current =>
        current.pending.dequeueOption match
          case Some(head -> rest) => Some(head) -> current.copy(pending = rest)
          case None               => None       -> current.copy(draining = false)

    /**
     * Run the queue to exhaustion, one task at a time, then stand down. Each task runs inside its own scope
     * via `use`, so that scope is closed — and its resources released — as soon as the task settles, whether
     * it succeeded, failed, or was interrupted.
     *
     * Carried on a fiber owned by the worker's scope, so the only thing that stops it early is the worker
     * being closed. That is deliberate and one-way: an interrupted drain never reaches the [[dequeue]] that
     * would clear `draining`, so a worker whose drain has been killed accepts submissions and runs none of
     * them. It is not built to be restarted.
     *
     * @return noop once the queue is empty
     */
    private def drain: UIO[Unit] = dequeue.flatMap:
      case None                => ZIO.unit
      case Some(task -> owned) => owned.use(task.run) *> drain
  }

}
