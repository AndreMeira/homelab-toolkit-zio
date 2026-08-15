package homelab.common.processing


import zio.*
import scala.collection.immutable


trait Worker:
  def submit[E, A](effect: ZIO[Scope, E, A]): UIO[Promise[E, A]]


object Worker {
  trait Task:
    def run: UIO[Unit]
    def drop: UIO[Unit]

  object Task:
    def make[E, A](effect: ZIO[Scope, E, A], promise: Promise[E, A]): URIO[Scope, Task] =
      for {
        scope <- ZIO.scope
      } yield new Task:
        override def drop: UIO[Unit] = promise.interrupt.unit
        override def run: UIO[Unit]  = promise.isDone.flatMap:
          case true  => ZIO.unit
          case false => scope.extend(effect.onExit(exit => promise.done(exit)).ignore.unit)

  /**
   * The queue, plus whether a drain fiber is on duty. One cell, because "must I start a drain?" and "is there
   * anything left to run?" have to be decided against the same instant: a submission landing while the drain
   * sits between its last task and noticing an empty queue must join that drain, not fork a second one.
   *
   * @param pending  the tasks waiting, in submission order, each with the scope it runs in
   * @param draining whether a drain is on duty — true from the submission that started one until that drain
   *                 finds the queue empty, which is a longer window than "a task is running"
   */
  private final case class Backlog(pending: immutable.Queue[(Task, Scope.Closeable)], draining: Boolean)

  private class Live(
    scope: Scope,
    backlog: Ref[Backlog],
  ) extends Worker {

    /**
     * Take the work on: give it a scope of its own, a promise to answer on, and a place in the queue —
     * starting a drain if none is on duty.
     *
     * Uninterruptible as a whole, and cheaply so, because nothing here suspends: a scope fork, a promise, a
     * finalizer and one `Ref` update. What it buys is that the bookkeeping cannot be torn in half. An
     * interrupt landing between [[enqueue]] and the fork would leave the queue holding work with no drain and
     * `draining` claiming otherwise, so no later submission would ever start one — the worker would accept
     * messages and silently run nothing. An earlier one would strand `child`, registered on the worker's
     * scope with nothing left to close it.
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
      ZIO.uninterruptible {
        for {
          child   <- scope.fork
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

    private def drain: UIO[Unit] = dequeue.flatMap:
      case None                => ZIO.unit
      case Some(task -> scope) => scope.use(task.run) *> drain
  }

}
