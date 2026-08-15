package homelab.common.flow


import homelab.common.error.ApplicationError
import zio.*
import zio.stm.TSemaphore


/**
 * A count of how much may be in flight at once: [[acquire]] takes one and suspends while none are left,
 * [[release]] gives one back and admits a waiter. Whoever holds a permit owes a release — a bound only holds
 * if every taken permit comes back on every path, including failure and interruption.
 *
 * What "in flight" means is the holder's business: a backlog cap ([[KeyedQueue]] takes one per queued value
 * and returns it when the value is claimed), a limit on accepted-but-unanswered work
 * ([[homelab.common.processing.Worker.bounded]]), or a cap on concurrent work. The unbounded variant makes
 * all of that free, so a bound can be optional without the code that uses one growing a branch.
 */
trait Permit:

  /**
   * Take a permit, suspending while none are available.
   *
   * @return noop once a permit is held
   */
  def acquire: UIO[Unit]

  /**
   * Return a permit, admitting a suspended [[acquire]].
   *
   * @return noop once the permit is released
   */
  def release: UIO[Unit]


object Permit:

  /**
   * A failure constructing a [[Permit]]. An [[ApplicationError.ImplementationError]] because it signals a
   * misused API (a violated construction invariant), not a runtime or domain condition.
   */
  enum Error extends ApplicationError.ImplementationError:

    /** The bound was set to a non-positive value, which cannot bound anything. */
    case NonPositiveBound(value: Int)

    override def message: String = this match
      case NonPositiveBound(value) => s"bound must be positive, was $value"

  /**
   * A permit bounded to `bound` holders at once, or an unbounded (never-blocking) one when `None`.
   *
   * @param bound how many may hold a permit at once; must be positive when set, unbounded when `None`
   * @return the permit; aborts with `Error.NonPositiveBound` when `bound` is non-positive
   */
  def make(bound: Option[Int]): IO[Error, Permit] = bound match {
    case Some(n) if n <= 0 => ZIO.fail(Error.NonPositiveBound(n))
    case Some(n)           => TSemaphore.makeCommit(n).map(Permit.bounded)
    case None              => ZIO.succeed(Permit.unbounded)
  }

  /** A permit whose `acquire`/`release` are no-ops — no backpressure. */
  def unbounded: Permit = new Permit:
    override def acquire: UIO[Unit] = ZIO.unit
    override def release: UIO[Unit] = ZIO.unit

  /**
   * A permit backed by a semaphore, bounded to its permit count.
   *
   * @param sem the semaphore doing the bounding
   * @return the permit
   */
  def bounded(sem: TSemaphore): Permit = new Permit:
    override def acquire: UIO[Unit] = sem.acquire.commit
    override def release: UIO[Unit] = sem.release.commit
