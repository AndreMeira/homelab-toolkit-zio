package homelab.common.flow

import zio.*


/**
 * Sections of work that a shutdown waits for. The logic that knows a section must not be cut off runs it
 * through [[graceful]], and [[await]] waits for every running section before the application closes its
 * scopes.
 *
 * Once [[await]] has begun, work is refused by parking it: it never fails, and stays interruptible, so the
 * scope closing afterwards interrupts it like any other waiting fiber. Parking rather than failing keeps a
 * refusal from tearing down a fail-fast caller. Two things are refused:
 *
 *   - '''a new section''', which never starts;
 *   - '''a [[gated]] effect inside a section admitted earlier''', which never starts either. It is refused by
 *     interrupting the fiber with this instance's own refusal, so whatever runs it settles it as interrupted
 *     work (a consumer's adapter hands its value back at once). The section ends with that refusal, is
 *     counted out, and then parks, outside the count, so [[await]] is not held up.
 *
 * That second form is what a drain over a fetching intake needs: the fetch runs as a section, and the work on
 * what it brings back is gated. The consumer drains, `Consumer.drainLocal` and `Consumer.globalDrain`, are
 * built this way.
 *
 * '''`await` must run before the scope interrupts the work''', so the application adds it as the last
 * finalizer of its scope, after starting whatever runs the sections. Anything that keeps forking into that
 * scope after startup installs finalizers after `await`'s, which run first; the processors in
 * `homelab.common.processing` fork into a scope of their own for that reason.
 *
 * '''`graceful` protects the section, and nothing after it.''' Once the last section returns, [[await]]
 * returns and the scope may interrupt the fiber that ran it before the code after the section has run. What
 * must happen belongs inside the section.
 *
 * A section must not run another through [[graceful]] inside itself: once shutdown has begun the inner one
 * parks, and the outer one, which [[await]] is waiting for, never finishes.
 *
 * @param state whether shutdown has begun, and how many sections are running
 * @param drained completed when the last running section finishes after shutdown has begun
 * @param refusal who a [[gated]] effect is interrupted by, so [[graceful]] parks this instance's refusals and
 *                lets every other interruption through
 */
final class Shutdown private (state: Ref[Shutdown.State], drained: Promise[Nothing, Unit], refusal: FiberId):

  /**
   * Run `section` as work a shutdown waits for, or park it forever if shutdown has already begun. A section
   * that ends with this instance's refusal (a [[gated]] effect refused inside it) is counted out, then parks.
   *
   * The count is taken and released inside one uninterruptible region, with only `section` and the parking
   * restored, so an interruption can never leave a section counted in that has stopped running. The refusal
   * is caught after the count is released, so the parking does not hold up [[await]].
   *
   * Only the refusal '''alone''' parks: a cause that also holds a failure or a defect (a cleanup that died on
   * the way out, say) is not a refusal any more, and reaches the caller. So does any other interruption,
   * including one `section` raises itself, and another instance's refusal.
   *
   * @param section the work that should not be cut off
   * @tparam R the environment `section` needs
   * @tparam E the error `section` aborts with
   * @tparam A the value `section` produces
   * @return the value of `section`; aborts with `E` when `section` does; never completes, until interrupted,
   *         when shutdown has begun or when `section` ended with this instance's refusal
   */
  def graceful[R, E, A](section: ZIO[R, E, A]): ZIO[R, E, A] =
    ZIO.uninterruptibleMask: restore =>
      enter.flatMap: admitted =>
        if admitted then
          restore(section)
            .ensuring(leave)
            .catchSomeCause:
              case cause if refusedAlone(cause) => restore(ZIO.never)
        else restore(ZIO.never)

  /**
   * Begin shutdown: refuse every section started from now on, then wait for the running ones to finish.
   * Calling it again waits for the same sections.
   *
   * @return noop once no section is running; never fails
   */
  def await: UIO[Unit] =
    state
      .modify(current => (current.running == 0, current.copy(closing = true)))
      .flatMap(idle => if idle then ZIO.unit else drained.await)

  /**
   * Whether shutdown has begun.
   *
   * @return true once [[await]] has been called
   */
  def closing: UIO[Boolean] = state.get.map(_.closing)

  /**
   * Run `effect`, or refuse it once shutdown has begun: the refusal interrupts the fiber with this instance's
   * own id, before `effect` starts. Use it inside a [[graceful]] section, which then parks on the refusal.
   *
   * It is an interruption rather than a failure so that whatever runs the effect settles it as it settles any
   * interrupted work (a consumer's adapter hands the value back at once), and so that no error type has to
   * make room for it. Being a self-interruption, it can be caught, which is what [[graceful]] does, outside
   * its count, so it never reaches a fail-fast caller. Outside any section, the refusal reaches the caller as
   * an interruption.
   *
   * @param effect the work to run unless shutdown has begun
   * @tparam R the environment `effect` needs
   * @tparam E the error `effect` aborts with
   * @tparam A the value `effect` produces
   * @return the value of `effect`; aborts with `E` when `effect` does; is interrupted by this instance's
   *         refusal, without running `effect`, once shutdown has begun
   */
  def gated[R, E, A](effect: ZIO[R, E, A]): ZIO[R, E, A] =
    closing.flatMap:
      case true  => ZIO.interruptAs(refusal)
      case false => effect

  /**
   * Whether `cause` is this instance's refusal and nothing else.
   *
   * @param cause how a section ended
   * @return true when `cause` is an interruption by [[gated]]'s refusal alone
   */
  private def refusedAlone(cause: Cause[Any]): Boolean =
    cause.isInterruptedOnly && cause.interruptors == Set(refusal)

  /**
   * Count a section in, unless shutdown has begun.
   *
   * @return whether the section may run
   */
  private def enter: UIO[Boolean] =
    state.modify: current =>
      if current.closing then (false, current)
      else (true, current.copy(running = current.running + 1))

  /**
   * Count a section out, and complete `drained` if it was the last one running after shutdown began.
   *
   * @return noop
   */
  private def leave: UIO[Unit] =
    state
      .modify: current =>
        val running = current.running - 1
        (current.closing && running == 0, current.copy(running = running))
      .flatMap(last => drained.succeed(()).when(last).unit)


object Shutdown:

  /**
   * A fresh instance, with no section running and shutdown not begun.
   *
   * Its refusals are interrupted by the id of a fiber forked and interrupted here, so they carry an id the
   * runtime minted for this instance rather than one built by hand. A fiber id compares by its number, start
   * time and fork site together, so no other fiber's interruption is mistaken for a refusal. The fiber is
   * forked interruptible, so this completes even inside an uninterruptible region, such as the acquisition
   * of a resource whose release is [[Shutdown.await]].
   *
   * @return the instance; never fails
   */
  def make: UIO[Shutdown] =
    for
      state   <- Ref.make(State(closing = false, running = 0))
      drained <- Promise.make[Nothing, Unit]
      refusal <- ZIO.never.interruptible.fork.flatMap(fiber => fiber.interrupt.as(fiber.id))
    yield new Shutdown(state, drained, refusal)

  /**
   * Where a [[Shutdown]] stands.
   *
   * @param closing whether [[Shutdown.await]] has been called
   * @param running how many sections are running
   */
  final private[Shutdown] case class State(closing: Boolean, running: Int)
