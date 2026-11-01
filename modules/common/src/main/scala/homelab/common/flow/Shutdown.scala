package homelab.common.flow


import zio.*


/**
 * Sections of work that a shutdown waits for. The logic that knows a section must not be cut off runs it
 * through [[graceful]], and [[await]] waits for every running section before the application closes its
 * scopes.
 *
 * Once [[await]] has begun, a new section is refused by parking it: it never starts, never fails, and stays
 * interruptible, so the scope closing afterwards interrupts it like any other waiting fiber. Parking rather
 * than failing keeps a refusal from tearing down a fail-fast caller.
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
 * Two more operations refuse work '''inside''' a section that was admitted before shutdown began, which is
 * what a drain over a fetching intake needs: [[refusing]] interrupts a step with this instance's own refusal
 * once shutdown has begun, and [[parkRefusals]] turns that refusal back into parking, outside the section.
 * The consumer drains, `Consumer.drainLocal` and `Consumer.globalDrain`, are built from these.
 *
 * @param state whether shutdown has begun, and how many sections are running
 * @param drained completed when the last running section finishes after shutdown has begun
 * @param refusal who a [[refusing]] step is interrupted by, so [[parkRefusals]] catches this instance's
 *                refusals and no other interruption
 */
final class Shutdown private (state: Ref[Shutdown.State], drained: Promise[Nothing, Unit], refusal: FiberId):

  /**
   * Run `section` as work a shutdown waits for, or park it forever if shutdown has already begun.
   *
   * The count is taken and released inside one uninterruptible region, with only `section` and the parking
   * restored, so an interruption can never leave a section counted in that has stopped running.
   *
   * @param section the work that should not be cut off
   * @tparam R the environment `section` needs
   * @tparam E the error `section` aborts with
   * @tparam A the value `section` produces
   * @return the value of `section`; aborts with `E` when `section` does; never completes, until interrupted,
   *         when shutdown has begun
   */
  def graceful[R, E, A](section: ZIO[R, E, A]): ZIO[R, E, A] =
    ZIO.uninterruptibleMask: restore =>
      enter.flatMap: admitted =>
        if admitted then restore(section).ensuring(leave)
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
   * Run `step`, or refuse it once shutdown has begun: the refusal interrupts the fiber with this instance's
   * own id, before `step` starts.
   *
   * It is an interruption rather than a failure so that whatever runs the step settles it as it settles any
   * interrupted work (a consumer's adapter hands the value back at once), and so that no error type has to
   * make room for it. Being a self-interruption, it can be caught: [[parkRefusals]] does, outside the
   * section, so it never reaches a fail-fast caller.
   *
   * @param step the work to run unless shutdown has begun
   * @tparam R the environment `step` needs
   * @tparam E the error `step` aborts with
   * @tparam A the value `step` produces
   * @return the value of `step`; aborts with `E` when `step` does; is interrupted by this instance's refusal,
   *         without running `step`, once shutdown has begun
   */
  def refusing[R, E, A](step: ZIO[R, E, A]): ZIO[R, E, A] =
    closing.flatMap:
      case true  => ZIO.interruptAs(refusal)
      case false => step

  /**
   * Run `effect`, and park if it ends with this instance's refusal alone, as a [[refusing]] step inside it
   * does once shutdown has begun. Put it around the section, so the parking is outside it and [[await]] is
   * not held up.
   *
   * Only the refusal alone is caught: a cause that also holds a failure or a defect (a cleanup that died on
   * the way out, say) is not a refusal any more, and reaches the caller. So does any other interruption,
   * including one `effect` raises itself.
   *
   * @param effect the work that may end refused
   * @tparam R the environment `effect` needs
   * @tparam E the error `effect` aborts with
   * @tparam A the value `effect` produces
   * @return the value of `effect`; aborts with whatever it aborts with; never completes, until interrupted,
   *         when it ended with this instance's refusal
   */
  def parkRefusals[R, E, A](effect: ZIO[R, E, A]): ZIO[R, E, A] =
    effect.catchSomeCause:
      case cause if cause.isInterruptedOnly && cause.interruptors == Set(refusal) => ZIO.never

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
