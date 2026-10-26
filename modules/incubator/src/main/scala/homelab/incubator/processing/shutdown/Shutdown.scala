package homelab.incubator.processing.shutdown


import homelab.common.messaging.Consumer
import zio.*


/**
 * Sections of work that a shutdown waits for. The logic that knows a section must not be cut off runs it
 * through [[graceful]], and [[await]] waits for every running section before the application closes its
 * scopes.
 *
 * Once [[await]] has begun, a new section is refused by parking it: it never starts, never fails, and stays
 * interruptible, so the scope closing afterwards interrupts it like any other waiting fiber. A refused
 * message is therefore settled by its adapter exactly as an interrupted one is, and nothing about it was done.
 *
 * A section must not run another through [[graceful]] inside itself: once shutdown has begun the inner one
 * parks, and the outer one, which [[await]] is waiting for, never finishes.
 *
 * @param state whether shutdown has begun, and how many sections are running
 * @param drained completed when the last running section finishes after shutdown has begun
 */
final class Shutdown private (state: Ref[Shutdown.State], drained: Promise[Nothing, Unit]):

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
   * A consumer that stops taking work once shutdown has begun. A `consume` call started after [[await]] parks
   * without reaching `consumer`, so nothing more is pulled or claimed, and stays interruptible, so the closing
   * scope ends it holding nothing. A call that had already reached `consumer` carries on as usual.
   *
   * @param consumer the consumer to stop taking work from
   * @tparam E the error `consumer` aborts with
   * @tparam A the value `consumer` delivers
   * @return the gated consumer
   */
  def gate[E, A](consumer: Consumer[E, A]): Consumer[E, A] =
    new Consumer[E, A]:
      override def consume[E2 >: E](logic: A => IO[E2, Unit]): IO[E2, Unit] =
        closing.flatMap(stopped => if stopped then ZIO.never else consumer.consume(logic))

  /**
   * Whether shutdown has begun.
   *
   * @return true once [[await]] has been called
   */
  def closing: UIO[Boolean] = state.get.map(_.closing)

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
   * @return the instance; never fails
   */
  def make: UIO[Shutdown] =
    for
      state   <- Ref.make(State(closing = false, running = 0))
      drained <- Promise.make[Nothing, Unit]
    yield new Shutdown(state, drained)

  /**
   * Where a [[Shutdown]] stands.
   *
   * @param closing whether [[Shutdown.await]] has been called
   * @param running how many sections are running
   */
  final private[Shutdown] case class State(closing: Boolean, running: Int)
