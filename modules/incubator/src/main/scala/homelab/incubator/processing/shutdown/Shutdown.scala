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
 * message is therefore settled by its adapter exactly as an interrupted one is, and nothing about it was done:
 * an adapter that can hand a message back does so at once (see `Consumer` on interruption), and one with no
 * acknowledgement (Core NATS) loses it, as it would lose any interrupted message. Parking rather than failing
 * also keeps a refusal from tearing down a fail-fast processor.
 *
 * A section must not run another through [[graceful]] inside itself: once shutdown has begun the inner one
 * parks, and the outer one, which [[await]] is waiting for, never finishes. That includes the `logic` given
 * to a [[drainLocal]] or [[globalDrain]] consumer, which already runs inside a section.
 *
 * @param state whether shutdown has begun, and how many sections are running
 * @param drained completed when the last running section finishes after shutdown has begun
 * @param refusal who a [[globalDrain]] refusal is interrupted by, so the wrapper catches its own refusals and
 *                no other interruption
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
   * A consumer whose in-process work a shutdown waits for, and which stops taking work once shutdown has
   * begun. Each delivered value runs through [[graceful]], so:
   *
   *   - a value whose `logic` is already running when [[await]] is called is waited for, and completes;
   *   - a `consume` call started after [[await]] parks without reaching `consumer`, so nothing more is pulled
   *     or claimed, and the closing scope interrupts it holding nothing;
   *   - a `consume` call that had already reached `consumer`, and receives a value after [[await]], is
   *     refused: `logic` never runs, the call parks, and the closing scope interrupts it, so the adapter
   *     hands the value back as it does any interrupted one (or loses it, where it cannot hand back).
   *
   * `logic` must not itself run a section through [[graceful]]: it is one already, and the inner one would
   * park once shutdown began, holding up [[await]] forever.
   *
   * @param consumer the consumer to drain
   * @tparam E the error `consumer` aborts with
   * @tparam A the value `consumer` delivers
   * @return the draining consumer
   */
  def drainLocal[E, A](consumer: Consumer[E, A]): Consumer[E, A] = new Consumer[E, A]:
    override def consume[E2 >: E](logic: A => IO[E2, Unit]): IO[E2, Unit] =
      closing.flatMap: stopped =>
        if stopped then ZIO.never
        else consumer.consume(value => graceful(logic(value)))

  /**
   * A consumer whose whole `consume` call a shutdown waits for, the fetch included, and which stops taking
   * work once shutdown has begun. Where [[drainLocal]] waits only for `logic`, this also waits for a fetch
   * that is still open, so a message the server sends while shutdown begins is not lost on its way but
   * delivered, refused and handed back:
   *
   *   - a `consume` call started before [[await]] is waited for, and if its value arrives before [[await]],
   *     `logic` runs and completes;
   *   - a value that arrives after [[await]] is refused by interrupting `logic` before it starts, so the
   *     adapter hands it back at once, as it does any interrupted value (or loses it, where it cannot hand
   *     back). The interruption is then caught here and the call parks outside the section, so [[await]] is
   *     not held up and a fail-fast processor does not see it as a failure;
   *   - a `consume` call started after [[await]] parks without reaching `consumer`.
   *
   * [[await]] waits for every open fetch, so `consumer` must make '''one bounded fetch per call''' and return
   * without calling `logic` when it comes back empty. A consumer that blocks until a value arrives (an
   * in-memory queue, a fetch loop) would hold up [[await]] for as long as its intake stays idle.
   *
   * `logic` must not itself run a section through [[graceful]]: it runs inside one, and the inner one would
   * park once shutdown began, holding up [[await]] forever.
   *
   * @param consumer the consumer to drain; one bounded fetch per `consume`
   * @tparam E the error `consumer` aborts with
   * @tparam A the value `consumer` delivers
   * @return the draining consumer
   */
  def globalDrain[E, A](consumer: Consumer[E, A]): Consumer[E, A] = new Consumer[E, A]:
    override def consume[E2 >: E](logic: A => IO[E2, Unit]): IO[E2, Unit] =
      graceful:
        consumer.consume: value =>
          closing.flatMap:
            case true  => ZIO.interruptAs(refusal)
            case false => logic(value)
      .catchSomeCause:
        case cause if cause.interruptors.contains(refusal) => ZIO.never

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
   * Its refusals are interrupted by the id of a fiber forked and interrupted here, so they carry an id the
   * runtime minted for this instance rather than one built by hand. A fiber id compares by its number, start
   * time and fork site together, so no other fiber's interruption is mistaken for a refusal.
   *
   * @return the instance; never fails
   */
  def make: UIO[Shutdown] =
    for
      state   <- Ref.make(State(closing = false, running = 0))
      drained <- Promise.make[Nothing, Unit]
      refusal <- ZIO.never.fork.flatMap(fiber => fiber.interrupt.as(fiber.id))
    yield new Shutdown(state, drained, refusal)

  /**
   * Where a [[Shutdown]] stands.
   *
   * @param closing whether [[Shutdown.await]] has been called
   * @param running how many sections are running
   */
  final private[Shutdown] case class State(closing: Boolean, running: Int)
