package homelab.incubator.time.v3


import homelab.incubator.time.v3.TimeLatch.State
import zio.*

import java.time.Instant


/**
 * A one-shot timer that fires at an instant which triggers can only bring earlier.
 *
 * Created armed, with the latest instant it fires at. Once fired, it stays fired: `await` returns at once and
 * every trigger is refused. It is thrown away rather than reset, so whoever needs another firing makes another
 * latch.
 *
 * The firing instant and the promise that wakes its waiters are one value, read and replaced together, so a
 * waiter always sees an instant with the promise a later trigger will complete, and a trigger moves both in one
 * step.
 *
 * @param state when it fires, and what wakes its waiters when a trigger brings that earlier
 * @param slack how much earlier a trigger must be to move it, which coalesces a burst of triggers
 */
final class TimeLatch private (state: Ref[State], slack: Duration) {

  /**
   * Fire no later than `duration` from now.
   *
   * @param duration how long from now, at the latest; a negative one means now
   * @return noop
   */
  def triggerIn(duration: Duration): UIO[Unit] =
    for
      now <- Clock.instant
      when = now.plus(duration max Duration.Zero)
      _   <- triggerAt(when)
    yield ()

  /**
   * Fire no later than `instant`.
   *
   * Moves the firing instant when `instant` is earlier than it by at least `slack`, and wakes every waiter so it
   * sleeps until the new one. Otherwise, or once the latch has fired, changes nothing.
   *
   * @param instant when to fire, at the latest
   * @return noop
   */
  def triggerAt(instant: Instant): UIO[Unit] =
    for
      fresh <- Promise.make[Nothing, Unit]
      moved <- state.modify: current =>
                 if current.when.isBefore(instant.plus(slack))
                 then None                 -> current
                 else Some(current.signal) -> State(instant, fresh)
      _     <- ZIO.foreachDiscard(moved)(_.succeed(()))
    yield ()

  /**
   * Wait until the latch fires.
   *
   * Sleeps until the firing instant, and sleeps again whenever a trigger brings it earlier. Returns at once
   * when the latch has already fired.
   *
   * @return noop, once fired
   */
  def await: UIO[Unit] =
    for
      current <- state.get
      now     <- Clock.instant
      wait     = Duration.fromInterval(now, current.when)
      moved   <- current.signal.await.timeout(wait)
      _       <- await.when(moved.isDefined)
    yield ()
}


object TimeLatch {

  /**
   * A latch armed to fire `in` from now at the latest; triggers can only bring it earlier.
   *
   * @param in the latest it fires, from now: the backstop. A negative one is a latch born fired
   * @param slack how much earlier a trigger must be to move it, which coalesces a burst of triggers
   * @return the latch
   */
  def make(in: Duration, slack: Duration): UIO[TimeLatch] =
    for
      now    <- Clock.instant
      signal <- Promise.make[Nothing, Unit]
      state  <- Ref.make(State(now.plus(in max Duration.Zero), signal))
    yield TimeLatch(state, slack)

  /**
   * When the latch fires, and what wakes its waiters when a trigger brings that earlier.
   *
   * @param when the firing instant
   * @param signal completed when a trigger replaces this state, never otherwise
   */
  final private[v3] case class State(when: Instant, signal: Promise[Nothing, Unit])
}
