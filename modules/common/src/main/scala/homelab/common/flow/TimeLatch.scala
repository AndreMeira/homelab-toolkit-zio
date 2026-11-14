package homelab.common.flow


import homelab.common.flow.TimeLatch.State
import zio.*

import java.time.Instant


/**
 * A one-shot timer that fires at an instant which triggers can only bring earlier.
 *
 * Created armed, with the latest instant it fires at. Once fired, it stays fired: `await` returns at once and
 * no trigger can make it wait again. It is thrown away rather than reset, so whoever needs another firing makes
 * another latch.
 *
 * "Fired" is a time, not a flag: the latch has fired once its firing instant is no longer in the future. A
 * trigger on a fired latch can only name an instant that is not earlier, so it changes nothing; one that names
 * an even earlier instant moves the firing instant further into the past, which leaves the latch fired.
 *
 * The firing instant and the promise that wakes its waiters are one value, read and replaced together, so a
 * waiter always sees an instant with the promise a later trigger will complete, and a trigger moves both in one
 * step. That is what keeps a trigger landing while a waiter is between reading the instant and sleeping from
 * being missed.
 *
 * Every input is accepted and nothing here throws. A delay is capped at `Duration.Infinity` (about 292 years):
 * the latch fires *at the latest* at the time asked, so firing at the cap instead of later still keeps that
 * promise, and it keeps every instant it computes far from `Instant.MAX`.
 *
 * @param state when it fires, and what wakes its waiters when a trigger brings that earlier
 * @param slack how much earlier a trigger must be to move it, which coalesces a burst of triggers
 */
final class TimeLatch private (state: Ref[State], slack: Duration) {

  /**
   * Fire no later than `duration` from now.
   *
   * @param duration how long from now, at the latest; a negative one means now, and one beyond
   *                 `Duration.Infinity` is capped there
   * @return noop
   */
  def triggerIn(duration: Duration): UIO[Unit] =
    for
      now <- Clock.instant
      _   <- triggerAt(now.plus(TimeLatch.bounded(duration)))
    yield ()

  /**
   * Fire no later than `instant`.
   *
   * Moves the firing instant when `instant` is earlier than it by at least `slack`, and wakes every waiter so it
   * sleeps until the new one. Otherwise, or once the latch has fired, changes nothing.
   *
   * The gap is measured with `Duration.fromInterval` and compared with `slack`, rather than by adding `slack` to
   * one instant: neither instant is shifted, so any `instant`, `Instant.MAX` and `Instant.MIN` included, is
   * compared without leaving the range of an `Instant`. (`fromInterval` falls back to seconds when the gap
   * overflows nanoseconds, so it does not throw either, across the whole range.)
   *
   * @param instant when to fire, at the latest
   * @return noop
   */
  def triggerAt(instant: Instant): UIO[Unit] =
    for
      fresh <- Promise.make[Nothing, Unit]
      moved <- state.modify: current =>
                 if Duration.fromInterval(instant, current.when).compareTo(slack) < 0
                 then None                 -> current
                 else Some(current.signal) -> State(instant, fresh)
      _     <- wake(moved)
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

  /**
   * Wake the waiters of the state a trigger replaced, if it replaced one.
   *
   * @param replaced the promise of the replaced state, or `None` when the trigger changed nothing
   * @return noop
   */
  private def wake(replaced: Option[Promise[Nothing, Unit]]): UIO[Unit] =
    replaced match
      case Some(signal) => signal.succeed(()).unit
      case None         => ZIO.unit
}


object TimeLatch {

  /**
   * A latch armed to fire `in` from now at the latest; triggers can only bring it earlier.
   *
   * @param in the latest it fires, from now: the backstop. A negative one is a latch born fired; one beyond
   *           `Duration.Infinity` is capped there
   * @param slack how much earlier a trigger must be to move it, which coalesces a burst of triggers; a negative
   *              one counts as zero, and one beyond `Duration.Infinity` is capped there
   * @return the latch
   */
  def make(in: Duration, slack: Duration): UIO[TimeLatch] =
    for
      now    <- Clock.instant
      signal <- Promise.make[Nothing, Unit]
      state  <- Ref.make(State(now.plus(bounded(in)), signal))
    yield TimeLatch(state, bounded(slack))

  /**
   * A duration brought within what the latch computes with: no less than zero, no more than
   * `Duration.Infinity`.
   *
   * The cap is what keeps `now.plus(delay)` within the range of an `Instant`, beyond which it would throw; and
   * ZIO's own timer returns at once when asked to wait far beyond it, which would fire the latch early instead
   * of never. Checked on ZIO 2.1.23: a wait of `Duration.Infinity` sleeps, one of a billion years returns at
   * once.
   *
   * For the slack, the floor is what matters: a trigger moves the latch when its gap to the firing instant is
   * at least the slack, and a later trigger's gap is negative, so a negative slack would let it delay the
   * latch.
   *
   * @param duration the duration asked for
   * @return it, clamped to `[0, Duration.Infinity]`
   */
  private def bounded(duration: Duration): Duration =
    duration.max(Duration.Zero).min(Duration.Infinity)

  /**
   * When the latch fires, and what wakes its waiters when a trigger brings that earlier.
   *
   * @param when the firing instant
   * @param signal completed when a trigger replaces this state, never otherwise
   */
  final private[flow] case class State(when: Instant, signal: Promise[Nothing, Unit])
}
