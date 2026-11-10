package homelab.incubator.time.v2


import zio.*

import java.time.Instant


/**
 * A tick at the earliest instant anyone asked for, with a periodic backstop.
 *
 * Whoever waits on [[awaitTick]] is woken at the earliest instant requested through [[at]], or after
 * `period` when nothing earlier was asked, so a request lost with an instance (a crash, a restart) is
 * answered late rather than never. A request is a hint about when to look: whatever is due should be
 * found in its own store by the work that follows a tick, which then asks for the next one.
 *
 * Only an earlier request wakes the waiter, and only so it sleeps again until the new instant. A burst of
 * requests gives one tick, never one each: a tick comes at least `slack` after the waiter starts waiting.
 *
 * @param period the longest a waiter sleeps without a request: the backstop
 * @param slack the shortest a waiter sleeps, which coalesces a burst of requests into one tick
 * @param due the earliest instant requested and not yet answered by a tick; `Instant.MAX` when none
 * @param nudge what wakes a sleeping waiter when a request is earlier than what it waits for; holds one,
 *              so repeats collapse and none is lost
 */
final class Ticker private (period: Duration, slack: Duration, due: Ref[Instant], nudge: Queue[Unit]):

  /**
   * Ask for a tick no later than `instant`.
   *
   * The earliest request wins. A request later than one already waiting changes nothing and wakes nobody.
   *
   * @param instant when the caller wants the next tick at the latest
   * @return noop
   */
  def at(instant: Instant): UIO[Unit] =
    due
      .modify(current => if instant.isBefore(current) then (true, instant) else (false, current))
      .flatMap(earlier => nudge.offer(()).when(earlier).unit)

  /**
   * Wait for the next tick: the earliest requested instant, or the backstop, whichever comes first.
   *
   * The backstop is fixed when the wait starts, so requests arriving more often than `period` cannot
   * postpone it.
   *
   * @return noop, once the tick has come
   */
  def awaitTick: UIO[Unit] =
    Clock.instant.flatMap(now => awaitUntil(now.plus(period)))

  /**
   * Sleep until the earliest requested instant or `backstop`, sleeping again whenever an earlier request
   * arrives.
   *
   * A tick clears only a request it answers, one due no later than the instant it slept until. A request
   * made while it was waking for a later one is kept for the next tick.
   *
   * @param backstop the latest this wait may end, fixed when it started
   * @return noop, once the tick has come
   */
  private def awaitUntil(backstop: Instant): UIO[Unit] =
    for
      now   <- Clock.instant
      until <- due.get.map(requested => if requested.isBefore(backstop) then requested else backstop)
      woken <- nudge.take.timeout(Duration.fromInterval(now, until) max slack)
      _     <- woken match
                 case Some(_) => awaitUntil(backstop)
                 case None    => due.update(requested => if requested.isAfter(until) then requested else Instant.MAX)
    yield ()


object Ticker:

  /**
   * A ticker with no request yet: its first tick is the backstop.
   *
   * @param period the longest a waiter sleeps without a request
   * @param slack the shortest a waiter sleeps, which coalesces a burst of requests into one tick
   * @return the ticker
   */
  def make(period: Duration, slack: Duration): UIO[Ticker] =
    for
      due   <- Ref.make(Instant.MAX)
      nudge <- Queue.sliding[Unit](1)
    yield Ticker(period, slack, due, nudge)
