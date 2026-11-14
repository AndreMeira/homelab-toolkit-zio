package homelab.common.flow


import zio.*
import zio.test.*

import java.time.Instant


/**
 * The time latch against the `TestClock`: when it fires, what a trigger can and cannot change, and that no input
 * makes it throw.
 *
 * A test that would hang if the latch never fired joins its waiter directly and relies on the suite's live
 * timeout to fail; a test that checks the latch has *not* fired yet waits until the waiter sleeps, moves the
 * clock, and polls it.
 */
object TimeLatchSpec extends ZIOSpecDefault:
  import Support.*

  def spec = suite("TimeLatch")(
    test("fires at its armed time, and not before") {
      for
        latch  <- TimeLatch.make(10.seconds, Duration.Zero)
        waiter <- latch.await.fork
        _      <- asleep(1)
        _      <- TestClock.adjust(9.seconds)
        early  <- waiter.poll
        _      <- TestClock.adjust(1.second)
        _      <- waiter.join
      yield assertTrue(early.isEmpty)
    },
    test("an earlier trigger wakes every waiter, at the new time") {
      for
        latch  <- TimeLatch.make(10.seconds, Duration.Zero)
        first  <- latch.await.fork
        second <- latch.await.fork
        _      <- asleep(2)
        _      <- latch.triggerIn(2.seconds)
        _      <- TestClock.adjust(2.seconds)
        _      <- first.join
        _      <- second.join
      yield assertCompletes
    },
    test("a later trigger, or one earlier by less than the slack, changes nothing") {
      for
        latch  <- TimeLatch.make(10.seconds, 1.second)
        waiter <- latch.await.fork
        _      <- asleep(1)
        _      <- latch.triggerIn(20.seconds)
        _      <- latch.triggerIn(9500.millis)
        _      <- TestClock.adjust(9600.millis)
        early  <- waiter.poll
        _      <- TestClock.adjust(400.millis)
        _      <- waiter.join
      yield assertTrue(early.isEmpty)
    },
    test("a negative slack counts as zero, so a later trigger still cannot delay it") {
      // Unclamped, a later trigger's gap (negative) would compare as at least the slack (more negative), and
      // move the firing time later: the join below would hang until the suite's timeout.
      for
        latch  <- TimeLatch.make(10.seconds, -10.seconds)
        waiter <- latch.await.fork
        _      <- asleep(1)
        _      <- latch.triggerIn(15.seconds)
        _      <- TestClock.adjust(10.seconds)
        _      <- waiter.join
      yield assertCompletes
    },
    test("once fired, await returns at once, and a trigger cannot make it wait again") {
      for
        latch <- TimeLatch.make(1.second, Duration.Zero)
        _     <- TestClock.adjust(1.second)
        _     <- latch.await
        _     <- latch.triggerIn(5.seconds)
        _     <- latch.await
      yield assertCompletes
    },
    test("a trigger landing while a waiter starts to wait is not missed") {
      // No wait for the waiter to sleep first: the trigger may land before it reads the firing time, between
      // the read and the sleep, or during the sleep. A missed one would leave it asleep until the 10 s
      // backstop, and the join would hang.
      for
        latch  <- TimeLatch.make(10.seconds, Duration.Zero)
        waiter <- latch.await.fork
        _      <- latch.triggerIn(1.second)
        _      <- TestClock.adjust(1.second)
        _      <- waiter.join
      yield assertCompletes
    } @@ TestAspect.nonFlaky(200),
    test("a trigger with a negative delay fires it now") {
      for
        latch  <- TimeLatch.make(10.seconds, Duration.Zero)
        waiter <- latch.await.fork
        _      <- asleep(1)
        _      <- latch.triggerIn(-5.seconds)
        _      <- waiter.join
      yield assertCompletes
    },
    test("a delay beyond what an Instant can hold is capped, so it does not throw") {
      for
        latch  <- TimeLatch.make(Duration.fromSeconds(twoBillionYears), Duration.Zero)
        waiter <- latch.await.fork
        _      <- asleep(1)
        _      <- TestClock.adjust(1.day)
        early  <- waiter.poll
        _      <- latch.triggerIn(Duration.Zero)
        _      <- waiter.join
      yield assertTrue(early.isEmpty)
    },
    test("on the live clock, a delay far beyond Duration.Infinity is capped, so it does not fire at once") {
      // The live timer returns at once when asked to wait a billion years, which the TestClock does not
      // reproduce: it schedules by instant. Uncapped, the waiter would be done before the short sleep ends.
      for
        latch  <- TimeLatch.make(Duration.fromSeconds(billionYears), Duration.Zero)
        waiter <- latch.await.fork
        _      <- ZIO.sleep(200.millis)
        early  <- waiter.poll
        _      <- waiter.interrupt
      yield assertTrue(early.isEmpty)
    } @@ TestAspect.withLiveClock,
    test("triggers at the extremes of Instant neither throw nor confuse the latch") {
      for
        latch  <- TimeLatch.make(10.seconds, Duration.Infinity)
        _      <- latch.triggerAt(Instant.MAX)
        _      <- latch.triggerIn(Duration.fromSeconds(twoBillionYears))
        waiter <- latch.await.fork
        _      <- asleep(1)
        _      <- TestClock.adjust(9.seconds)
        early  <- waiter.poll
        _      <- latch.triggerAt(Instant.MIN)
        _      <- waiter.join
        _      <- latch.triggerAt(Instant.MIN)
        _      <- latch.await
      yield assertTrue(early.isEmpty)
    },
  ) @@ TestAspect.timeout(30.seconds)

  private object Support {

    /** A billion years in seconds: far beyond `Duration.Infinity`, and still a `Long`. */
    val billionYears: Long = 1_000_000_000L * 365 * 24 * 3600

    /** Two billion years in seconds: added to 1970, where the `TestClock` starts, beyond `Instant.MAX`. */
    val twoBillionYears: Long = 2 * billionYears

    /**
     * Wait until at least `count` fibers are asleep on the `TestClock`, so that moving the clock wakes them
     * rather than racing their sleep.
     *
     * @param count how many sleeps to wait for
     * @return noop once that many are registered
     */
    def asleep(count: Int): UIO[Unit] =
      for
        sleeps <- TestClock.sleeps
        _      <- (ZIO.yieldNow *> asleep(count)).when(sleeps.size < count)
      yield ()
  }
