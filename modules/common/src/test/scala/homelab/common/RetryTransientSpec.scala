package homelab.common


import homelab.common.error.ApplicationError
import homelab.common.error.ApplicationError.TransientError
import zio.*
import zio.test.*


object RetryTransientSpec extends ZIOSpecDefault:

  private case object Blip extends TransientError:
    override def message: String = "transient blip"

  private case object Fatal extends ApplicationError:
    override def message: String = "not transient"

  def spec: Spec[TestEnvironment & Scope, Any] = suite("retryTransient")(
    test("retries a transient failure and eventually succeeds") {
      for
        attempts <- Ref.make(0)
        effect    = attempts.updateAndGet(_ + 1).flatMap(n => if n <= 2 then ZIO.fail(Blip) else ZIO.succeed(n))
        result   <- retryTransient(Schedule.recurs(5))(effect)
        count    <- attempts.get
      yield assertTrue(result == 3, count == 3) // failed on attempts 1 & 2, succeeded on 3
    },
    test("fails fast on a non-transient error, without retrying") {
      for
        attempts <- Ref.make(0)
        effect    = attempts.update(_ + 1) *> ZIO.fail(Fatal)
        exit     <- retryTransient(Schedule.recurs(5))(effect).either
        count    <- attempts.get
      yield assertTrue(exit == Left(Fatal), count == 1)
    },
    test("gives up with the transient error once the schedule is exhausted") {
      for
        attempts <- Ref.make(0)
        effect    = attempts.update(_ + 1) *> ZIO.fail(Blip)
        exit     <- retryTransient(Schedule.recurs(3))(effect).either
        count    <- attempts.get
      yield assertTrue(exit == Left(Blip), count == 4) // initial attempt + 3 retries
    },
    test("spaces retries by the schedule's delay (driven by the test clock)") {
      // 3 retries, each 1s apart. Fork it, then advance virtual time — no real waiting.
      val schedule = Schedule.recurs(3) && Schedule.spaced(1.second)
      for
        attempts <- Ref.make(0)
        effect    = attempts.update(_ + 1) *> ZIO.fail(Blip)
        fiber    <- retryTransient(schedule)(effect).either.fork
        _        <- TestClock.adjust(1.second)  // wakes retry #1
        afterOne <- attempts.get
        _        <- TestClock.adjust(2.seconds) // wakes retries #2 and #3, exhausting the schedule
        exit     <- fiber.join
        total    <- attempts.get
      yield assertTrue(afterOne == 2, exit == Left(Blip), total == 4)
    },
  )
