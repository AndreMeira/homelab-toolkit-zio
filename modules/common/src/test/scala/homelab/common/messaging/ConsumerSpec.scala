package homelab.common.messaging


import zio.*
import zio.test.*


object ConsumerSpec extends ZIOSpecDefault:

  def spec: Spec[TestEnvironment & Scope, Any] = suite("Consumer.serial")(
    test("serialises concurrent consume calls — logic never overlaps") {
      for
        inFlight <- Ref.make(0)
        peak     <- Ref.make(0)
        base      = new Consumer[Nothing, Int]:
                      def consume[E2 >: Nothing](logic: Int => IO[E2, Unit]): IO[E2, Unit] = logic(1)
        serial   <- base.serial
        logic     = (_: Int) =>
                      inFlight.updateAndGet(_ + 1).flatMap(n => peak.update(_ max n)) *>
                        ZIO.sleep(20.millis) *>
                        inFlight.update(_ - 1)
        _        <- ZIO.foreachParDiscard(1 to 20)(_ => serial.consume(logic)) // without `serial`, peak ≈ 20
        seen     <- peak.get
      yield assertTrue(seen == 1)
    },
    test("propagates the delivered value and logic failures") {
      val boom = new RuntimeException("boom")
      for
        got    <- Ref.make(0)
        base    = new Consumer[Nothing, Int]:
                    def consume[E2 >: Nothing](logic: Int => IO[E2, Unit]): IO[E2, Unit] = logic(42)
        serial <- base.serial
        _      <- serial.consume(a => got.set(a))
        value  <- got.get
        failed <- serial.consume(_ => ZIO.fail(boom)).either
      yield assertTrue(value == 42, failed == Left(boom))
    },
  ) @@ TestAspect.withLiveClock @@ TestAspect.timeout(60.seconds)
