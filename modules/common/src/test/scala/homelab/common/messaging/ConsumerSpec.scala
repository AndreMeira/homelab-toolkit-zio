package homelab.common.messaging


import zio.*
import zio.test.*


object ConsumerSpec extends ZIOSpecDefault:
  import Support.*

  def spec: Spec[TestEnvironment & Scope, Any] = suite("Consumer")(serialSuite, onceSuite) @@ TestAspect.withLiveClock @@
    TestAspect.timeout(60.seconds)

  private def serialSuite = suite("serial")(
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
  )

  private def onceSuite = suite("once")(
    test("retries a consume that returned without running logic, until one value is processed") {
      for
        calls    <- Ref.make(0)
        received <- Ref.make(List.empty[Int])
        _        <- emptyThen(calls, 3, 7).once(value => received.update(_ :+ value))
        made     <- calls.get
        values   <- received.get
      yield assertTrue(made == 4, values == List(7))
    },
    test("stops after the first processed value") {
      for
        calls <- Ref.make(0)
        _     <- emptyThen(calls, 0, 7).once(_ => ZIO.unit)
        made  <- calls.get
      yield assertTrue(made == 1)
    },
    test("a failing logic aborts once, without retrying") {
      for
        calls  <- Ref.make(0)
        failed <- emptyThen(calls, 0, 7).once(_ => ZIO.fail("refused")).either
        made   <- calls.get
      yield assertTrue(failed == Left("refused"), made == 1)
    },
  )

  private object Support:

    /**
     * A consumer whose first `empty` calls return without running `logic`, as an empty fetch does, and which
     * then hands `value` over on every call, counting every call.
     *
     * @param calls counts every `consume` call
     * @param empty how many calls return empty first
     * @param value what every later call hands over
     * @return the consumer
     */
    def emptyThen(calls: Ref[Int], empty: Int, value: Int): Consumer[Nothing, Int] = new Consumer[Nothing, Int]:
      def consume[E2 >: Nothing](logic: Int => IO[E2, Unit]): IO[E2, Unit] =
        calls.getAndUpdate(_ + 1).flatMap(call => if call < empty then ZIO.unit else logic(value))
