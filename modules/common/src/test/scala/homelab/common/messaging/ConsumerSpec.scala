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
    test("a value whose failure the adapter settles itself ends once, without another fetch") {
      // Fails if once retries after a delivered value whose failure did not escape consume: every later call
      // delivers and fails again, so once never returns, and the timeout leaves it unfinished.
      for
        calls    <- Ref.make(0)
        finished <- settlingFailures(calls, 7).once(_ => ZIO.fail("refused")).timeout(1.second)
        made     <- calls.get
      yield assertTrue(finished.isDefined, made == 1)
    },
    test("a logic that completes while an interrupt is pending is still settled as done") {
      // Fails with "handed back" if once adds an interruptible step after logic: the pending interrupt
      // lands there, and the adapter sees an interrupted handler.
      for
        settled <- Promise.make[Nothing, String]
        started <- Promise.make[Nothing, Unit]
        gate    <- Promise.make[Nothing, Unit]
        handler  = (_: Int) => ZIO.uninterruptible(started.succeed(()) *> gate.await)
        fiber   <- settlingByExit(settled, 7).once(handler).fork
        _       <- started.await
        _       <- fiber.interruptFork
        _       <- gate.succeed(())
        _       <- fiber.await
        verdict <- settled.await
      yield assertTrue(verdict == "done")
    },
  )

  private object Support:

    /**
     * A consumer that settles a failed `logic` itself, as a redelivery or discard policy does, so the call
     * returns successfully whatever `logic` did. Counts every call.
     *
     * @param calls counts every `consume` call
     * @param value what every call hands over
     * @return the consumer
     */
    def settlingFailures(calls: Ref[Int], value: Int): Consumer[Nothing, Int] = new Consumer[Nothing, Int]:
      def consume[E2 >: Nothing](logic: Int => IO[E2, Unit]): IO[E2, Unit] =
        calls.update(_ + 1) *> logic(value).ignore

    /**
     * A consumer that settles by how `logic` ended, as the JetStream consumers do: `logic` runs
     * interruptibly, the rest does not, and the verdict is recorded in `settled`.
     *
     * @param settled completed with "done" or "handed back"
     * @param value what every call hands over
     * @return the consumer
     */
    def settlingByExit(settled: Promise[Nothing, String], value: Int): Consumer[Nothing, Int] = new Consumer[Nothing, Int]:
      def consume[E2 >: Nothing](logic: Int => IO[E2, Unit]): IO[E2, Unit] =
        ZIO.uninterruptibleMask: restore =>
          restore(logic(value)).exit.flatMap: exit =>
            settled.succeed(if exit.isInterrupted then "handed back" else "done") *> ZIO.done(exit)

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
