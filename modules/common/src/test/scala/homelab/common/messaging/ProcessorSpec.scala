package homelab.common.messaging


import zio.*
import zio.test.*


// Property tests for Processor.Parallel as an execution POLICY, deliberately implementation-agnostic:
// a static pool parks `parallelism` consumers on an idle input, a demand-driven spawner parks one —
// both are valid. What must hold regardless: outstanding consume calls stay within the cap (no runaway
// spawning), at least one consumer is always listening, every value is processed, and concurrent
// `process` runs never exceed `parallelism`. Per-key serialisation is pinned separately, in the
// inmemory suite, where a keyed input exists.
object ProcessorSpec extends ZIOSpecDefault:

  def spec = suite("Processor.Parallel")(
    test("outstanding consume calls stay within [1, parallelism] on an idle input") {
      for
        entered <- Ref.make(0)
        source   = new Consumer[Nothing, Int]:
                     def consume[E2 >: Nothing](logic: Int => IO[E2, Unit]): IO[E2, Unit] =
                       entered.update(_ + 1) *> ZIO.never // an idle input: every consume call parks
        proc     = new Processor.Parallel[Nothing, Int]:
                     def parallelism = 4
                     def input: Consumer[Nothing, Int]          = source
                     def process(value: Int): IO[Nothing, Unit] = ZIO.unit
        fiber   <- proc.run.fork
        _       <- ZIO.sleep(200.millis) // ample time for a runaway spawner to blow past the cap
        count   <- entered.get
        _       <- fiber.interrupt
      yield assertTrue(1 <= count, count <= 4) // ≥ 1: someone is listening; ≤ 4: spawning is bounded
    },
    test("drains every value, with concurrent process runs capped by parallelism") {
      val n = 60
      for
        queue    <- Queue.unbounded[Int]
        _        <- queue.offerAll(1 to n)
        inCalls  <- Ref.make(0)
        callPeak <- Ref.make(0)
        source    = new Consumer[Nothing, Int]:
                      def consume[E2 >: Nothing](logic: Int => IO[E2, Unit]): IO[E2, Unit] =
                        (inCalls.updateAndGet(_ + 1).flatMap(c => callPeak.update(_ max c)) *>
                          queue.take.flatMap(logic)).ensuring(inCalls.update(_ - 1))
        inFlight <- Ref.make(0)
        peak     <- Ref.make(0)
        seen     <- Ref.make(Set.empty[Int])
        allIn    <- Promise.make[Nothing, Unit]
        proc      = new Processor.Parallel[Nothing, Int]:
                      def parallelism = 4
                      def input: Consumer[Nothing, Int] = source
                      def process(value: Int): IO[Nothing, Unit] =
                        inFlight.updateAndGet(_ + 1).flatMap(c => peak.update(_ max c)) *>
                          ZIO.sleep(10.millis) *>
                          inFlight.update(_ - 1) *>
                          seen.updateAndGet(_ + value).flatMap(s => allIn.succeed(()).when(s.size == n)).unit
        fiber    <- proc.run.fork
        _        <- allIn.await // completes only if every value was processed — no lost values
        _        <- fiber.interrupt
        p        <- peak.get
        cp       <- callPeak.get
        s        <- seen.get
      yield assertTrue(
        s == (1 to n).toSet, // nothing lost, nothing invented
        p <= 4,              // the cap held under a 60-value backlog
        p >= 2,              // and the policy actually parallelised (a serial loop would sit at 1)
        cp <= 5,             // outstanding consume calls stay ≤ parallelism + one waiting listener
      )
    },
  ) @@ TestAspect.withLiveClock @@ TestAspect.timeout(60.seconds)
