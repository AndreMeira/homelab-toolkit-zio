package homelab.common.messaging


import homelab.common.processing.Through
import zio.*
import zio.test.*


// Property tests for the Through.Parallel / Through.Batched.Parallel execution POLICY, deliberately
// implementation-agnostic: a demand-driven spawner parks one listener on an idle input. Whatever the
// shape, outstanding consume calls stay within the cap (no runaway spawning), at least one consumer is
// always listening, every value is processed, and concurrent `process` runs never exceed `parallelism`.
// Per-key serialisation is pinned separately, in the inmemory suite, where a keyed input exists.
object ThroughSpec extends ZIOSpecDefault:

  // A no-op output sink: these tests measure concurrency inside `process`, not what is emitted.
  private val sink: Producer[Nothing, Int] = _ => ZIO.unit

  def spec = suite("Through parallel run loops")(
    test("Through.Parallel keeps outstanding consume calls within [1, parallelism] on an idle input") {
      for
        entered <- Ref.make(0)
        source   = new Consumer[Nothing, Int]:
                     def consume[E2 >: Nothing](logic: Int => IO[E2, Unit]): IO[E2, Unit] =
                       entered.update(_ + 1) *> ZIO.never // an idle input: every consume call parks
        through  = new Through.Parallel[Nothing, Int, Int]:
                     def parallelism                           = 4
                     def input: Consumer[Nothing, Int]         = source
                     def output: Producer[Nothing, Int]        = sink
                     def process(value: Int): IO[Nothing, Int] = ZIO.succeed(value)
        fiber   <- ZIO.scoped(through.run).fork
        _       <- ZIO.sleep(200.millis) // ample time for a runaway spawner to blow past the cap
        count   <- entered.get
        _       <- fiber.interrupt
      yield assertTrue(1 <= count, count <= 4) // ≥ 1: someone is listening; ≤ 4: spawning is bounded
    },
    test("Through.Parallel drains every value, with concurrent process runs capped by parallelism") {
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
        through   = new Through.Parallel[Nothing, Int, Int]:
                      def parallelism                    = 4
                      def input: Consumer[Nothing, Int]  = source
                      def output: Producer[Nothing, Int] = sink
                      def process(value: Int): IO[Nothing, Int] =
                        inFlight.updateAndGet(_ + 1).flatMap(c => peak.update(_ max c)) *>
                          ZIO.sleep(10.millis) *>
                          inFlight.update(_ - 1) *>
                          seen.updateAndGet(_ + value).flatMap(s => allIn.succeed(()).when(s.size == n)) *>
                          ZIO.succeed(value)
        fiber    <- ZIO.scoped(through.run).fork
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
    test("Through.Batched.Parallel drains every batch, with concurrent process runs capped by parallelism") {
      for
        batches  <- Queue.unbounded[List[Int]]
        _        <- batches.offerAll(List(List(1, 2), List(3, 4), List(5, 6), List(7, 8)))
        source    = new Consumer.Batched[Nothing, Int]:
                      def consume[E2 >: Nothing](logic: List[Int] => IO[E2, Unit]): IO[E2, Unit] =
                        batches.take.flatMap(logic)
        inFlight <- Ref.make(0)
        peak     <- Ref.make(0)
        seen     <- Ref.make(Set.empty[Int])
        allIn    <- Promise.make[Nothing, Unit]
        through   = new Through.Batched.Parallel[Nothing, Int, Int]:
                      def parallelism                           = 2
                      def input: Consumer.Batched[Nothing, Int] = source
                      def output: Producer[Nothing, Int]        = sink
                      def process(values: List[Int]): IO[Nothing, List[Int]] =
                        inFlight.updateAndGet(_ + 1).flatMap(c => peak.update(_ max c)) *>
                          ZIO.sleep(20.millis) *>
                          inFlight.update(_ - 1) *>
                          seen.updateAndGet(_ ++ values).flatMap(s => allIn.succeed(()).when(s.size == 8)) *>
                          ZIO.succeed(values)
        fiber    <- ZIO.scoped(through.run).fork
        _        <- allIn.await // completes only if every batch was processed — no lost batches
        _        <- fiber.interrupt
        p        <- peak.get
        s        <- seen.get
      yield assertTrue(
        s == (1 to 8).toSet, // every element across every batch processed
        p <= 2,              // the cap held over the four-batch backlog
        p >= 2,              // and batches actually ran concurrently (a serial loop would sit at 1)
      )
    },
  ) @@ TestAspect.withLiveClock @@ TestAspect.timeout(60.seconds)
