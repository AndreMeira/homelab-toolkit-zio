package homelab.inmemory.messaging


import homelab.common.messaging.*
import zio.*
import zio.test.*


// Correctness spec for the in-memory messaging adapter and the common topology run loops it drives.
// Live clock throughout (only the Source.Tick test sleeps); a per-suite timeout turns any hang into a
// failure rather than blocking the run.
object InMemoryMessagingSpec extends ZIOSpecDefault:

  def spec = suite("in-memory messaging")(
    suite("queue channel")(
      test("a wire round-trips values through the port API in FIFO order") {
        for
          wire <- Wire.make[Int]
          ref  <- Ref.make(List.empty[Int])
          _    <- ZIO.foreachDiscard(1 to 100)(wire.emit)
          _    <- ZIO.foreachDiscard(1 to 100)(_ => wire.consume(a => ref.update(_ :+ a)))
          out  <- ref.get
        yield assertTrue(out == (1 to 100).toList)
      },
      test("consumer.map transforms every delivered value") {
        for
          wire  <- Wire.make[Int]
          ref   <- Ref.make(List.empty[Int])
          mapped = wire.consumer.map(_ * 10)
          _     <- ZIO.foreachDiscard(1 to 5)(wire.emit)
          _     <- ZIO.foreachDiscard(1 to 5)(_ => mapped.consume(a => ref.update(_ :+ a)))
          out   <- ref.get
        yield assertTrue(out == (1 to 5).map(_ * 10).toList)
      },
      test("a batched consumer delivers up to maxBatchSize per call, preserving order") {
        for
          queue   <- Queue.unbounded[Int]
          consumer = new QueueConsumer.Batched(QueueSource.Pure(queue), 10)
          _       <- queue.offerAll((1 to 25).toList)
          ref     <- Ref.make(List.empty[List[Int]])
          _       <- ZIO.foreachDiscard(1 to 3)(_ => consumer.consume(batch => ref.update(_ :+ batch)))
          out     <- ref.get
        yield assertTrue(out.map(_.size) == List(10, 10, 5), out.flatten == (1 to 25).toList)
      },
      test("a merged source delivers the union of its sources") {
        for
          q1  <- Queue.unbounded[Int]
          q2  <- Queue.unbounded[Int]
          _   <- q1.offerAll((1 to 50).toList)
          _   <- q2.offerAll((51 to 100).toList)
          m   <- QueueSource.Merged.make(List(QueueSource.Pure(q1), QueueSource.Pure(q2)))
          out <- ZIO.foreach((1 to 100).toList)(_ => m.take)
        yield assertTrue(out.size == 100, out.toSet == (1 to 100).toSet)
      },
    ),
    suite("distributer")(
      test("concurrent consume loops drain every value, preserving per-key FIFO order") {
        for
          d        <- Distributer.make[Int, Int](maxBuffer = None)(_ % 3)
          received <- Ref.make(List.empty[Int])
          allIn    <- Promise.make[Nothing, Unit]
          logic     = (a: Int) => received.updateAndGet(_ :+ a).flatMap(l => allIn.succeed(()).when(l.size == 9)).unit
          drivers  <- ZIO.foreach(1 to 3)(_ => d.consume(logic).forever.fork) // one loop per key
          _        <- ZIO.foreachDiscard(1 to 9)(d.emit) // keys 1,2,0 repeating
          _        <- allIn.await
          _        <- ZIO.foreachDiscard(drivers)(_.interrupt)
          all      <- received.get
        yield assertTrue(
          all.toSet == (1 to 9).toSet,             // every value processed
          all.filter(_ % 3 == 1) == List(1, 4, 7), // per-key FIFO preserved (one in-flight per key, in order)
          all.filter(_ % 3 == 2) == List(2, 5, 8),
          all.filter(_ % 3 == 0) == List(3, 6, 9),
        )
      },
      test("consume blocks until an emit wakes it, then processes — no lost wake-up") {
        for
          d     <- Distributer.make[Int, Int](None)(identity)
          ref   <- Ref.make(Option.empty[Int])
          fiber <- d.consume(a => ref.set(Some(a))).fork // parks: nothing pending
          _     <- d.emit(42)
          _     <- fiber.join                            // returns only once the value is processed
          out   <- ref.get
        yield assertTrue(out == Some(42))
      },
      test("a slow key never blocks other keys") {
        for
          d       <- Distributer.make[Int, Int](None)(_ % 2) // even = slow key, odd = fast key
          gate    <- Promise.make[Nothing, Unit] // the slow value parks here until the fast key has drained
          fast    <- Ref.make(List.empty[Int])
          allFast <- Promise.make[Nothing, Unit]
          logic    = (v: Int) =>
                       if v % 2 == 0 then gate.await
                       else fast.updateAndGet(_ :+ v).flatMap(l => allFast.succeed(()).when(l.size == 5)).unit
          drivers <- ZIO.foreach(1 to 2)(_ => d.consume(logic).forever.fork) // one gets stuck on the slow key
          _       <- d.emit(0)                                       // one slow value on the even key
          _       <- ZIO.foreachDiscard(List(1, 3, 5, 7, 9))(d.emit) // five fast values on the odd key
          _       <- allFast.await // completes ONLY if the fast key progresses while the slow value blocks
          _       <- gate.succeed(())
          _       <- ZIO.foreachDiscard(drivers)(_.interrupt)
          order   <- fast.get
        yield assertTrue(order == List(1, 3, 5, 7, 9))
      },
      test("keys are served round-robin — a hot key cannot starve others") {
        for
          d        <- Distributer.make[Int, Int](None)(_ % 2)
          received <- Ref.make(List.empty[Int])
          allIn    <- Promise.make[Nothing, Unit]
          logic     = (a: Int) => received.updateAndGet(_ :+ a).flatMap(l => allIn.succeed(()).when(l.size == 6)).unit
          _        <- ZIO.foreachDiscard(List(0, 2, 4))(d.emit) // backlog the even key first…
          _        <- ZIO.foreachDiscard(List(1, 3, 5))(d.emit) // …then the odd key
          driver   <- d.consume(logic).forever.fork             // ONE driver: fairness must come from scheduling
          _        <- allIn.await
          _        <- driver.interrupt
          all      <- received.get
        yield assertTrue(all == List(0, 1, 2, 3, 4, 5)) // strict alternation; head-of-map bias would give 0,2,4,1,3,5
      },
      test("a failed logic aborts its own consume call and frees the key") {
        val boom = new RuntimeException("boom")
        for
          d    <- Distributer.make[Int, Int](None)(identity)
          _    <- d.emit(1)
          exit <- d.consume(_ => ZIO.fail(boom)).exit // the failing call itself surfaces the error
          _    <- d.emit(1)                           // same key again — claimable only if the failure released it
          ref  <- Ref.make(0)
          _    <- d.consume(a => ref.set(a))
          out  <- ref.get
        yield assertTrue(exit == Exit.fail(boom), out == 1)
      },
      test("stress: a parking consumer never loses a concurrent emit's wake-up") {
        val n = 5000
        for
          d        <- Distributer.make[Int, Int](None)(identity)
          received <- Ref.make(Set.empty[Int])
          allIn    <- Promise.make[Nothing, Unit]
          consumer <- d.consume(a => received.updateAndGet(_ + a).flatMap(s => allIn.succeed(()).when(s.size == n)).unit).forever.fork
          _        <- ZIO.foreachDiscard(1 to n)(d.emit) // races the parking/waking consumer
          _        <- allIn.await                        // completes only if every value arrived
          _        <- consumer.interrupt
          out      <- received.get
        yield assertTrue(out == (1 to n).toSet)
      },
      test("stress: maxBuffer=1 — permit-gated emits never lose a wake-up or deadlock") {
        val n = 5000
        for
          d        <- Distributer.make[Int, Int](maxBuffer = Some(1))(identity)
          received <- Ref.make(Set.empty[Int])
          allIn    <- Promise.make[Nothing, Unit]
          consumer <- d.consume(a => received.updateAndGet(_ + a).flatMap(s => allIn.succeed(()).when(s.size == n)).unit).forever.fork
          _        <- ZIO.foreachDiscard(1 to n)(d.emit) // each emit blocks on the lone permit until a claim frees it
          _        <- allIn.await
          _        <- consumer.interrupt
          out      <- received.get
        yield assertTrue(out == (1 to n).toSet)
      },
      test("never deadlocks on the emit-after-permit-release timeline") {
        for {
          promise     <- Promise.make[Nothing, Int]
          distributer <- Distributer.make[Int, Int](maxBuffer = Some(1))(identity)
          _           <- distributer.consume(i => ZIO.sleep(1.seconds) *> promise.succeed(i).when(i == 2).unit).forever.fork
          _           <- distributer.emit(1)
          _           <- distributer.emit(2)
          total       <- promise.await.timeout(3.seconds)
        } yield assertTrue(total.contains(2))
      },
    ),
    suite("topology run loops")(
      test("Processor.Parallel runs its pool concurrently — two keys in flight at once") {
        for
          d      <- Distributer.make[Int, Int](None)(_ % 2)
          inWork <- Ref.make(Set.empty[Int])
          bothIn <- Promise.make[Nothing, Unit]
          gate   <- Promise.make[Nothing, Unit]
          proc    = new Processor.Parallel[Nothing, Int]:
                      def parallelism = 2
                      def input: Consumer[Nothing, Int] = d
                      def process(v: Int): IO[Nothing, Unit] =
                        inWork.updateAndGet(_ + v).flatMap(s => bothIn.succeed(()).when(s.size == 2)) *> gate.await
          fiber  <- proc.run.fork
          _      <- d.emit(0)
          _      <- d.emit(1)
          _      <- bothIn.await // completes ONLY if both values are being processed simultaneously
          _      <- gate.succeed(())
          _      <- fiber.interrupt
          seen   <- inWork.get
        yield assertTrue(seen == Set(0, 1))
      },
      test("Processor.Parallel preserves the input's per-key serialisation") {
        for
          d        <- Distributer.make[Int, Int](None)(_ => 0) // ONE key: values must process strictly one at a time
          inFlight <- Ref.make(0)
          peak     <- Ref.make(0)
          count    <- Ref.make(0)
          done     <- Promise.make[Nothing, Unit]
          proc      = new Processor.Parallel[Nothing, Int]:
                        def parallelism = 4
                        def input: Consumer[Nothing, Int] = d
                        def process(value: Int): IO[Nothing, Unit] =
                          inFlight.updateAndGet(_ + 1).flatMap(n => peak.update(_ max n)) *>
                            ZIO.sleep(20.millis) *>
                            inFlight.update(_ - 1) *>
                            count.updateAndGet(_ + 1).flatMap(c => done.succeed(()).when(c == 3)).unit
          fiber    <- proc.run.fork
          _        <- ZIO.foreachDiscard(1 to 3)(d.emit)
          _        <- done.await
          _        <- fiber.interrupt
          p        <- peak.get
        yield assertTrue(p == 1) // hand-off dispatch releases the key early and drives this to 3
      },
      test("Processor.Parallel fails fast — one loop's failure aborts run") {
        val boom = new RuntimeException("boom")
        for
          d     <- Distributer.make[Int, Int](None)(identity)
          proc   = new Processor.Parallel[Exception, Int]:
                     def parallelism = 2
                     def input: Consumer[Exception, Int] = d
                     def process(v: Int): IO[Exception, Unit] = ZIO.fail(boom)
          fiber <- proc.run.fork
          _     <- d.emit(1)
          exit  <- fiber.await
          failed = exit match
                     case Exit.Failure(cause) => cause.failures.contains(boom)
                     case _                   => false
        yield assertTrue(failed)
      },
      test("Pipe.PerItem consumes, transforms, and emits in a loop") {
        for
          in    <- Wire.make[Int]
          out   <- Wire.make[Int]
          pipe   = new Pipe.PerItem[Nothing, Int, Int]:
                     def input: Consumer[Nothing, Int]         = in
                     def output: Producer[Nothing, Int]        = out
                     def process(value: Int): IO[Nothing, Int] = ZIO.succeed(value * 10)
          fiber <- pipe.run.fork
          _     <- ZIO.foreachDiscard(1 to 20)(in.emit)
          res   <- ZIO.foreach((1 to 20).toList)(_ => out.consumer.source.take)
          _     <- fiber.interrupt
        yield assertTrue(res == (1 to 20).map(_ * 10).toList)
      },
      test("Pipe.Batched consumes batches, transforms, and emits") {
        for
          queue  <- Queue.unbounded[Int]
          out    <- Wire.make[Int]
          batched = new QueueConsumer.Batched(QueueSource.Pure(queue), 5)
          pipe    = new Pipe.Batched[Nothing, Int, Int]:
                      def input: Consumer.Batched[Nothing, Int]              = batched
                      def output: Producer[Nothing, Int]                     = out
                      def process(values: List[Int]): IO[Nothing, List[Int]] = ZIO.succeed(values.map(_ * 10))
          fiber  <- pipe.run.fork
          _      <- queue.offerAll((1 to 20).toList)
          res    <- ZIO.foreach((1 to 20).toList)(_ => out.consumer.source.take)
          _      <- fiber.interrupt
        yield assertTrue(res == (1 to 20).map(_ * 10).toList)
      },
      test("Source.Repeat generates and emits until interrupted") {
        for
          out     <- Wire.make[Int]
          counter <- Ref.make(0)
          source   = new Source.Repeat[Nothing, Int]:
                       def output: Producer[Nothing, Int] = out
                       def generate: IO[Nothing, Int]     = counter.updateAndGet(_ + 1)
          fiber   <- source.run.fork
          res     <- ZIO.foreach((1 to 10).toList)(_ => out.consumer.source.take)
          _       <- fiber.interrupt
        yield assertTrue(res == (1 to 10).toList)
      },
    ),
    suite("source scheduling")(
      test("Source.Tick waits the initial delay, then emits every interval") {
        for
          out     <- Wire.make[Int]
          counter <- Ref.make(0)
          source   = new Source.Tick[Nothing, Int]:
                       def initial: Duration              = 200.millis
                       def interval: Duration             = 50.millis
                       def output: Producer[Nothing, Int] = out
                       def generate: IO[Nothing, Int]     = counter.updateAndGet(_ + 1)
          fiber   <- source.run.fork
          // nothing within the first 100ms — the initial delay is honoured
          delayed <- out.consumer.source.take.as(false).race(ZIO.sleep(100.millis).as(true))
          e1      <- out.consumer.source.take
          e2      <- out.consumer.source.take
          e3      <- out.consumer.source.take
          _       <- fiber.interrupt
        yield assertTrue(delayed, e1 == 1, e2 == 2, e3 == 3)
      }
    ),
  ) @@ TestAspect.withLiveClock @@ TestAspect.timeout(60.seconds)
