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
      test("processes one element per key per round, keeping per-key FIFO order") {
        for
          d   <- Distributer.make[Int, Int](parallelism = None)(_ % 3)
          _   <- ZIO.foreachDiscard(1 to 9)(d.emit) // keys 1,2,0 repeating
          ref <- Ref.make(List.empty[Int])
          _   <- ZIO.foreachDiscard(1 to 3)(_ => d.consume(a => ref.update(_ :+ a)))
          all <- ref.get
          round1 = all.take(3)
        yield assertTrue(
          round1.toSet == Set(1, 2, 3),            // one head per key in the first round
          all.filter(_ % 3 == 1) == List(1, 4, 7), // per-key FIFO preserved across rounds
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
          _     <- fiber.join                            // returns only if the wake-up landed
          out   <- ref.get
        yield assertTrue(out == Some(42))
      },
      test("bounded parallelism still processes every element of a round") {
        for
          d   <- Distributer.make[Int, Int](parallelism = Some(2))(_ % 4)
          _   <- ZIO.foreachDiscard(1 to 8)(d.emit)
          ref <- Ref.make(Set.empty[Int])
          _   <- ZIO.foreachDiscard(1 to 2)(_ => d.consume(a => ref.update(_ + a)))
          out <- ref.get
        yield assertTrue(out == (1 to 8).toSet)
      },
    ),
    suite("topology run loops")(
      test("Pipe.PerItem consumes, transforms, and emits in a loop") {
        for
          in  <- Wire.make[Int]
          out <- Wire.make[Int]
          pipe = new Pipe.PerItem[Nothing, Int, Int]:
                   def input: Consumer[Nothing, Int]  = in
                   def output: Producer[Nothing, Int] = out
                   def process(value: Int): IO[Nothing, Int] = ZIO.succeed(value * 10)
          fiber <- pipe.run.fork
          _     <- ZIO.foreachDiscard(1 to 20)(in.emit)
          res   <- ZIO.foreach((1 to 20).toList)(_ => out.consumer.source.take)
          _     <- fiber.interrupt
        yield assertTrue(res == (1 to 20).map(_ * 10).toList)
      },
      test("Pipe.Batched consumes batches, transforms, and emits") {
        for
          queue   <- Queue.unbounded[Int]
          out     <- Wire.make[Int]
          batched  = new QueueConsumer.Batched(QueueSource.Pure(queue), 5)
          pipe     = new Pipe.Batched[Nothing, Int, Int]:
                       def input: Consumer.Batched[Nothing, Int] = batched
                       def output: Producer[Nothing, Int]        = out
                       def process(values: List[Int]): IO[Nothing, List[Int]] = ZIO.succeed(values.map(_ * 10))
          fiber   <- pipe.run.fork
          _       <- queue.offerAll((1 to 20).toList)
          res     <- ZIO.foreach((1 to 20).toList)(_ => out.consumer.source.take)
          _       <- fiber.interrupt
        yield assertTrue(res == (1 to 20).map(_ * 10).toList)
      },
      test("Source.Repeat generates and emits until interrupted") {
        for
          out    <- Wire.make[Int]
          counter <- Ref.make(0)
          source  = new Source.Repeat[Nothing, Int]:
                      def output: Producer[Nothing, Int] = out
                      def generate: IO[Nothing, Int]     = counter.updateAndGet(_ + 1)
          fiber  <- source.run.fork
          res    <- ZIO.foreach((1 to 10).toList)(_ => out.consumer.source.take)
          _      <- fiber.interrupt
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
      },
    ),
  ) @@ TestAspect.withLiveClock @@ TestAspect.timeout(60.seconds)
