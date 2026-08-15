package homelab.common.processing


import homelab.common.error.ApplicationError.AdapterError
import homelab.common.messaging.Pipe
import zio.*
import zio.test.*


// Correctness spec for the request/reply Worker: replies routed to the caller that asked, failures confined
// to that caller, `send` not waiting, order preserved by the serial worker and concurrency capped by the
// parallel one — and, the case that is easy to get wrong, an in-flight caller released when the worker is
// torn down rather than left parked. Driven with promises rather than sleeps; a per-suite timeout turns a
// stranded caller into a failure instead of a hang.
object WorkerSpec extends ZIOSpecDefault:

  private val boom: AdapterError = new AdapterError:
    override def message: String = "boom"

  private type Payload = (String, Promise[AdapterError, Int])

  /** A serial worker over a fresh queue, handling messages with `handler`. */
  private def build(handler: String => IO[AdapterError, Int]): UIO[Worker[AdapterError, String, Int]] =
    Queue.unbounded[Payload].map(Pipe.fromQueue).map { pipe =>
      new Worker[AdapterError, String, Int]:
        override val input: Pipe[AdapterError, Payload]        = pipe
        override val receive: String => IO[AdapterError, Int] = handler
    }

  /** The same, handling up to `limit` messages at once. */
  private def buildParallel(limit: Int)(handler: String => IO[AdapterError, Int]): UIO[Worker[AdapterError, String, Int]] =
    Queue.unbounded[Payload].map(Pipe.fromQueue).map { pipe =>
      new Worker.Parallel[AdapterError, String, Int]:
        override val input: Pipe[AdapterError, Payload]        = pipe
        override val receive: String => IO[AdapterError, Int] = handler
        override val parallelism: Int                         = limit
    }

  def spec: Spec[TestEnvironment & Scope, Any] = suite("Worker")(
    test("ask returns what the handler produced") {
      ZIO.scoped {
        for
          worker <- build(message => ZIO.succeed(message.length))
          _      <- worker.run.forkScoped
          reply  <- worker.ask("hello")
        yield assertTrue(reply == 5)
      }
    },
    test("a failing handler fails only the caller that asked, and the worker carries on") {
      ZIO.scoped {
        for
          worker <- build(message => if message == "bad" then ZIO.fail(boom) else ZIO.succeed(message.length))
          _      <- worker.run.forkScoped
          failed <- worker.ask("bad").exit
          after  <- worker.ask("hi")
        yield assertTrue(failed == Exit.fail(boom), after == 2)
      }
    },
    test("handles one message at a time") {
      ZIO.scoped {
        for
          inFlight <- Ref.make(0)
          peak     <- Ref.make(0)
          worker   <- build: _ =>
                        inFlight.updateAndGet(_ + 1).flatMap(now => peak.update(_ max now)) *>
                          ZIO.yieldNow.repeatN(3) *> // every chance for a second handler to overlap
                          inFlight.update(_ - 1).as(0)
          _        <- worker.run.forkScoped
          _        <- ZIO.foreachParDiscard(1 to 20)(n => worker.ask(n.toString))
          observed <- peak.get
        yield assertTrue(observed == 1)
      }
    },
    test("handles messages in the order they were sent") {
      ZIO.scoped {
        for
          trace  <- Ref.make(List.empty[String])
          worker <- build(message => trace.update(_ :+ message).as(0))
          _      <- worker.run.forkScoped
          _      <- worker.send("a")
          _      <- worker.send("b")
          _      <- worker.send("c")
          _      <- worker.ask("last") // arrives behind the three sends, so its reply proves they ran
          order  <- trace.get
        yield assertTrue(order == List("a", "b", "c", "last"))
      }
    },
    test("send returns without waiting for the handler") {
      ZIO.scoped {
        for
          gate      <- Promise.make[Nothing, Unit]
          started   <- Promise.make[Nothing, Unit]
          completed <- Ref.make(false)
          worker    <- build(_ => started.succeed(()) *> gate.await *> completed.set(true).as(0))
          _         <- worker.run.forkScoped
          _         <- worker.send("slow")
          _         <- started.await          // the handler is in flight…
          pending   <- completed.get          // …and send has already returned
          _         <- gate.succeed(())
        yield assertTrue(!pending)
      }
    },
    test("tearing the worker down releases the caller whose message was in flight") {
      // Without settling the promise from an `onExit`, this caller would wait for the life of its fiber and
      // the suite would time out rather than fail.
      for
        gate    <- Promise.make[Nothing, Unit]
        started <- Promise.make[Nothing, Unit]
        asked   <- ZIO.scoped {
                     for
                       worker <- build(_ => started.succeed(()) *> gate.await.as(0))
                       _      <- worker.run.forkScoped
                       caller <- worker.ask("held").exit.fork
                       _      <- started.await // the handler is running when the scope closes
                     yield caller
                   }
        exit    <- asked.join
      yield assertTrue(exit.isInterrupted)
    },
    test("Parallel runs up to its limit at once, and no further") {
      ZIO.scoped {
        for
          inFlight <- Ref.make(0)
          peak     <- Ref.make(0)
          filled   <- Promise.make[Nothing, Unit]
          gate     <- Promise.make[Nothing, Unit]
          worker   <- buildParallel(3): _ =>
                        inFlight
                          .updateAndGet(_ + 1)
                          .flatMap(now => peak.update(_ max now) *> filled.succeed(()).when(now == 3)) *>
                          gate.await *> inFlight.update(_ - 1).as(0)
          _        <- worker.run.forkScoped
          callers  <- ZIO.foreach(1 to 5)(n => worker.ask(n.toString).fork)
          _        <- filled.await // three were inside the handler together
          _        <- gate.succeed(())
          _        <- ZIO.foreachDiscard(callers)(_.join)
          observed <- peak.get
        yield assertTrue(observed == 3) // never four, though five were asked
      }
    },
  ) @@ TestAspect.withLiveClock @@ TestAspect.timeout(60.seconds)
