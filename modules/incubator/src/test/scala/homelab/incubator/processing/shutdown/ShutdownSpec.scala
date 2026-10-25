package homelab.incubator.processing.shutdown

import homelab.common.messaging.Consumer
import zio.*
import zio.test.*


object ShutdownSpec extends ZIOSpecDefault:
  import Support.*

  override def spec: Spec[TestEnvironment & Scope, Any] =
    suite("Shutdown")(
      test("await returns at once when no section is running"):
        for
          shutdown <- Shutdown.make
          _        <- shutdown.await
        yield assertCompletes
      ,
      test("await waits for a running section, which completes with its value"):
        for
          shutdown <- Shutdown.make
          started  <- Promise.make[Nothing, Unit]
          gate     <- Promise.make[Nothing, Unit]
          section  <- shutdown.graceful(started.succeed(()) *> gate.await.as("posted")).fork
          _        <- started.await
          waiting  <- shutdown.await.fork
          before   <- waiting.poll
          _        <- gate.succeed(())
          _        <- waiting.join
          value    <- section.join
        yield assertTrue(before.isEmpty, value == "posted")
      ,
      test("a section started after shutdown began never starts, and parks until interrupted"):
        for
          shutdown <- Shutdown.make
          _        <- shutdown.await
          ran      <- Ref.make(false)
          refused  <- shutdown.graceful(ran.set(true)).fork
          parked   <- refused.poll
          exit     <- refused.interrupt
          didRun   <- ran.get
        yield assertTrue(parked.isEmpty, exit.isInterrupted, !didRun)
      ,
      test("a refused section does not hold up the shutdown that refused it"):
        for
          shutdown <- Shutdown.make
          started  <- Promise.make[Nothing, Unit]
          gate     <- Promise.make[Nothing, Unit]
          running  <- shutdown.graceful(started.succeed(()) *> gate.await).fork
          _        <- started.await
          waiting  <- shutdown.await.fork
          _        <- untilClosing(shutdown)
          ran      <- Ref.make(false)
          refused  <- shutdown.graceful(ran.set(true)).fork
          _        <- gate.succeed(())
          _        <- waiting.join
          _        <- running.join
          parked   <- refused.poll
          didRun   <- ran.get
          _        <- refused.interrupt
        yield assertTrue(parked.isEmpty, !didRun)
      ,
      test("an interrupted section is counted out"):
        for
          shutdown <- Shutdown.make
          started  <- Promise.make[Nothing, Unit]
          section  <- shutdown.graceful(started.succeed(()) *> ZIO.never).fork
          _        <- started.await
          _        <- section.interrupt
          _        <- shutdown.await
        yield assertCompletes
      ,
      test("a failing section passes its error through and is counted out"):
        for
          shutdown <- Shutdown.make
          exit     <- shutdown.graceful(ZIO.fail("refused by the remote")).exit
          _        <- shutdown.await
        yield assertTrue(exit == Exit.fail("refused by the remote"))
      ,
      test("a refused section is interrupted when its scope closes"):
        for
          shutdown <- Shutdown.make
          _        <- shutdown.await
          ran      <- Ref.make(false)
          exit     <- ZIO.scoped(shutdown.graceful(ran.set(true)).forkScoped).flatMap(_.await)
          didRun   <- ran.get
        yield assertTrue(exit.isInterrupted, !didRun)
      ,
      test("await added as the last finalizer drains before an interrupted app's scope interrupts its work"):
        for
          shutdown    <- Shutdown.make
          started     <- Promise.make[Nothing, Unit]
          gate        <- Promise.make[Nothing, Unit]
          outcome     <- Promise.make[Nothing, Exit[Nothing, String]]
          app         <- ZIO.scoped {
                           for
                             _ <- shutdown.graceful(started.succeed(()) *> gate.await.as("posted"))
                                    .exit
                                    .flatMap(outcome.succeed)
                                    .forkScoped
                             _ <- ZIO.addFinalizer(shutdown.await)
                             _ <- ZIO.never
                           yield ()
                         }.fork
          _           <- started.await
          terminating <- app.interrupt.fork
          _           <- untilClosing(shutdown)
          before      <- outcome.poll
          _           <- gate.succeed(())
          _           <- terminating.join
          section     <- outcome.await
        yield assertTrue(before.isEmpty, section == Exit.succeed("posted"))
      ,
      test("a gated consumer delivers as usual before shutdown begins"):
        for
          shutdown <- Shutdown.make
          queue    <- Queue.unbounded[String]
          _        <- queue.offer("request")
          received <- Ref.make(Option.empty[String])
          _        <- shutdown.gate(Consumer.fromQueue(queue)).consume(value => received.set(Some(value)))
          value    <- received.get
        yield assertTrue(value.contains("request"))
      ,
      test("a consume started after shutdown began never reaches the underlying consumer"):
        for
          shutdown <- Shutdown.make
          queue    <- Queue.unbounded[String]
          _        <- queue.offer("request")
          _        <- shutdown.await
          ran      <- Ref.make(false)
          gated    <- shutdown.gate(Consumer.fromQueue(queue)).consume(_ => ran.set(true)).fork
          parked   <- gated.poll
          exit     <- gated.interrupt
          left     <- queue.size
          didRun   <- ran.get
        yield assertTrue(parked.isEmpty, exit.isInterrupted, left == 1, !didRun)
      ,
      test("a consume already waiting when shutdown begins still delivers"):
        for
          shutdown <- Shutdown.make
          queue    <- Queue.unbounded[String]
          received <- Promise.make[Nothing, String]
          waiting  <- shutdown.gate(Consumer.fromQueue(queue)).consume(value => received.succeed(value).unit).fork
          _        <- untilTaking(queue)
          _        <- shutdown.await
          _        <- queue.offer("request")
          value    <- received.await
          _        <- waiting.join
        yield assertTrue(value == "request")
    )

  private object Support:

    /**
     * Wait until shutdown has begun, by probing with a section that runs only while it has not. Each probe that
     * is admitted completes at once, and the first refused one is interrupted.
     *
     * @param shutdown the instance being shut down
     * @return noop once a section would be refused
     */
    def untilClosing(shutdown: Shutdown): UIO[Unit] =
      Live
        .live(shutdown.graceful(ZIO.unit).timeout(10.millis))
        .repeatUntil(_.isEmpty)
        .unit

    /**
     * Wait until a consumer is parked taking from `queue`.
     *
     * @param queue the queue the consumer takes from
     * @return noop once one taker is waiting
     */
    def untilTaking(queue: Queue[String]): UIO[Unit] =
      queue.size.repeatUntil(_ < 0).unit
