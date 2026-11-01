package homelab.common.flow


import zio.*
import zio.test.*


object ShutdownSpec extends ZIOSpecDefault:
  import Support.*

  override def spec: Spec[TestEnvironment & Scope, Any] =
    suite("Shutdown")(
      test("make completes inside an uninterruptible region, as an acquisition is"):
        // Fails unfinished if make forks a fiber that inherits the region's status: interrupting it then waits
        // forever. Run on a fiber of its own and awaited from outside, so the timeout can fire.
        for
          making   <- ZIO.uninterruptible(Shutdown.make).fork
          finished <- Live.live(making.await.timeout(2.seconds))
        yield assertTrue(finished.exists(_.isSuccess))
      ,
      test("make completes as the acquisition of a resource released by await"):
        for
          acquired <- ZIO.scoped(ZIO.acquireRelease(Shutdown.make)(_.await)).fork
          finished <- Live.live(acquired.await.timeout(2.seconds))
        yield assertTrue(finished.exists(_.isSuccess))
      ,
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
          outcome     <- Promise.make[Nothing, String]
          ready       <- Promise.make[Nothing, Unit]
          app         <- ZIO.scoped {
                           for
                             // the outcome is recorded inside the section: once the section returns, the scope
                             // may interrupt its fiber before anything after it runs
                             _ <- shutdown
                                    .graceful(started.succeed(()) *> gate.await *> outcome.succeed("posted"))
                                    .forkScoped
                             _ <- ZIO.addFinalizer(shutdown.await)
                             _ <- ready.succeed(())
                             _ <- ZIO.never
                           yield ()
                         }.fork
          _           <- started.await
          // interrupting before the finalizer is registered would close the scope with no shutdown at all
          _           <- ready.await
          terminating <- app.interrupt.fork
          _           <- untilClosing(shutdown)
          before      <- outcome.poll
          _           <- gate.succeed(())
          _           <- terminating.join
          section     <- outcome.poll.flatMap(ZIO.foreach(_)(identity))
        yield assertTrue(before.isEmpty, section.contains("posted"))
      ,
      test("gated runs its effect before shutdown begins"):
        for
          shutdown <- Shutdown.make
          value    <- shutdown.gated(ZIO.succeed(7))
        yield assertTrue(value == 7)
      ,
      test("gated interrupts with the refusal, without running the effect, once shutdown has begun"):
        for
          shutdown <- Shutdown.make
          _        <- shutdown.await
          ran      <- Ref.make(false)
          exit     <- shutdown.gated(ran.set(true)).exit
          didRun   <- ran.get
        yield assertTrue(exit.isInterrupted, !didRun)
      ,
      test("a section whose gated effect is refused is counted out, then parks until interrupted"):
        for
          shutdown <- Shutdown.make
          started  <- Promise.make[Nothing, Unit]
          gate     <- Promise.make[Nothing, Unit]
          ran      <- Ref.make(false)
          section  <- shutdown.graceful(started.succeed(()) *> gate.await *> shutdown.gated(ran.set(true))).fork
          _        <- started.await
          waiting  <- shutdown.await.fork
          _        <- untilClosing(shutdown)
          _        <- gate.succeed(())
          _        <- waiting.join
          _        <- untilParkedOrDone(section)
          parked   <- section.poll
          exit     <- section.interrupt
          didRun   <- ran.get
        yield assertTrue(parked.isEmpty, exit.isInterrupted, !didRun)
      ,
      test("graceful lets any other interruption through"):
        for
          shutdown <- Shutdown.make
          exit     <- shutdown.graceful(ZIO.interrupt).exit
        yield assertTrue(exit.isInterrupted)
      ,
      test("graceful lets a refusal joined with a defect through"):
        for
          shutdown <- Shutdown.make
          started  <- Promise.make[Nothing, Unit]
          gate     <- Promise.make[Nothing, Unit]
          mixed     = shutdown
                        .gated(ZIO.unit)
                        .catchAllCause(cause => ZIO.failCause(cause ++ Cause.die(new RuntimeException("cleanup died"))))
          section  <- shutdown.graceful(started.succeed(()) *> gate.await *> mixed).exit.fork
          _        <- started.await
          waiting  <- shutdown.await.fork
          _        <- untilClosing(shutdown)
          _        <- gate.succeed(())
          _        <- waiting.join
          exit     <- Live.live(section.join.timeout(2.seconds))
        yield assertTrue(exit.exists(_.causeOption.flatMap(_.dieOption).exists(_.getMessage == "cleanup died")))
      ,
      test("graceful does not park another instance's refusal"):
        for
          closing <- Shutdown.make
          other   <- Shutdown.make
          _       <- closing.await
          exit    <- other.graceful(closing.gated(ZIO.unit)).exit
        yield assertTrue(exit.isInterrupted),
    ) @@ TestAspect.timeout(30.seconds)

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
     * Wait until `fiber` has stopped running: suspended, which for a refused step can only be the parking, or
     * done.
     *
     * @param fiber the fiber to watch
     * @return noop once the fiber is suspended or done
     */
    def untilParkedOrDone(fiber: Fiber.Runtime[?, ?]): UIO[Unit] =
      fiber.status
        .repeatUntil:
          case _: Fiber.Status.Suspended => true
          case Fiber.Status.Done         => true
          case _                         => false
        .unit
