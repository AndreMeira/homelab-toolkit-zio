package homelab.incubator.processing.shutdown


import homelab.common.messaging.Consumer
import homelab.common.processing.Processor
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
          ready       <- Promise.make[Nothing, Unit]
          app         <- ZIO.scoped {
                           for
                             _ <- shutdown
                                    .graceful(started.succeed(()) *> gate.await.as("posted"))
                                    .exit
                                    .flatMap(outcome.succeed)
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
          section     <- outcome.await
        yield assertTrue(before.isEmpty, section == Exit.succeed("posted"))
      ,
      test("a drainLocal consumer delivers as usual before shutdown begins"):
        for
          shutdown <- Shutdown.make
          queue    <- Queue.unbounded[String]
          _        <- queue.offer("request")
          received <- Ref.make(Option.empty[String])
          _        <- shutdown.drainLocal(Consumer.fromQueue(queue)).consume(value => received.set(Some(value)))
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
          gated    <- shutdown.drainLocal(Consumer.fromQueue(queue)).consume(_ => ran.set(true)).fork
          parked   <- gated.poll
          exit     <- gated.interrupt
          left     <- queue.size
          didRun   <- ran.get
        yield assertTrue(parked.isEmpty, exit.isInterrupted, left == 1, !didRun)
      ,
      test("await waits for a value a drainLocal consumer is already handling"):
        for
          shutdown <- Shutdown.make
          queue    <- Queue.unbounded[String]
          _        <- queue.offer("request")
          started  <- Promise.make[Nothing, Unit]
          gate     <- Promise.make[Nothing, Unit]
          received <- Ref.make(Option.empty[String])
          handling <- shutdown
                        .drainLocal(Consumer.fromQueue(queue))
                        .consume(value => started.succeed(()) *> gate.await *> received.set(Some(value)))
                        .fork
          _        <- started.await
          waiting  <- shutdown.await.fork
          _        <- untilClosing(shutdown)
          before   <- waiting.poll
          _        <- gate.succeed(())
          _        <- waiting.join
          _        <- handling.join
          value    <- received.get
        yield assertTrue(before.isEmpty, value.contains("request"))
      ,
      test("a value taken after shutdown began is refused, and parks until interrupted"):
        for
          shutdown <- Shutdown.make
          queue    <- Queue.unbounded[String]
          handed   <- Promise.make[Nothing, Unit]
          ran      <- Ref.make(false)
          waiting  <- shutdown.drainLocal(handing(queue, handed)).consume(_ => ran.set(true)).fork
          _        <- untilTaking(queue)
          _        <- shutdown.await
          _        <- queue.offer("request")
          _        <- handed.await
          _        <- untilParkedOrDone(waiting)
          parked   <- waiting.poll
          exit     <- waiting.interrupt
          didRun   <- ran.get
        yield assertTrue(parked.isEmpty, exit.isInterrupted, !didRun)
      ,
      test("globalDrain: await waits for an open fetch, which may end empty"):
        for
          shutdown <- Shutdown.make
          fetches  <- Queue.unbounded[Option[String]]
          adapter  <- Fetching.make(fetches)
          ran      <- Ref.make(false)
          pulling  <- shutdown.globalDrain(adapter).consume(_ => ran.set(true)).fork
          _        <- untilTaking(fetches)
          waiting  <- shutdown.await.fork
          _        <- untilClosing(shutdown)
          before   <- waiting.poll
          _        <- fetches.offer(None)
          _        <- waiting.join
          _        <- pulling.join
          didRun   <- ran.get
        yield assertTrue(before.isEmpty, !didRun)
      ,
      test("globalDrain: a value on its way when shutdown begins is handed back at once, and the call parks"):
        for
          shutdown <- Shutdown.make
          fetches  <- Queue.unbounded[Option[String]]
          adapter  <- Fetching.make(fetches)
          ran      <- Ref.make(false)
          pulling  <- shutdown.globalDrain(adapter).consume(_ => ran.set(true)).fork
          _        <- untilTaking(fetches)
          waiting  <- shutdown.await.fork
          _        <- untilClosing(shutdown)
          _        <- fetches.offer(Some("late"))
          _        <- waiting.join
          back     <- adapter.handedBack.get
          _        <- untilParkedOrDone(pulling)
          parked   <- pulling.poll
          exit     <- pulling.interrupt
          didRun   <- ran.get
        yield assertTrue(back == List("late"), parked.isEmpty, exit.isInterrupted, !didRun)
      ,
      test("globalDrain: a value that arrived before shutdown runs, and await waits for it"):
        for
          shutdown <- Shutdown.make
          fetches  <- Queue.unbounded[Option[String]]
          _        <- fetches.offer(Some("request"))
          adapter  <- Fetching.make(fetches)
          started  <- Promise.make[Nothing, Unit]
          gate     <- Promise.make[Nothing, Unit]
          received <- Ref.make(Option.empty[String])
          handling <- shutdown
                        .globalDrain(adapter)
                        .consume(value => started.succeed(()) *> gate.await *> received.set(Some(value)))
                        .fork
          _        <- started.await
          waiting  <- shutdown.await.fork
          _        <- untilClosing(shutdown)
          before   <- waiting.poll
          _        <- gate.succeed(())
          _        <- waiting.join
          _        <- handling.join
          value    <- received.get
          back     <- adapter.handedBack.get
        yield assertTrue(before.isEmpty, value.contains("request"), back.isEmpty)
      ,
      test("globalDrain: a consume started after shutdown began never fetches"):
        for
          shutdown <- Shutdown.make
          fetches  <- Queue.unbounded[Option[String]]
          _        <- fetches.offer(Some("request"))
          adapter  <- Fetching.make(fetches)
          _        <- shutdown.await
          ran      <- Ref.make(false)
          gated    <- shutdown.globalDrain(adapter).consume(_ => ran.set(true)).fork
          parked   <- gated.poll
          exit     <- gated.interrupt
          left     <- fetches.size
          didRun   <- ran.get
        yield assertTrue(parked.isEmpty, exit.isInterrupted, left == 1, !didRun)
      ,
      test("globalDrain: a refusal does not fail the parallel processor running it"):
        for
          shutdown  <- Shutdown.make
          fetches   <- Queue.unbounded[Option[String]]
          adapter   <- Fetching.make(fetches)
          processor <- Processor.parallel(shutdown.globalDrain(adapter), 4)(_ => ZIO.unit).forkScoped
          _         <- untilTaking(fetches)
          waiting   <- shutdown.await.fork
          _         <- untilClosing(shutdown)
          _         <- fetches.offer(Some("late"))
          _         <- waiting.join
          back      <- adapter.handedBack.get
          running   <- processor.poll
          _         <- processor.interrupt
        yield assertTrue(back == List("late"), running.isEmpty)
      ,
      test("globalDrain: an interruption raised by logic itself is not mistaken for a refusal"):
        for
          shutdown <- Shutdown.make
          fetches  <- Queue.unbounded[Option[String]]
          _        <- fetches.offer(Some("request"))
          adapter  <- Fetching.make(fetches)
          exit     <- shutdown.globalDrain(adapter).consume(_ => ZIO.interrupt).exit
          back     <- adapter.handedBack.get
        yield assertTrue(exit.isInterrupted, back == List("request")),
    ) @@ TestAspect.diagnose(20.seconds) @@ TestAspect.timeout(30.seconds)

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
    def untilTaking(queue: Queue[?]): UIO[Unit] =
      queue.size.repeatUntil(_ < 0).unit

    /**
     * A consumer over `queue` that signals once it has taken a value and is about to hand it to `logic`, so a
     * spec knows the value has left the queue before deciding anything about it.
     *
     * @param queue the queue to take from
     * @param handed completed once a value has been taken
     * @return the signalling consumer
     */
    def handing(queue: Queue[String], handed: Promise[Nothing, Unit]): Consumer[Nothing, String] =
      new Consumer[Nothing, String]:
        override def consume[E2 >: Nothing](logic: String => IO[E2, Unit]): IO[E2, Unit] =
          queue.take.flatMap(value => handed.succeed(()) *> logic(value))

    /**
     * Wait until `fiber` has stopped running: suspended, which past [[handing]]'s signal can only be the
     * refusal's parking, or done, which is what an admitted `logic` would lead to.
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

    /**
     * A fake adapter that fetches with a timeout, as a JetStream pull does: each `consume` makes one fetch,
     * which is a take from `fetches` (`None` an empty fetch, `Some` a value), and an interrupted `logic` has
     * its value handed back, recorded in `handedBack`, before the interruption carries on.
     *
     * @param fetches what each fetch returns, in order
     * @param handedBack the values handed back, oldest first
     */
    final class Fetching private (fetches: Queue[Option[String]], val handedBack: Ref[List[String]]) extends Consumer[Nothing, String]:

      override def consume[E2 >: Nothing](logic: String => IO[E2, Unit]): IO[E2, Unit] =
        ZIO.uninterruptibleMask: restore =>
          restore(fetches.take).flatMap:
            case None        => ZIO.unit
            case Some(value) =>
              restore(logic(value)).exit.flatMap: exit =>
                handedBack.update(_ :+ value).when(exit.isInterrupted) *> ZIO.done(exit)

    object Fetching:

      /**
       * A fake adapter over `fetches`, with nothing handed back yet.
       *
       * @param fetches what each fetch returns, in order
       * @return the adapter; never fails
       */
      def make(fetches: Queue[Option[String]]): UIO[Fetching] =
        Ref.make(List.empty[String]).map(new Fetching(fetches, _))
