package homelab.common.messaging


import homelab.common.flow.Shutdown
import homelab.common.processing.Processor
import zio.*
import zio.test.*


object ConsumerDrainSpec extends ZIOSpecDefault:
  import Support.*

  override def spec: Spec[TestEnvironment & Scope, Any] =
    suite("Consumer drains")(
      test("await waits for the handlers of a parallel processor started in the same scope before it"):
        // Fails with none finished if the processor forks its listeners into the app's scope: their finalizers
        // are installed after await's, so they run first and cut the handlers before await can wait.
        for
          shutdown <- Shutdown.make
          queue    <- Queue.unbounded[Int]
          gate     <- Promise.make[Nothing, Unit]
          running  <- Ref.make(0)
          finished <- Ref.make(0)
          ready    <- Promise.make[Nothing, Unit]
          handle    = (_: Int) => running.update(_ + 1) *> gate.await *> finished.update(_ + 1)
          app      <- ZIO.scoped {
                        for
                          _ <- Processor.parallel(Consumer.fromQueue(queue).drainLocal(shutdown), 4)(handle).forkScoped
                          _ <- ZIO.addFinalizer(shutdown.await)
                          _ <- ready.succeed(())
                          _ <- ZIO.never
                        yield ()
                      }.fork
          _        <- ready.await
          _        <- queue.offerAll(List(1, 2))
          _        <- running.get.repeatUntil(_ == 2)
          stop     <- app.interrupt.fork
          _        <- untilClosing(shutdown)
          _        <- gate.succeed(())
          _        <- stop.join
          done     <- finished.get
        yield assertTrue(done == 2)
      ,
      test("a drainLocal consumer delivers as usual before shutdown begins"):
        for
          shutdown <- Shutdown.make
          queue    <- Queue.unbounded[String]
          _        <- queue.offer("request")
          received <- Ref.make(Option.empty[String])
          _        <- Consumer.fromQueue(queue).drainLocal(shutdown).consume(value => received.set(Some(value)))
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
          gated    <- Consumer.fromQueue(queue).drainLocal(shutdown).consume(_ => ran.set(true)).fork
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
          handling <- Consumer
                        .fromQueue(queue)
                        .drainLocal(shutdown)
                        .consume(value => started.succeed(()) *> gate.await *> received.set(Some(value)))
                        .fork
          _        <- started.await
          waiting  <- shutdown.await.fork
          _        <- untilClosing(shutdown)
          _        <- untilWaiting(waiting)
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
          waiting  <- handing(queue, handed).drainLocal(shutdown).consume(_ => ran.set(true)).fork
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
          pulling  <- adapter.globalDrain(shutdown).consume(_ => ran.set(true)).fork
          _        <- untilTaking(fetches)
          waiting  <- shutdown.await.fork
          _        <- untilClosing(shutdown)
          _        <- untilWaiting(waiting)
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
          pulling  <- adapter.globalDrain(shutdown).consume(_ => ran.set(true)).fork
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
          handling <- adapter
                        .globalDrain(shutdown)
                        .consume(value => started.succeed(()) *> gate.await *> received.set(Some(value)))
                        .fork
          _        <- started.await
          waiting  <- shutdown.await.fork
          _        <- untilClosing(shutdown)
          _        <- untilWaiting(waiting)
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
          gated    <- adapter.globalDrain(shutdown).consume(_ => ran.set(true)).fork
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
          processor <- Processor.parallel(adapter.globalDrain(shutdown), 4)(_ => ZIO.unit).forkScoped
          _         <- untilTaking(fetches)
          waiting   <- shutdown.await.fork
          _         <- untilClosing(shutdown)
          _         <- fetches.offer(Some("late"))
          _         <- waiting.join
          back      <- adapter.handedBack.get
          _         <- untilWaiting(processor)
          running   <- processor.poll
          _         <- processor.interrupt
        yield assertTrue(back == List("late"), running.isEmpty)
      ,
      test("globalDrain: with decoding on top of the drain, a late value that does not decode is still refused"):
        for
          shutdown <- Shutdown.make
          fetches  <- Queue.unbounded[Option[String]]
          adapter  <- Fetching.make(fetches)
          pulling  <- adapter.globalDrain(shutdown).mapZIO(decode).consume(_ => ZIO.unit).fork
          _        <- untilTaking(fetches)
          waiting  <- shutdown.await.fork
          _        <- untilClosing(shutdown)
          _        <- fetches.offer(Some("oops"))
          _        <- waiting.join
          back     <- adapter.handedBack.get
          _        <- untilParkedOrDone(pulling)
          parked   <- pulling.poll
          _        <- pulling.interrupt
        yield assertTrue(back == List("oops"), parked.isEmpty)
      ,
      test("globalDrain: with decoding inside the drain, a late value that does not decode reaches the failure"):
        for
          shutdown <- Shutdown.make
          fetches  <- Queue.unbounded[Option[String]]
          adapter  <- Fetching.make(fetches)
          pulling  <- adapter.mapZIO(decode).globalDrain(shutdown).consume(_ => ZIO.unit).exit.fork
          _        <- untilTaking(fetches)
          waiting  <- shutdown.await.fork
          _        <- untilClosing(shutdown)
          _        <- fetches.offer(Some("oops"))
          _        <- waiting.join
          exit     <- pulling.join
          back     <- adapter.handedBack.get
        yield assertTrue(exit == Exit.fail("not a number: oops"), back.isEmpty)
      ,
      test("globalDrain: a refusal that comes back with a defect is not swallowed"):
        // Fails parked if the wrapper catches any cause that contains its refusal: the defect would be hidden
        // and the fail-fast processor would never see it.
        for
          shutdown <- Shutdown.make
          fetches  <- Queue.unbounded[Option[String]]
          adapter  <- Fetching.make(fetches)
          pulling  <- dyingOnHandBack(adapter).globalDrain(shutdown).consume(_ => ZIO.unit).exit.fork
          _        <- untilTaking(fetches)
          waiting  <- shutdown.await.fork
          _        <- untilClosing(shutdown)
          _        <- fetches.offer(Some("late"))
          _        <- waiting.join
          exit     <- Live.live(pulling.join.timeout(2.seconds))
        yield assertTrue(exit.exists(_.causeOption.flatMap(_.dieOption).exists(_.getMessage == "cleanup died")))
      ,
      test("globalDrain: an interruption raised by logic itself is not mistaken for a refusal"):
        for
          shutdown <- Shutdown.make
          fetches  <- Queue.unbounded[Option[String]]
          _        <- fetches.offer(Some("request"))
          adapter  <- Fetching.make(fetches)
          exit     <- adapter.globalDrain(shutdown).consume(_ => ZIO.interrupt).exit
          back     <- adapter.handedBack.get
        yield assertTrue(exit.isInterrupted, back == List("request")),
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
     * Wait until `fiber` is waiting on a promise, as `await` does, or done. Waiting means suspended and
     * blocking on another fiber: a fiber that has not started yet, or is between steps, also reports
     * `Suspended`, but blocking on nobody, so `Suspended` alone would let a test look before it has begun to
     * wait. Refused work parks on `ZIO.never`, which blocks on nobody too, so this is not for refused fibers;
     * [[untilParkedOrDone]] is.
     *
     * @param fiber a fiber expected to wait on a promise
     * @return noop once it is waiting on one, or done
     */
    def untilWaiting(fiber: Fiber.Runtime[?, ?]): UIO[Unit] =
      (ZIO.yieldNow *> fiber.status)
        .repeatUntil:
          case Fiber.Status.Suspended(_, _, blockingOn) => blockingOn != FiberId.None
          case Fiber.Status.Done                        => true
          case _                                        => false
        .unit

    /**
     * A consumer whose cleanup dies whenever `logic` is interrupted, so an interruption comes back joined
     * with a defect, as a failing finalizer would make it.
     *
     * @param consumer the consumer to wrap
     * @return the consumer; dies with "cleanup died" on top of any interruption of `logic`
     */
    def dyingOnHandBack(consumer: Consumer[Nothing, String]): Consumer[Nothing, String] = new Consumer[Nothing, String]:
      override def consume[E2 >: Nothing](logic: String => IO[E2, Unit]): IO[E2, Unit] =
        consumer
          .consume(logic)
          .catchAllCause: cause =>
            if cause.isInterrupted then ZIO.failCause(cause ++ Cause.die(new RuntimeException("cleanup died")))
            else ZIO.failCause(cause)

    /**
     * Decode a fetched value as a number, as a decoding consumer's `mapZIO` would.
     *
     * @param value the fetched text
     * @return the number; aborts with a message when `value` is not one
     */
    def decode(value: String): IO[String, Int] =
      ZIO.fromOption(value.toIntOption).orElseFail(s"not a number: $value")

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
