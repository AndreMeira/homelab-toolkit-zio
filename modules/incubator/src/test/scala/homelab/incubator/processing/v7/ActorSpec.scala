package homelab.incubator.processing.v7


import homelab.common.error.ApplicationError.AdapterError
import homelab.common.store.Bucket
import homelab.incubator.processing.actor.v7.Actor
import homelab.incubator.processing.actor.v7.Actor.{ Next, Running, Self, Terminated }
import zio.*
import zio.test.*


// Correctness spec for the v7 actor: state carried between messages, one message at a time in arrival order,
// failures confined to their own caller, `Done` ending the entity for good, self-messages queueing rather
// than re-entering, and background work outliving the step that started it. Driven with promises and yields
// rather than sleeps; a per-suite timeout turns a wedged entity into a failure rather than a hang.
object ActorSpec extends ZIOSpecDefault:

  private val boom: AdapterError = new AdapterError:
    override def message: String = "boom"

  private enum Message:
    case Inc(by: Int)
    case Stop
    case Boom
    case Wait(gate: Promise[Nothing, Unit])
    case Follow(by: Int) // self-sends an Inc from inside the step
    case Later(work: UIO[Message]) // pipes the work's result back as a message

  /**
   * A counter: every message replies with the running total. `seeds` counts how often the entity was seeded,
   * so a test can tell "carried its state" from "started again".
   */
  private def counter(seeds: Ref[Int], trace: Ref[List[String]] = null): Actor[AdapterError, Message, Int, Int] =
    new Actor[AdapterError, Message, Int, Int]:
      def init(message: Message): IO[AdapterError, Int] = seeds.update(_ + 1).as(0)

      def next(self: Self[Message]): (input: Message, state: Int) => IO[AdapterError, Next[Int, Int]] =
        (message, total) =>
          message match
            case Message.Inc(by)     => ZIO.succeed(Next.Continue(total + by, total + by))
            case Message.Stop        => ZIO.succeed(Next.Done(total))
            case Message.Boom        => ZIO.fail(boom)
            case Message.Wait(gate)  => gate.await.as(Next.Continue(total, total))
            case Message.Follow(by)  => self.send(Message.Inc(by)).as(Next.Continue(total, total))
            case Message.Later(work) => self.pipeToSelf(work).as(Next.Continue(total, total))

  private def spawn(seeds: Ref[Int], state: Option[Bucket[Int]] = None, mailbox: Option[Int] = None) =
    Actor.spawn(counter(seeds), state, mailbox)

  def spec: Spec[TestEnvironment & Scope, Any] = suite("Actor")(
    test("carries its state between messages, seeding once") {
      ZIO.scoped {
        for
          seeds <- Ref.make(0)
          actor <- spawn(seeds)
          first <- actor.ask(Message.Inc(1))
          then_ <- actor.ask(Message.Inc(2))
          count <- seeds.get
        yield assertTrue(first == 1, then_ == 3, count == 1)
      }
    },
    test("processes one message at a time under concurrent senders") {
      ZIO.scoped {
        for
          seeds    <- Ref.make(0)
          inFlight <- Ref.make(0)
          peak     <- Ref.make(0)
          behaviour = new Actor[AdapterError, Message, Int, Int]:
                        def init(message: Message): IO[AdapterError, Int] = seeds.update(_ + 1).as(0)
                        def next(self: Self[Message])                     =
                          (_, total) =>
                            inFlight.updateAndGet(_ + 1).flatMap(now => peak.update(_ max now)) *>
                              ZIO.yieldNow.repeatN(3) *> // every chance for a second drain to interleave
                              inFlight.update(_ - 1).as(Next.Continue(total + 1, total + 1))
          actor    <- Actor.spawn(behaviour)
          _        <- ZIO.foreachParDiscard(1 to 30)(_ => actor.ask(Message.Inc(1)))
          observed <- peak.get
          total    <- actor.ask(Message.Inc(1))
        yield assertTrue(observed == 1, total == 31)
      }
    },
    test("a failing step fails only its own caller, and the entity carries on") {
      ZIO.scoped {
        for
          seeds  <- Ref.make(0)
          actor  <- spawn(seeds)
          failed <- actor.ask(Message.Boom).exit
          after  <- actor.ask(Message.Inc(1))
        yield assertTrue(failed == Exit.fail(boom), after == 1)
      }
    },
    test("Done replies, ends the entity, and refuses everything after") {
      ZIO.scoped {
        for
          seeds <- Ref.make(0)
          actor <- spawn(seeds)
          _     <- actor.ask(Message.Inc(5))
          ended <- actor.ask(Message.Stop)
          after <- actor.ask(Message.Inc(1)).exit
          last  <- actor.await
        yield assertTrue(ended == 5, after == Exit.fail(Terminated), last == 5)
      }
    },
    test("messages queued behind a Done are refused rather than run") {
      ZIO.scoped {
        for
          seeds   <- Ref.make(0)
          gate    <- Promise.make[Nothing, Unit]
          actor   <- spawn(seeds)
          _       <- actor.send(Message.Wait(gate)) // occupies the entity
          _       <- actor.send(Message.Stop)       // queued behind it
          refused <- actor.ask(Message.Inc(1)).exit.fork
          _       <- ZIO.yieldNow.repeatN(10)       // let it queue while the entity is still gated
          _       <- gate.succeed(())
          exit    <- refused.join
        yield assertTrue(exit == Exit.fail(Terminated))
      }
    },
    test("a self-send queues behind the current step instead of re-entering it") {
      ZIO.scoped {
        for
          trace    <- Ref.make(List.empty[String])
          behaviour = new Actor[AdapterError, Message, Int, Int]:
                        def init(message: Message): IO[AdapterError, Int] = ZIO.succeed(0)
                        def next(self: Self[Message])                     =
                          (message, total) =>
                            message match
                              case Message.Follow(by) =>
                                trace.update(_ :+ "follow:start") *>
                                  self.send(Message.Inc(by)) *>
                                  trace.update(_ :+ "follow:end").as(Next.Continue(total, total))
                              case _                  =>
                                trace.update(_ :+ "inc").as(Next.Continue(total + 1, total + 1))
          actor    <- Actor.spawn(behaviour)
          _        <- actor.ask(Message.Follow(1))
          _        <- actor.ask(Message.Inc(1)) // ordering: runs after the self-sent one
          recorded <- trace.get
        yield assertTrue(recorded == List("follow:start", "follow:end", "inc", "inc"))
      }
    },
    test("pipeToSelf outlives the step that started it and comes back as a message") {
      ZIO.scoped {
        for
          seeds <- Ref.make(0)
          gate  <- Promise.make[Nothing, Unit]
          actor <- spawn(seeds)
          // The step ends immediately; the work completes only once the gate opens, well after the
          // submission's own scope has closed.
          _     <- actor.ask(Message.Later(gate.await.as(Message.Inc(7))))
          _     <- gate.succeed(())
          total <- actor.ask(Message.Inc(0)).repeatUntil(_ == 7)
        yield assertTrue(total == 7)
      }
    },
    test("a replacement over the same slot seeds afresh, because Done emptied it") {
      ZIO.scoped {
        for
          seeds <- Ref.make(0)
          slot  <- Bucket.inmemory[Int]
          first <- spawn(seeds, Some(slot))
          _     <- first.ask(Message.Inc(5))
          _     <- first.ask(Message.Stop)
          next  <- spawn(seeds, Some(slot))
          value <- next.ask(Message.Inc(1))
        yield assertTrue(value == 1)
      }
    },
    test("closing the scope releases whoever is waiting on the entity") {
      for
        seeds <- Ref.make(0)
        gate  <- Promise.make[Nothing, Unit]
        actor <- ZIO.scoped {
                   for
                     actor <- spawn(seeds)
                     _     <- actor.send(Message.Wait(gate))
                     _     <- actor.send(Message.Inc(1))
                   yield actor
                 }
        exit  <- actor.await.exit
      yield assertTrue(exit.isInterrupted)
    },
    test("a bounded mailbox admits only its bound before making a sender wait") {
      ZIO.scoped {
        for
          seeds  <- Ref.make(0)
          gate   <- Promise.make[Nothing, Unit]
          actor  <- spawn(seeds, mailbox = Some(1))
          _      <- actor.send(Message.Wait(gate)) // accepted, running — the mailbox is now full
          second <- actor.send(Message.Inc(1)).fork
          _      <- ZIO.yieldNow.repeatN(20)
          parked <- second.poll.map(_.isEmpty)
          _      <- gate.succeed(())               // the first settles, making room
          _      <- second.join
        yield assertTrue(parked)
      }
    },
  ) @@ TestAspect.withLiveClock @@ TestAspect.timeout(60.seconds)
