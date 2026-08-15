package homelab.incubator.processing.v7


import homelab.common.error.ApplicationError.AdapterError
import homelab.common.messaging.Partitioner
import homelab.incubator.processing.v7.Actor.{ Next, Self }
import zio.*
import zio.test.*


// Correctness spec for the v7 pool: an entity per key, exactly one however hard the key is hit, serialised
// per key but concurrent across keys, finished entities replaced without the caller noticing, and failures
// and backpressure confined to the key that caused them. Seed counts are the instrument throughout — an
// entity seeds once, so counting seeds per key counts entities per key.
object DistributedSpec extends ZIOSpecDefault:

  private val boom: AdapterError = new AdapterError:
    override def message: String = "boom"

  private enum Op:
    case Inc
    case Stop
    case Boom
    case Wait(gate: Promise[Nothing, Unit])

  private final case class Message(key: String, op: Op)

  private given Partitioner.Key[Message] with
    type Type = String
    def get(value: Message): String = value.key

  /** A counter per key, recording how often each key's entity was seeded. */
  private def counter(seeds: Ref[Map[String, Int]]): Actor[AdapterError, Message, Int, Int] =
    new Actor[AdapterError, Message, Int, Int]:
      def init(message: Message): IO[AdapterError, Int] =
        seeds.update(counts => counts.updated(message.key, counts.getOrElse(message.key, 0) + 1)).as(0)

      def next(self: Self[Message]): (input: Message, state: Int) => IO[AdapterError, Next[Int, Int]] =
        (message, total) =>
          message.op match
            case Op.Inc        => ZIO.succeed(Next.Continue(total + 1, total + 1))
            case Op.Stop       => ZIO.succeed(Next.Done(total))
            case Op.Boom       => ZIO.fail(boom)
            case Op.Wait(gate) => gate.await.as(Next.Continue(total, total))

  def spec: Spec[TestEnvironment & Scope, Any] = suite("Distributed")(
    test("keeps a separate entity per key") {
      ZIO.scoped {
        for
          seeds <- Ref.make(Map.empty[String, Int])
          pool  <- Distributed.make(counter(seeds))
          a1    <- pool.ask(Message("a", Op.Inc))
          a2    <- pool.ask(Message("a", Op.Inc))
          b1    <- pool.ask(Message("b", Op.Inc))
          count <- seeds.get
        yield assertTrue(a1 == 1, a2 == 2, b1 == 1, count == Map("a" -> 1, "b" -> 1))
      }
    },
    test("spawns exactly one entity for a key however many callers race for it") {
      // Two entities for one key would seed twice and lose half the increments — the registry's whole job.
      ZIO.scoped {
        for
          seeds <- Ref.make(Map.empty[String, Int])
          pool  <- Distributed.make(counter(seeds))
          _     <- ZIO.foreachParDiscard(1 to 30)(_ => pool.ask(Message("hot", Op.Inc)))
          total <- pool.ask(Message("hot", Op.Inc))
          count <- seeds.get
        yield assertTrue(count("hot") == 1, total == 31)
      }
    },
    test("serialises one key while letting another run beside it") {
      ZIO.scoped {
        for
          seeds    <- Ref.make(Map.empty[String, Int])
          inFlight <- Ref.make(0)
          peak     <- Ref.make(0)
          both     <- Promise.make[Nothing, Unit]
          arrived  <- Ref.make(0)
          behaviour = new Actor[AdapterError, Message, Int, Int]:
                        def init(message: Message): IO[AdapterError, Int] = ZIO.succeed(0)
                        def next(self: Self[Message]) =
                          (message, total) =>
                            message.op match
                              // Completes only if both keys are inside a step at the same moment.
                              case Op.Wait(_) =>
                                arrived.updateAndGet(_ + 1).flatMap(n => both.succeed(()).when(n == 2)) *>
                                  both.await.as(Next.Continue(total, total))
                              case _          =>
                                inFlight.updateAndGet(_ + 1).flatMap(now => peak.update(_ max now)) *>
                                  ZIO.yieldNow.repeatN(3) *>
                                  inFlight.update(_ - 1).as(Next.Continue(total + 1, total + 1))
          pool     <- Distributed.make(behaviour)
          gate     <- Promise.make[Nothing, Unit]
          _        <- pool.ask(Message("x", Op.Wait(gate))) <&> pool.ask(Message("y", Op.Wait(gate)))
          _        <- ZIO.foreachParDiscard(1 to 20)(_ => pool.ask(Message("one", Op.Inc)))
          observed <- peak.get
        yield assertTrue(observed == 1) // reaching here at all proves x and y overlapped
      }
    },
    test("replaces a finished entity without the caller noticing") {
      ZIO.scoped {
        for
          seeds <- Ref.make(Map.empty[String, Int])
          pool  <- Distributed.make(counter(seeds))
          _     <- pool.ask(Message("a", Op.Inc))
          ended <- pool.ask(Message("a", Op.Stop))
          again <- pool.ask(Message("a", Op.Inc)) // refused by the dead entity, retried against a new one
          count <- seeds.get
        yield assertTrue(ended == 1, again == 1, count("a") == 2)
      }
    },
    test("a failing step affects only its own key") {
      ZIO.scoped {
        for
          seeds  <- Ref.make(Map.empty[String, Int])
          pool   <- Distributed.make(counter(seeds))
          failed <- pool.ask(Message("a", Op.Boom)).exit
          other  <- pool.ask(Message("b", Op.Inc))
          same   <- pool.ask(Message("a", Op.Inc)) // the entity survived its own failure
        yield assertTrue(failed == Exit.fail(boom), other == 1, same == 1)
      }
    },
    test("a saturated key holds up its own senders and nobody else's") {
      ZIO.scoped {
        for
          seeds  <- Ref.make(Map.empty[String, Int])
          pool   <- Distributed.make(counter(seeds), mailbox = Some(1))
          gate   <- Promise.make[Nothing, Unit]
          _      <- pool.send(Message("slow", Op.Wait(gate))) // fills the slow entity's mailbox
          stuck  <- pool.send(Message("slow", Op.Inc)).fork
          _      <- ZIO.yieldNow.repeatN(20)
          parked <- stuck.poll.map(_.isEmpty)
          fast   <- pool.ask(Message("fast", Op.Inc)) // a different key, unaffected
          _      <- gate.succeed(())
          _      <- stuck.join
        yield assertTrue(parked, fast == 1)
      }
    },
    test("closing the pool releases callers queued behind a busy entity") {
      for
        seeds  <- Ref.make(Map.empty[String, Int])
        gate   <- Promise.make[Nothing, Unit]
        queued <- ZIO.scoped {
                    for
                      pool <- Distributed.make(counter(seeds))
                      _    <- pool.send(Message("a", Op.Wait(gate)))
                      held <- pool.ask(Message("a", Op.Inc)).exit.fork
                      _    <- ZIO.yieldNow.repeatN(10) // let it queue behind the gated step
                    yield held
                  }
        exit   <- queued.join
      yield assertTrue(exit.isInterrupted)
    },
  ) @@ TestAspect.withLiveClock @@ TestAspect.timeout(60.seconds)
