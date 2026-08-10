package homelab.inmemory.processing


import homelab.common.error.ApplicationError.AdapterError
import homelab.common.messaging.Partitioner
import homelab.common.processing.ActorWorker
import homelab.common.store.KeyValueStore
import homelab.inmemory.store.InMemoryKeyValueStore
import zio.*
import zio.test.*


// End-to-end spec for ActorWorker over the in-memory store. Single actor (Worker.PerItem) and keyed pool
// (Worker.Parallel over a key-safe pipe): fire/ask with out-of-band replies, per-key serialization,
// pipeToSelf, passivation, and failure delivery. Live clock — the concurrency tests use real fibers.
object ActorWorkerSpec extends ZIOSpecDefault:

  private given Partitioner.Key[String] with
    type Type = String
    def get(value: String): String = value

  // Increments the stored count on every message, replying the new count; never passivates.
  private val counter: ActorWorker.Logic[Nothing, String, Int, Int] =
    new ActorWorker.Logic[Nothing, String, Int, Int]:
      def initial(input: String): UIO[Int] = ZIO.succeed(0)
      def next(self: ActorWorker.Self[String]): (String, Int) => UIO[ActorWorker.Step[Int, Int]] =
        (_, state) => ZIO.succeed(ActorWorker.Step.Continue(state + 1, state + 1))

  // Fails on the input "bad"; otherwise increments like `counter`.
  private val faulty: ActorWorker.Logic[String, String, Int, Int] =
    new ActorWorker.Logic[String, String, Int, Int]:
      def initial(input: String): IO[String, Int] = ZIO.succeed(0)
      def next(self: ActorWorker.Self[String]): (String, Int) => IO[String, ActorWorker.Step[Int, Int]] =
        (input, state) =>
          if input == "bad" then ZIO.fail("boom")
          else ZIO.succeed(ActorWorker.Step.Continue(state + 1, state + 1))

  // Passivates (deleting state) on "stop", replying the count reached; otherwise increments.
  private val stoppable: ActorWorker.Logic[Nothing, String, Int, Int] =
    new ActorWorker.Logic[Nothing, String, Int, Int]:
      def initial(input: String): UIO[Int] = ZIO.succeed(0)
      def next(self: ActorWorker.Self[String]): (String, Int) => UIO[ActorWorker.Step[Int, Int]] =
        (input, state) =>
          if input == "stop" then ZIO.succeed(ActorWorker.Step.Passivate(state))
          else ZIO.succeed(ActorWorker.Step.Continue(state + 1, state + 1))

  // On "start", pipes a background "bump" back to self; each "bump" increments — proving in-context delivery.
  private val piping: ActorWorker.Logic[Nothing, String, Int, Int] =
    new ActorWorker.Logic[Nothing, String, Int, Int]:
      def initial(input: String): UIO[Int] = ZIO.succeed(0)
      def next(self: ActorWorker.Self[String]): (String, Int) => UIO[ActorWorker.Step[Int, Int]] =
        (input, state) =>
          if input == "start" then self.pipeToSelf(ZIO.succeed("bump")).as(ActorWorker.Step.Continue(state, state))
          else ZIO.succeed(ActorWorker.Step.Continue(state + 1, state + 1))

  // Poll the store for a key until it holds `target` (or the suite timeout fires).
  private def awaitCount[K](store: KeyValueStore[K, Int], key: K, target: Int): IO[AdapterError, Unit] =
    (store.get(key).map(_.getOrElse(0)) <* ZIO.sleep(1.milli)).repeatUntil(_ == target).unit

  def spec = suite("ActorWorker")(
    test("single: fire then ask threads and persists state") {
      ZIO.scoped {
        for
          store  <- InMemoryKeyValueStore.make[Unit, Int]
          worker <- ActorWorker.make(store, counter)
          _      <- worker.run.forkScoped
          _      <- worker.fire("inc")     // state 1, reply discarded
          n      <- worker.ask("inc")      // state 2, reply 2
          saved  <- store.get(())
        yield assertTrue(n == 2, saved == Some(2))
      }
    },
    test("single: a failing step fails that ask, and the drain keeps going") {
      ZIO.scoped {
        for
          store  <- InMemoryKeyValueStore.make[Unit, Int]
          worker <- ActorWorker.make(store, faulty)
          _      <- worker.run.forkScoped
          a      <- worker.ask("inc")       // 1
          bad    <- worker.ask("bad").exit  // fails, state untouched
          c      <- worker.ask("inc")       // loop alive → 2
        yield assertTrue(a == 1, bad.isFailure, c == 2)
      }
    },
    test("single: Passivate deletes state, and the next message re-seeds") {
      ZIO.scoped {
        for
          store   <- InMemoryKeyValueStore.make[Unit, Int]
          worker  <- ActorWorker.make(store, stoppable)
          _       <- worker.run.forkScoped
          _       <- worker.fire("inc")
          _       <- worker.fire("inc")
          reached <- worker.ask("stop")     // Passivate replies the count reached (2), deletes state
          evicted <- store.get(())
          reseed  <- worker.ask("inc")       // fresh entity from initial → 1
        yield assertTrue(reached == 2, evicted.isEmpty, reseed == 1)
      }
    },
    test("single: pipeToSelf delivers its result back as a message, in-context") {
      ZIO.scoped {
        for
          store  <- InMemoryKeyValueStore.make[Unit, Int]
          worker <- ActorWorker.make(store, piping)
          _      <- worker.run.forkScoped
          _      <- worker.fire("start")     // pipes "bump" → increments to 1
          _      <- awaitCount(store, (), 1)
          saved  <- store.get(())
        yield assertTrue(saved == Some(1))
      }
    },
    test("pool: concurrent messages on one key never lose an update") {
      ZIO.scoped {
        for
          store <- InMemoryKeyValueStore.make[String, Int]
          pool  <- ActorWorker.Pool.make(store, counter, parallelism = 8)
          _     <- pool.run.forkScoped
          _     <- ZIO.foreachParDiscard(1 to 100)(_ => pool.fire("a")) // key-safety: no interleave on "a"
          _     <- awaitCount(store, "a", 100)                          // reaches exactly 100, or times out
          n     <- store.get("a")
        yield assertTrue(n == Some(100))
      }
    },
    test("pool: distinct keys keep independent state") {
      ZIO.scoped {
        for
          store <- InMemoryKeyValueStore.make[String, Int]
          pool  <- ActorWorker.Pool.make(store, counter, parallelism = 4)
          _     <- pool.run.forkScoped
          _     <- pool.fire("a")
          _     <- pool.fire("a")
          a     <- pool.ask("a")    // 2 fires + ask → 3
          b     <- pool.ask("b")    // fresh key → 1
        yield assertTrue(a == 3, b == 1)
      }
    },
    test("pool: withParallelism copy drains correctly") {
      ZIO.scoped {
        for
          store <- InMemoryKeyValueStore.make[String, Int]
          pool  <- ActorWorker.Pool.make(store, counter)
          _     <- pool.withParallelism(4).run.forkScoped
          a     <- pool.ask("a")
        yield assertTrue(a == 1)
      }
    },
  ) @@ TestAspect.withLiveClock @@ TestAspect.timeout(60.seconds)
