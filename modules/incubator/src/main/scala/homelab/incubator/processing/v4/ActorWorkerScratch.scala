package homelab.incubator.processing.v4

import homelab.common.error.ApplicationError.AdapterError
import homelab.common.messaging.Partitioner
import homelab.common.processing.ActorWorker
import homelab.common.store.KeyValueStore
import zio.*

// Scratchpad for the ActorWorker (Worker.PerItem single + KeyedQueue pool), out-of-band replies.
object ActorWorkerScratch:

  given Partitioner.Key[String] with
    type Type = String
    def get(value: String): String = value

  def store[K, V]: UIO[KeyValueStore[K, V]] =
    Ref.make(Map.empty[K, V]).map: ref =>
      new KeyValueStore[K, V]:
        def get(key: K): IO[AdapterError, Option[V]]      = ref.get.map(_.get(key))
        def set(key: K, value: V): IO[AdapterError, Unit] = ref.update(_.updated(key, value))
        def delete(key: K): IO[AdapterError, Boolean]     = ref.modify(m => (m.contains(key), m - key))

  // Out-of-band: every step returns an O — `fire` discards it, `ask` receives it.
  val counting: ActorWorker.Logic[Nothing, String, Int, Int] =
    new ActorWorker.Logic[Nothing, String, Int, Int]:
      def initial(input: String): UIO[Int] = ZIO.succeed(0)
      def next(self: ActorWorker.Self[String]): (input: String, state: Int) => UIO[ActorWorker.Step[Int, Int]] =
        (input, state) =>
          val n = state + 1
          ZIO.succeed(ActorWorker.Step.Continue(n, n))

  // Single actor: fork run, then fire/ask.
  val single: ZIO[Scope, AdapterError, Int] =
    for
      kv     <- store[Unit, Int]
      worker <- ActorWorker.make(kv, counting)
      _      <- worker.run.forkScoped
      _      <- worker.fire("inc")
      n      <- worker.ask("inc")
    yield n // 2

  // Keyed pool: one entity per key; a failed step would surface through `ask`, not hang.
  val pooled: ZIO[Scope, AdapterError, Int] =
    for
      kv   <- store[String, Int]
      pool <- ActorWorker.Pool.make(kv, counting, parallelism = 4)
      _    <- pool.run.forkScoped
      _    <- pool.fire("a")
      n    <- pool.ask("a")
    yield n // 2
