package homelab.incubator.processing.actor.v3


import homelab.common.error.ApplicationError.AdapterError
import homelab.common.messaging.Partitioner
import homelab.common.store.KeyValueStore
import zio.*


// Scratchpad for experimenting with the v3 Stateful. Not wired into any build target beyond compilation.
object ActorScratch:

  given Partitioner.Key[String] with
    type Type = String
    def get(value: String): String = value

  // A throwaway in-memory KeyValueStore over a Ref (inmemory module isn't on llm's classpath).
  def store[K, V]: UIO[KeyValueStore[K, V]] =
    Ref
      .make(Map.empty[K, V])
      .map: ref =>
        new KeyValueStore[K, V]:
          def get(key: K): IO[AdapterError, Option[V]]      = ref.get.map(_.get(key))
          def set(key: K, value: V): IO[AdapterError, Unit] = ref.update(_.updated(key, value))
          def delete(key: K): IO[AdapterError, Boolean]     = ref.modify(m => (m.contains(key), m - key))

  // Behaviour: a per-key counter that replies the running total, passivating once it hits `limit`.
  final class Counting(limit: Int) extends Actor.Logic[Nothing, String, Int, Int]:
    def initial(input: String): Int                                                          = 0
    def next(self: Actor.Self[String])(input: String, state: Int): UIO[Actor.Step[Int, Int]] =
      val n = state + 1
      ZIO.succeed(if n >= limit then Actor.Step.Passivate(n) else Actor.Step.Continue(n, n))

  // pipeToSelf demo: kick off background work; its outcome comes back as a message, handled in-context.
  final class Piping(fetch: String => IO[String, Int]) extends Actor.Logic[Nothing, String, Int, Int]:
    def initial(input: String): Int                                                          = 0
    def next(self: Actor.Self[String])(input: String, state: Int): UIO[Actor.Step[Int, Int]] =
      if input.startsWith("result:") then ZIO.succeed(Actor.Step.Continue(input.drop(7).toInt, state))
      else self.pipeToSelf(fetch(input).fold(_ => "result:-1", n => s"result:$n")).as(Actor.Step.Continue(state, state))

  // Single entity — K = Unit, Fault = Nothing → spawn error is just AdapterError.
  val single: ZIO[Scope, AdapterError, Int] =
    for
      kv      <- store[Unit, Int]
      mailbox <- Actor(Actor.Runtime.local[String], kv, Counting(3)).spawn
      a       <- mailbox.ask("hi")
    yield a

  // Keyed pool — K = String; setup and interactions both abort with AdapterError.
  val pooled: ZIO[Scope, AdapterError, Int] =
    for
      kv      <- store[String, Int]
      mailbox <- Actor(Actor.Runtime.pool[String](parallelism = 4), kv, Counting(3)).spawn
      _       <- mailbox.tell("a")
      b       <- mailbox.ask("a")
    yield b
