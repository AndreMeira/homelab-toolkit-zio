package homelab.incubator.processing.actor.v4


import homelab.common.error.ApplicationError.AdapterError
import homelab.common.store.KeyValueStore
import Actor.{ ask, tell }
import zio.*


// Scratchpad for the v4 (in-band reply) actor — compare with v3's ActorScratch.
object ActorV4Scratch:

  def store[K, V]: UIO[KeyValueStore[K, V]] =
    Ref
      .make(Map.empty[K, V])
      .map: ref =>
        new KeyValueStore[K, V]:
          def get(key: K): IO[AdapterError, Option[V]]      = ref.get.map(_.get(key))
          def set(key: K, value: V): IO[AdapterError, Unit] = ref.update(_.updated(key, value))
          def delete(key: K): IO[AdapterError, Boolean]     = ref.modify(m => (m.contains(key), m - key))

  // The message protocol carries its own reply channel — the in-band part. `Get` asks; `Increment` tells.
  enum Counter:
    case Increment
    case Get(replyTo: Promise[Nothing, Int])

  val counting: Actor.Logic[Nothing, Counter, Int] = new Actor.Logic[Nothing, Counter, Int] {
    def initial(input: Counter): Int = 0

    def next(self: Actor.Self[Counter])(input: Counter, state: Int): UIO[Actor.Step[Int]] =
      input match
        case Counter.Increment    => ZIO.succeed(Actor.Step.Continue(state + 1))
        case Counter.Get(replyTo) => replyTo.succeed(state).as(Actor.Step.Continue(state)) // reply in-band
  }

  // No `O`, no Mailbox — the handle is a Producer; tell/ask are extensions.
  val demo: ZIO[Scope, AdapterError, Int] =
    for
      kv      <- store[Unit, Int]
      mailbox <- Actor(Actor.Runtime.local[Counter], kv, counting).spawn
      _       <- mailbox.tell(Counter.Increment)
      _       <- mailbox.tell(Counter.Increment)
      n       <- mailbox.ask(Counter.Get(_)) // IO[AdapterError, Int]
    yield n // 2
