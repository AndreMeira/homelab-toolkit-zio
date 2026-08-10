package homelab.incubator.processing.v1

import homelab.common.error.ApplicationError.AdapterError
import homelab.common.flow.{ KeyedQueue, Loop }
import homelab.common.messaging.{ Consumer, Partitioner }
import homelab.common.processing.Processor
import homelab.common.store.KeyValueStore
import zio.*


/**
 * SKETCH — an **addressable mailbox**: something you `ask` (request/reply) or `tell` (fire-and-forget). Both
 * a single [[Actor]] and a keyed [[Pool]] of actors present this one interface, so the caller never cares
 * whether there is one entity behind it or a whole shard region.
 *
 * @tparam E the error an interaction aborts with
 * @tparam I the message accepted
 * @tparam O the reply an `ask` produces
 */
trait Mailbox[+E, -I, +O]:

  /** Request/reply: enqueue `input` and await its transition's reply. */
  def ask(input: I): IO[E, O]

  /** Fire-and-forget: enqueue `input`; its reply and any error are dropped. */
  def tell(input: I): IO[E, Unit]


object Mailbox {

  /** A queued message: the input, plus a reply channel when it is an `ask` (absent for a `tell`). */
  private[processing] type Envelope[E, I, O] = (I, Option[Promise[E, O]])

  /** Deliver a transition's outcome to its reply channel — or drop it, for a `tell`. */
  private[processing] def reply[E, O](replyTo: Option[Promise[E, O]], result: Exit[E, O]): UIO[Unit] =
    ZIO.foreachDiscard(replyTo)(_.done(result))

  /** The `ask`/`tell` facade over an `offer` that enqueues one envelope onto the backing mailbox. */
  private[processing] def facade[E, I, O](offer: Envelope[E, I, O] => UIO[Any]): Mailbox[E, I, O] =
    new Mailbox[E, I, O]:
      def ask(input: I): IO[E, O] =
        for
          promise <- Promise.make[E, O]
          _       <- offer((input, Some(promise)))
          output  <- promise.await
        yield output

      def tell(input: I): IO[E, Unit] = offer((input, None)).unit
}


object Actor {

  /** The send-to-self capability handed to a [[Logic]] through its environment. */
  trait Self[+E, -I]:
    /** Enqueue `input` onto this entity's own mailbox (fire-and-forget). */
    def send(input: I): IO[E, Unit]

  /** One transition's outcome. Both branches reply an `O`; [[Stop]] also ends the entity's life. */
  enum Step[+S, +O]:
    /** Persist `state` and reply `reply`. */
    case Continue(state: S, reply: O)

    /** Reply `reply`, then delete the state and stop the entity — re-seeded on the next message, for a [[Pool]]. */
    case Stop(reply: O)

  /**
   * The actor **behaviour**: given a message and the current state, produce the next [[Step]]. It runs with a
   * [[Self]] in its environment (so a transition can message itself) and returns a [[Step]] (so it can
   * passivate). This is a [[homelab.common.processing.StateMachine.Logic]] enriched with self-reference and a
   * lifecycle — the plain state machine is exactly "always [[Step.Continue]], no [[Self]]".
   *
   * @tparam R the environment a transition needs, beyond [[Self]]
   * @tparam E the error a transition may fail with
   * @tparam I the message accepted
   * @tparam S the entity state
   * @tparam O the reply produced
   */
  trait Logic[-R, E, I, S, +O]:
    def next(input: I, state: S): ZIO[R & Self[E, I], E, Step[S, O]]

  /**
   * Run `logic` as a single **in-memory** actor seeded at `initial`. One background fiber drains the mailbox
   * and threads the state through [[Loop]]: each message applies the transition (with `self` in scope) and
   * either continues with the new state or, on [[Step.Stop]], ends the loop. A failed transition replies the
   * error and leaves the state unchanged. (A *durable* single actor is just a [[Pool]] with one key.)
   */
  def make[R, E: Tag, I: Tag, S, O](
    logic: Logic[R, E, I, S, O],
    initial: S,
  ): ZIO[Scope & R, Nothing, Mailbox[E, I, O]] =
    for
      env   <- ZIO.environment[R]
      queue <- Queue.unbounded[Mailbox.Envelope[E, I, O]]
      self   = new Self[E, I]:
                 def send(input: I): IO[E, Unit] = queue.offer((input, None)).unit
      _     <- Loop(initial) { state =>
                 queue.take.flatMap { (input, replyTo) =>
                   logic
                     .next(input, state)
                     .provideEnvironment(env.add[Self[E, I]](self))
                     .exit
                     .flatMap {
                       case Exit.Success(Step.Continue(next, out)) =>
                         Mailbox.reply(replyTo, Exit.succeed(out)).as(Loop.continue(next))
                       case Exit.Success(Step.Stop(out)) =>
                         Mailbox.reply(replyTo, Exit.succeed(out)).as(Loop.done(()))
                       case Exit.Failure(cause) =>
                         Mailbox.reply(replyTo, Exit.failCause(cause)).as(Loop.continue(state))
                     }
                 }
               }.forkScoped
    yield Mailbox.facade(queue.offer)
}


object Pool {

  /**
   * Run `logic` as a **pool of keyed actors** — one entity per partition key of `I` — over the durable
   * `store`. `parallelism` consumers drain a [[KeyedQueue]], so entities run concurrently across keys but
   * never overlap within one. A first message for a key seeds its state via `initial`; [[Actor.Step.Stop]]
   * *passivates* — deletes the key's state, to be re-seeded on its next message. Each entity's [[Actor.Self]]
   * targets its *own* key, so a self-message stays on the same entity whatever key it would otherwise extract
   * to.
   *
   * '''Precondition''': `store` is keyed by the same partition key as the mailbox — that is the whole point.
   */
  def make[R, E: Tag, I: Partitioner.Key as key, S, O](
    logic: Actor.Logic[R, E, I, S, O],
    initial: I => S,
    store: KeyValueStore[key.Type, S],
    parallelism: Int,
    maxBuffer: Option[Int] = None,
  )(using Tag[I]): ZIO[Scope & R, KeyedQueue.Error, Mailbox[E | AdapterError, I, O]] =
    for
      env   <- ZIO.environment[R]
      queue <- KeyedQueue.make[key.Type, Mailbox.Envelope[E | AdapterError, I, O]](maxBuffer)
      // A key-preserving consumer: `Consumer.fromKeyedQueue` drops the key, but the pool needs it to load
      // per-entity state and to build the entity's own `Self`.
      keyed  = new Consumer[Nothing, (key.Type, Mailbox.Envelope[E | AdapterError, I, O])]:
                 def consume[E2 >: Nothing](
                   run: ((key.Type, Mailbox.Envelope[E | AdapterError, I, O])) => IO[E2, Unit]
                 ): IO[E2, Unit] =
                   queue.takeWith((k, envelope) => run((k, envelope)))
      _     <- Processor.parallel(keyed, parallelism) {
                 case (k, (input, replyTo)) =>
                   val self = new Actor.Self[E, I]:
                     def send(message: I): IO[E, Unit] = queue.offer(k, (message, None)).unit
                   val process =
                     for
                       stored <- store.get(k)
                       state  <- ZIO.succeed(stored.getOrElse(initial(input)))
                       step   <- logic.next(input, state).provideEnvironment(env.add[Actor.Self[E, I]](self))
                       output <- step match
                                   case Actor.Step.Continue(s, o) => store.set(k, s).as(o) // persist
                                   case Actor.Step.Stop(o)        => store.delete(k).as(o) // passivate
                     yield output
                   process.exit.flatMap(exit => Mailbox.reply(replyTo, exit))
               }.forkScoped
    yield Mailbox.facade(envelope => queue.offer(key.get(envelope._1), envelope))
}
