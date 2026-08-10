package homelab.incubator.processing.v2


import homelab.common.flow.Loop
import zio.*


object Actor {

  /** The send-to-self capability handed to a [[Logic]] through its environment. */
  trait Self[+E, -I]:
    /** Enqueue `input` onto this entity's own mailbox (fire-and-forget). */
    def send(input: I): IO[E, Unit]

  trait Logic[-R, E, I, S, +O]:
    def next(input: I, state: S): ZIO[R & Self[E, I], E, Loop.Next[(S, O), O]]
}
