package homelab.incubator.processing.actor.v6


import homelab.incubator.processing.actor.v5.Actor.{ Next, Terminated }
import homelab.incubator.processing.actor.v5.{ Actor, Distributed }
import zio.*


class Actor {}


object Actor {

  case object Terminated
  type Terminated = Terminated.type

  trait Running[+E, -I, +O]:
    def ask(message: I): IO[E | Terminated, O]
    def send(message: I): IO[E | Terminated, Unit]
    def expect[E2 >: E, A](fn: Promise[E2, A] => I): IO[E2 | Terminated, A]

  trait Self[-I]:
    def send(input: I): UIO[Unit]
    def pipeToSelf(message: UIO[I]): UIO[Unit]
    def scoped[R, E, A](effect: ZIO[R & Scope, E, A]): ZIO[R, E, A]

}
