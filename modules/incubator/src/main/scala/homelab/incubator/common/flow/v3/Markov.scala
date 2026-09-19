package homelab.incubator.common.flow.v3

import zio.*


trait Markov[R, E, S <: Markov[R, E, S]] {
  self: S =>
  def next: ZIO[R, E, S]
}


object Markov:
  trait Lambda[R, E, S] extends Markov[R, E, Lambda[R, E, S]]:
    def current: S

  def run[R, E, S <: Markov[R, E, S], A](state: S)(terminal: PartialFunction[S, A]): ZIO[R, E, A] =
    ZIO.uninterruptibleMask: restore =>
      terminal.lift(state) match
        case Some(a) => ZIO.succeed(a)
        case None    =>
          for
            state  <- restore(state.next)
            result <- restore(run(state)(terminal))
          yield result

  def transition[R, E, S](initial: S)(fn: S => ZIO[R, E, S]): Lambda[R, E, S] = new Lambda:
    override def current: S                       = initial
    override def next: ZIO[R, E, Lambda[R, E, S]] = fn(initial).map(transition(_)(fn))
