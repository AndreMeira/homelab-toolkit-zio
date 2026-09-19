package homelab.incubator.common.flow.v4

import zio.*


trait Markov[R, E, S]:
  self =>
  def state: S
  def transition: S => ZIO[R, E, S]

  def next: ZIO[R, E, Markov[R, E, S]] =
    transition(state).map(s => Markov.transition(s)(transition))


object Markov {
  trait Reflective[R, E, S <: Reflective[R, E, S]] extends Markov[R, E, S]:
    self: S =>
    def nextState: ZIO[R, E, S]
    def current: S                             = self
    override def next: ZIO[R, E, S]            = nextState
    override def transition: S => ZIO[R, E, S] = _ => nextState

  def run[R, E, S, A](state: Markov[R, E, S])(terminal: PartialFunction[S, A]): ZIO[R, E, A] =
    ZIO.uninterruptibleMask: restore =>
      terminal.lift(state.state) match
        case Some(a) => ZIO.succeed(a)
        case None    =>
          for
            state  <- restore(state.next)
            result <- restore(run(state)(terminal))
          yield result

  def transition[R, E, S](initial: S)(nextState: S => ZIO[R, E, S]): Markov[R, E, S] = new Markov {
    override def state: S                      = initial
    override def transition: S => ZIO[R, E, S] = nextState
  }
}
