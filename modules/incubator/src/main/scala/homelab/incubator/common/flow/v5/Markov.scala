package homelab.incubator.common.flow.v5

import zio.*


/**
 * A state, and what follows it.
 *
 * The root carries only the two things a run needs: the state a question is asked about, and the step that
 * leaves the machine in the next one. Whether that step comes from a function held beside the state or from
 * the state itself is the choice between [[Markov.Transitional]] and [[Markov.Reflective]].
 *
 * @tparam R what a step needs
 * @tparam E what a step aborts with
 * @tparam S the state a question is asked about
 */
trait Markov[-R, +E, S]:

  /**
   * The state as a question sees it.
   *
   * @return it
   */
  def state: S

  /**
   * Advance by one step.
   *
   * @return the machine the step leaves behind; aborts with `E` when the step fails
   */
  def next: ZIO[R, E, Markov[R, E, S]]


object Markov:

  /**
   * A state advanced by a function beside it.
   *
   * The state is data and the step is held separately, so a family needs nothing of its own — an `Int`, a
   * config, a cursor. `transition` reads any state of the family, which is what makes it shareable.
   *
   * @tparam R what a step needs
   * @tparam E what a step aborts with
   * @tparam S the state
   */
  trait Transitional[-R, +E, S] extends Markov[R, E, S]:

    /**
     * How any state of this family becomes the next.
     *
     * @return the step, total over the family
     */
    def transition: S => ZIO[R, E, S]

    override def next: ZIO[R, E, Markov[R, E, S]] =
      transition(state).map(reached => Markov.make(reached)(transition))

  /**
   * A state that is its own step.
   *
   * Each state carries the transition out of itself, so the family is the set of classes and a reader finds
   * a step where the state it belongs to is defined. There is no function over the family, because no state
   * here can advance another.
   *
   * @tparam R what a step needs
   * @tparam E what a step aborts with
   * @tparam S the family, which each member is a member of
   */
  trait Reflective[-R, +E, S <: Reflective[R, E, S]] extends Markov[R, E, S]:
    self: S =>
    override def state: S = self
    override def next: ZIO[R, E, S]

  /**
   * Follow a machine until a question recognises the state it is in.
   *
   * A step runs interruptible and the space between two steps does not, so a step's own handlers are what
   * decide the fate of anything that step holds. The question is the caller's: the same machine stopped at
   * one answers with a grant, and stopped at another reports where it stands.
   *
   * @param machine where to start
   * @param terminal the states that end this run, and what each reports
   * @tparam R what a step needs
   * @tparam E what a step aborts with
   * @tparam S the state a question is asked about
   * @tparam A what this run reports
   * @return what `terminal` read from the state it stopped at; aborts with `E` when a step fails
   */
  def run[R, E, S, A](machine: Markov[R, E, S])(terminal: PartialFunction[S, A]): ZIO[R, E, A] =
    ZIO.uninterruptibleMask: restore =>
      terminal.lift(machine.state) match
        case Some(answer) => Exit.succeed(answer)
        case None         =>
          for
            reached <- restore(machine.next)
            answer  <- restore(run(reached)(terminal))
          yield answer

  /**
   * A machine from a state and a step over its family.
   *
   * @param initial where it starts
   * @param step how any state of the family becomes the next
   * @tparam R what the step needs
   * @tparam E what the step aborts with
   * @tparam S the state
   * @return the machine
   */
  def make[R, E, S](initial: S)(step: S => ZIO[R, E, S]): Markov[R, E, S] =
    new Transitional[R, E, S]:
      override def state: S                      = initial
      override def transition: S => ZIO[R, E, S] = step
