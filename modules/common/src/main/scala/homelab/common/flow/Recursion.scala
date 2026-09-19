package homelab.common.flow

import zio.*


/**
 * A computation that has got somewhere, and knows what remains.
 *
 * The root carries the two things a run needs: the state a question is asked about, and the step that
 * leaves the rest of the work behind. A state here is the remainder rather than a record of progress — a
 * recursive call written as a value, so the whole of it is one of these. Whether the step comes from a
 * function beside the state or from the state itself is the choice between [[Recursion.Driven]] and
 * [[Recursion.Reflective]].
 *
 * @tparam R what a step needs
 * @tparam E what a step aborts with
 * @tparam S the state a question is asked about
 */
trait Recursion[-R, +E, S]:

  /**
   * The state as a question sees it.
   *
   * @return it
   */
  def state: S

  /**
   * Advance by one step.
   *
   * @return the recursion the step leaves behind; aborts with `E` when the step fails
   */
  def next: ZIO[R, E, Recursion[R, E, S]]


object Recursion:

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
  trait Driven[-R, +E, S] extends Recursion[R, E, S]:

    /**
     * How any state of this family becomes the next.
     *
     * @return the step, total over the family
     */
    def transition: S => ZIO[R, E, S]

    override def next: ZIO[R, E, Recursion[R, E, S]] =
      transition(state).map(reached => Recursion.make(reached)(transition))

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
  trait Reflective[-R, +E, S <: Reflective[R, E, S]] extends Recursion[R, E, S]:
    self: S =>
    override def state: S = self
    override def next: ZIO[R, E, S]

  /**
   * Follow the remainder until a question recognises the state it has reached.
   *
   * A step runs interruptible and the space between two steps does not, so a step's own handlers are what
   * decide the fate of anything that step holds. What one step takes for another to release is outside
   * that: an interrupt pending when the next step is reached ends the run before that step, and its
   * handlers with it. A resource that crosses a step boundary needs a deadline of its own.
   *
   * The question is the caller's: the same states stopped at one answer with a grant, and stopped at
   * another report where they stand.
   *
   * @param from where to start
   * @param terminal the states that end this run, and what each reports
   * @tparam R what a step needs
   * @tparam E what a step aborts with
   * @tparam S the state a question is asked about
   * @tparam A what this run reports
   * @return what `terminal` read from the state it stopped at; aborts with `E` when a step fails
   */
  def run[R, E, S, A](from: Recursion[R, E, S])(terminal: PartialFunction[S, A]): ZIO[R, E, A] =
    ZIO.uninterruptibleMask: restore =>
      terminal.lift(from.state) match
        case Some(answer) => ZIO.succeed(answer)
        case None         =>
          for
            reached <- restore(from.next)
            answer  <- restore(run(reached)(terminal))
          yield answer

  /**
   * A recursion from a state and a step over its family.
   *
   * @param initial where it starts
   * @param step how any state of the family becomes the next
   * @tparam R what the step needs
   * @tparam E what the step aborts with
   * @tparam S the state
   * @return the recursion
   */
  def make[R, E, S](initial: S)(step: S => ZIO[R, E, S]): Recursion[R, E, S] =
    new Driven[R, E, S]:
      override def state: S                      = initial
      override def transition: S => ZIO[R, E, S] = step
