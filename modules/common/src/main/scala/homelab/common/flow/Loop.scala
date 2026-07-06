package homelab.common.flow

import zio.*


/**
 * A stack-safe, effectful loop driven by an explicit state machine. Starting from `state`, it runs
 * `run` to get the next [[Loop.Next]] step and either continues with a new state or stops with a
 * result — so termination can produce a different type (`O`) than the state it loops on (`S`).
 *
 * Stack-safe for any number of iterations: the recursion threads through ZIO's `flatMap`, which the
 * runtime trampolines, so it never grows the JVM stack. (It's the `tailRecM` pattern, with a readable
 * [[Loop.Next]] in place of `Either[S, O]`.)
 */
object Loop:

  /**
   * Run the loop from `state` until `run` returns [[Next.Done]].
   *
   * @tparam R the step effect's environment
   * @tparam E the step effect's error
   * @tparam S the state threaded through each iteration
   * @tparam O the value produced when the loop finishes
   * @param state the initial state
   * @param run   the step: given the current state, continue (with a new state) or finish (with a result)
   * @return the `O` from the terminating [[Next.Done]]; fails with `E` if any step fails
   */
  def apply[R, E, S, O](state: S)(run: S => ZIO[R, E, Next[S, O]]): ZIO[R, E, O] =
    run(state).flatMap:
      case Next.Continue(nextState) => Loop(nextState)(run)
      case Next.Done(value)         => ZIO.succeed(value)


  /**
   * Smart constructor for [[Next.Continue]]: leaves `O` as `Nothing` so covariance widens the result to
   * the loop's `O` at the call site — callers write `Loop.continue(state)` without naming `O`.
   */
  def continue[S](state: S): Next[S, Nothing] = Next.Continue(state)

  /**
   * Smart constructor for [[Next.Done]]: leaves `S` as `Nothing` so covariance widens the result to the
   * loop's `S` at the call site — callers write `Loop.done(outcome)` without naming `S`.
   */
  def done[O](outcome: O): Next[Nothing, O] = Next.Done(outcome)

  /**
   * [[done]] lifted into an effect — `ZIO.succeed(Loop.done(outcome))` — for the common terminal branch
   * of a step, so callers write `Loop.succeed(outcome)` directly. Like [[done]], it leaves `S` as
   * `Nothing` so covariance fits it into any step's result type.
   */
  def succeed[O](outcome: O): UIO[Next[Nothing, O]] = ZIO.succeed(Next.Done(outcome))


  /**
   * The outcome of one loop step: [[Continue]] to iterate again with a new state,
   * or [[Done]] to stop and yield the result. A readable stand-in for `Either[S, O]`.
   *
   * @tparam S the loop state carried by [[Continue]]
   * @tparam O the result carried by [[Done]]
   */
  enum Next[+S, +O]:
    /** Iterate again with `state`. */
    case Continue(state: S)

    /** Stop the loop, yielding `value`. */
    case Done(value: O)
