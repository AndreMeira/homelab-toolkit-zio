package homelab.common.flow

import zio.*

/**
 * A named, effectful **stepper** — repeatedly advances a state `S` via [[next]] until a step finishes,
 * yielding the terminal state and an output. Autonomous: each step is driven by the current state
 * alone, with no per-step input — hence "stepper" rather than a `(In, State) => (State, Out)` machine.
 *
 * Implement `next` as a case-lambda over the state:
 * {{{
 *   override def next =
 *     case Counting(n) if n < limit => tick.as(Loop.continue(Counting(n + 1)))
 *     case Counting(n)              => summary(n).map(o => Loop.done((Counting(n), o)))
 * }}}
 *
 * Resumable by replay: because `Continue` carries the whole state, an executor can persist `S` after
 * each step and resume by calling `next` on the stored state. That requires `S` to be **serializable**
 * and each step to be **idempotent** — resuming re-runs the step, so re-execution must be harmless.
 *
 * @tparam R the environment each step needs
 * @tparam E the error a step may fail with
 * @tparam S the state advanced from step to step
 * @tparam O the output produced when the stepper finishes
 */
trait Stepper[R, E, S, O] {

  /**
   * The transition: given the current state, continue with a new state or finish with the terminal
   * state and output. Implemented as a case-lambda over `S`.
   */
  def next: S => ZIO[R, E, Loop.Next[S, (S, O)]]

  /**
   * Run the stepper in memory from `initial` until [[next]] finishes. Stack-safe (it delegates to
   * [[Loop]]); a persistent, resumable runner would live in its own executor.
   *
   * @param initial the starting state
   * @return the terminal `(state, output)`; fails with `E` if any step fails
   */
  def execute(initial: S): ZIO[R, E, (S, O)] = Loop(initial)(next)
}
