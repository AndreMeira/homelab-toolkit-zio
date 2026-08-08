package homelab.common.flow


import homelab.common.error.ApplicationError
import homelab.common.error.ApplicationError.AdapterError
import homelab.common.store.KeyValueStore
import homelab.common.flow.Loop.Next.*
import zio.*


/**
 * A named, effectful **Workflow** (stepper)— repeatedly advances a state `S` via [[next]] until a step finishes,
 * yielding an output `O`. Autonomous: each step is driven by the current state alone, with no per-step
 * input — hence "stepper" rather than a `(In, State) => (State, Out)` machine.
 *
 * `S` is the resume cursor, not the result: it exists so each `Continue` carries enough to persist and
 * replay a step. The result is `O` alone. If a workflow's terminal state is worth returning, the author
 * folds it into `O` — the framework does not presume it (a state machine, where the terminal state *is*
 * the contract, would carry `S` in `Done`; a `Workflow` is a checkpointing execution flow that produces
 * an output).
 *
 * Implement `next` as a case-lambda over the state:
 * {{{
 *   override def next =
 *     case Counting(n) if n < limit => tick.as(Loop.continue(Counting(n + 1)))
 *     case Counting(n)              => summary(n).map(Loop.done)
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
trait Workflow[R, E, S, O] {
  self =>

  /**
   * The transition: given the current state, continue with a new state or finish with the output.
   * Implemented as a case-lambda over `S`.
   *
   * @return the transition function; its effect continues with a new `S` or finishes with an `O`, and
   *         fails with `E` if the step fails
   */
  def next: S => ZIO[R, E, Loop.Next[S, O]]

  /**
   * Run the stepper in memory from `initial` until [[next]] finishes. Stack-safe (it delegates to
   * [[Loop]]); a persistent, resumable runner would live in its own executor.
   *
   * @param initial the starting state
   * @return the output produced when the stepper finishes; fails with `E` if any step fails
   */
  def execute(initial: S): ZIO[R, E, O] = Loop(initial)(next)

  /**
   * Make this stepper **durable**: checkpoint the state to `persistence` before each step so an
   * interrupted run resumes from the last stored state instead of restarting. `init` seeds the first
   * state from the input `I`, which also serves as the store key. See [[Workflow.Durable]] for the
   * resume semantics.
   *
   * @param persistence the key-value store that checkpoints state under the input key
   * @param init        derives the starting state from the input; used only when the store holds nothing yet
   * @tparam I the run input, which doubles as the store key
   * @return a durable wrapper around this stepper
   */
  def durable[I](persistence: KeyValueStore[I, S])(init: I => S): Workflow.Durable[R, E, I, S, O] =
    new Workflow.Durable[R, E, I, S, O] {
      override def workflow: Workflow[R, E, S, O]  = self
      override def initial(input: I): ZIO[R, E, S] = ZIO.succeed(init(input))
      override def store: KeyValueStore[I, S]      = persistence
    }
}


object Workflow {

  /**
   * Build a [[Workflow]] from a bare transition function, without declaring a named subtype — for ad-hoc
   * or inline steppers where a full trait would be overkill.
   *
   * @param run the transition: given the current state, continue with a new `S` or finish with an `O`
   * @tparam R the environment each step needs
   * @tparam E the error a step may fail with
   * @tparam S the state advanced from step to step
   * @tparam O the output produced when the stepper finishes
   * @return a workflow whose [[Workflow.next]] is `run`
   */
  def make[R, E, S, O](run: S => ZIO[R, E, Loop.Next[S, O]]): Workflow[R, E, S, O] =
    new Workflow:
      override def next: S => ZIO[R, E, Loop.Next[S, O]] = run

  /**
   * A helper trait for common domain implementations:
   * the stepper needs no environment and fails only with [[ApplicationError]].
   *
   * @tparam S the state advanced from step to step
   * @tparam O the output produced when the stepper finishes
   */
  trait Simple[S, O] extends Workflow[Any, ApplicationError, S, O]

  /**
   * A [[Workflow]] made resumable by checkpointing. [[execute]] checkpoints each new state to [[store]] as
   * the workflow advances and deletes it once the workflow finishes, so a crashed or interrupted run
   * resumes from the last stored state rather than from the start. The input `I` keys the store;
   * [[initial]] seeds the first state when the store holds nothing for that key.
   *
   * Resume is by replay — a resumed run re-runs the step whose state it last persisted — so, as with
   * [[Workflow]], `S` must be **serializable** and each step **idempotent**. Like [[Workflow.execute]],
   * the run returns only the output `O`; the final checkpoint is deleted on completion.
   *
   * @tparam R the environment each step needs
   * @tparam E the error a step may fail with
   * @tparam I the run input that seeds the run and keys the store
   * @tparam S the state advanced from step to step
   * @tparam O the output produced when the stepper finishes
   */
  trait Durable[R, E, I, S, O]:
    /**
     * Seed the starting state for a fresh run — used only when [[store]] holds nothing under `input`.
     *
     * @param input the run input, also the store key
     * @return the initial state; fails with `E` if seeding fails
     */
    def initial(input: I): ZIO[R, E, S]

    /**
     * The underlying stepper this durable wrapper drives, one [[Workflow.next]] step at a time.
     *
     * @return the wrapped workflow
     */
    def workflow: Workflow[R, E, S, O]

    /**
     * The checkpoint store, keyed by the run input `I`, holding the latest state between steps.
     *
     * @return the key-value store used to persist and resume state
     */
    def store: KeyValueStore[I, S]

    /**
     * Run to completion, resuming from the checkpoint when one exists. Loads any state stored under
     * `input` (falling back to [[initial]] on a miss), then steps until the workflow finishes —
     * checkpointing each new state as it is produced and deleting the checkpoint on completion. Stack-safe
     * via [[Loop]].
     *
     * @param input the run input, also the store key
     * @return the workflow output; fails with `E` if a step fails, or `AdapterError` if the store fails
     */
    def execute(input: I): ZIO[R, E | AdapterError, O] =
      for
        found  <- store.get(input)
        state  <- found.fold(initial(input))(ZIO.succeed)
        result <- loop(input, state)
      yield result

    /**
     * Step from `state` until the workflow finishes: on each transition, checkpoint the produced next
     * state to [[store]] and continue, or delete the checkpoint and return the output on the terminal
     * step. The seed `state` is stepped as-is — only states it produces are checkpointed. Stack-safe via
     * [[Loop]].
     *
     * @param input the run input, also the store key under which each new state is checkpointed
     * @param state the state to step from — either [[initial]]'s seed or a resumed checkpoint
     * @return the workflow output; fails with `E` if a step fails, or `AdapterError` if the store fails
     */
    private def loop(input: I, state: S): ZIO[R, E | AdapterError, O] = Loop(state): current =>
      workflow.next(current).flatMap {
        case Continue(nextState) => store.set(input, nextState).as(Loop.continue(nextState))
        case Done(output)        => store.delete(input).as(Loop.done(output))
      }

}
