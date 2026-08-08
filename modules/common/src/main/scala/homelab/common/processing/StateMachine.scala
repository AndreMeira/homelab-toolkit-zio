package homelab.common.processing


import homelab.common.error.ApplicationError.AdapterError
import homelab.common.store.KeyValueStore
import zio.*


/**
 * A persistent, event-driven **state machine**: each [[execute]] applies one transition to the state
 * currently stored for an input key `I`, persists the new state, and returns an output `O`. Where a
 * [[homelab.common.flow.Workflow]] is autonomous — it drives itself to completion from a seed and yields
 * only an output — a `StateMachine` is externally driven: every call supplies a fresh input `I`, advances
 * the state by exactly one step, and leaves the new state in the store for the next call.
 *
 * `S` is the durable subject, not a resume cursor: it is the machine's identity, kept in [[store]] between
 * calls and never deleted (the machine has no "completion"). [[initial]] seeds it the first time an input
 * key is seen; every later call resumes from what was stored.
 *
 * The transition runs *before* the new state is persisted, so a store write that fails after [[logic]] ran
 * loses the new state while any effects `logic` performed have already happened — a retry then re-applies
 * `logic` to the *old* state. Keep `logic` idempotent where a persist failure must be recoverable.
 * Concurrent [[execute]] calls for the same input key are not serialised: they read the same state and
 * last-writer-wins on the store.
 *
 * @tparam R the environment each transition needs
 * @tparam E the error a transition may fail with
 * @tparam I the input driving a transition, which also keys the store
 * @tparam S the state advanced by each transition and persisted between calls
 * @tparam O the output produced by a transition
 */
trait StateMachine[-R, +E, -I, S, +O]:

  /**
   * The store holding each input key's current state between transitions.
   *
   * @return the key-value store the machine's state lives in
   */
  def store: KeyValueStore[I, S]

  /**
   * Seed the state for an input key seen for the first time — used only when [[store]] holds nothing under
   * it.
   *
   * @param input the input whose starting state to seed
   * @return the initial state; aborts with `E` on failure
   */
  def initial(input: I): ZIO[R, E, S]

  /**
   * The transition applied on each [[execute]]: maps the current `(input, state)` to a new state and an
   * output.
   *
   * @return the machine's transition logic
   */
  def logic: StateMachine.Logic[R, E, I, S, O]

  /**
   * Apply one transition for `input`: load its stored state (seeding via [[initial]] on first sight), run
   * [[logic]] to obtain the next state and an output, persist the new state, and return the output.
   *
   * @param input the input driving this transition, also the store key
   * @return the transition's output; aborts with `E` if seeding or the transition fails, or with
   *         `AdapterError` if the store read or write fails
   */
  def execute(input: I): ZIO[R, E | AdapterError, O] =
    for
      found          <- store.get(input)
      state          <- found.fold(initial(input))(ZIO.succeed)
      result         <- logic.next(input, state)
      (newState, out) = result
      _              <- store.set(input, newState)
    yield out


object StateMachine {

  /**
   * The transition function of a [[StateMachine]], separated from the machine so it can be defined and
   * tested on its own: given an input and the current state, produce the next state and an output.
   *
   * @tparam R the environment the transition needs
   * @tparam E the error the transition may fail with
   * @tparam I the input driving the transition
   * @tparam S the state read and advanced by the transition
   * @tparam O the output produced by the transition
   */
  trait Logic[-R, +E, -I, S, +O]:

    /**
     * The transition: map the current `(input, state)` to the next state and an output.
     *
     * @return a function from `(input, state)` to `(nextState, output)`; its effect aborts with `E` on
     *         failure
     */
    def next: (I, S) => ZIO[R, E, (S, O)]
}
