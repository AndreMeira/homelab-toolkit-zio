package homelab.incubator.processing.v3

import homelab.common.error.ApplicationError.{ AdapterError, DecodingError }
import homelab.common.flow.Loop.Next.*
import homelab.common.flow.{ KeyLock, Loop }
import homelab.common.store.KeyValueStore
import zio.*


trait Workflow[-R, +E, S, +O]:

  def name: String

  /**
   * The transition: given the current state, continue with a new state or finish with the output.
   * Implemented as a case-lambda over `S`.
   *
   * @return the transition function; its effect continues with a new `S` or finishes with an `O`, and
   *         fails with `E` if the step fails
   */
  def next: S => ZIO[R, E, Loop.Next[S, O]]

  /**
   * Run the stepper in memory from `initial` until [[next]] finishes, without persistence. Stack-safe (it
   * delegates to [[Loop]]); the durable, resumable runner is [[Workflow.Runner.Default]].
   *
   * @param initial the starting state
   * @return the output produced when the stepper finishes; fails with `E` if any step fails
   */
  def run(initial: S): ZIO[R, E, O] = Loop(initial)(next)


object Workflow:
  trait Id[A]:
    def encode(value: A): String

  object Id:
    def apply[A](using id: Id[A]): Id[A] = id

  trait Serde[A]:
    def encode(value: A): String
    def decode(encoded: String): Either[String, A]

  object Serde:
    def apply[A](using serde: Serde[A]): Serde[A]          = serde
    def error(cause: String): DecodingError & AdapterError =
      new DecodingError with AdapterError:
        override def message: String = cause

  /**
   * How a [[Workflow]] is run. The behaviour is fixed; the runner decides whether the run is persisted and
   * serialised — `Err` is what that costs the error channel ([[Runner.Inline]] adds `Nothing`,
   * [[Runner.Default]] adds `AdapterError`). Both share one signature so callers can swap them freely.
   */
  trait Runner[Err]:
    def run[R, E, I: Id, S: Serde, O](
      workflow: Workflow[R, E, S, O],
      input: I,
    )(
      init: I => S
    ): ZIO[Scope & R, E | Err, O]

  object Runner:

    /** In-memory: step to completion with no persistence or locking. `Id`/`Serde` are ignored here. */
    trait Inline extends Runner[Nothing]:
      override def run[R, E, I: Id, S: Serde, O](
        workflow: Workflow[R, E, S, O],
        input: I,
      )(
        init: I => S
      ): ZIO[Scope & R, E, O] =
        workflow.run(init(input))

    /**
     * Durable and concurrency-safe: state is checkpointed to [[store]] under `(workflow.name, encoded input)`
     * after each step and deleted on completion, and [[lock]] serialises concurrent runs of the same key.
     * The lock is held for the whole run; the step loop threads state in memory (no re-read, no re-lock).
     */
    trait Default extends Runner[AdapterError]:
      def store: KeyValueStore[(String, String), String]
      def lock: KeyLock[(String, String)]

      override def run[R, E, I: Id, S: Serde, O](
        workflow: Workflow[R, E, S, O],
        input: I,
      )(
        init: I => S
      ): ZIO[Scope & R, E | AdapterError, O] =
        val key = workflow.name -> Id[I].encode(input)
        lock.withPermit(key):
          for
            found <- store.get(key)
            state <- found match
                       case None          => ZIO.succeed(init(input))
                       case Some(encoded) => ZIO.fromEither(Serde[S].decode(encoded).left.map(Serde.error))
            out   <- loop(workflow, key, state)
          yield out

      /**
       * Next `workflow` to completion from `state` under the already-held lock: checkpoint each new state to
       * [[store]] and continue, or delete the checkpoint and return the output on the terminal step. Threads
       * state in memory (no re-read) and takes no lock — the caller holds it. Stack-safe via ZIO's trampoline.
       *
       * @param workflow the stepper to advance
       * @param key      the store key under which each new state is checkpointed
       * @param state    the state to step from
       * @tparam R the environment each step needs
       * @tparam E the error a step may fail with
       * @tparam S the state advanced from step to step
       * @tparam O the output produced when the stepper finishes
       * @return the workflow output; fails with `E` if a step fails, or `AdapterError` if the store fails
       */
      private def loop[R, E, S: Serde, O](
        workflow: Workflow[R, E, S, O],
        key: (String, String),
        state: S,
      ): ZIO[R, E | AdapterError, O] =
        workflow.next(state).flatMap {
          case Continue(nextState) => store.set(key, Serde[S].encode(nextState)) *> loop(workflow, key, nextState)
          case Done(outcome)       => store.delete(key).as(outcome)
        }
