package homelab.common.processing

import homelab.common.error.ApplicationError.{ AdapterError, DecodingError }
import homelab.common.flow.KeyLock
import homelab.common.processing.Workflow.Step
import homelab.common.store.KeyValueStore
import zio.*


/**
 * A named stepper whose lifecycle is `Init → Continue* → Done`. Given a non-terminal [[Step.Pending]], its
 * [[next]] produces the following [[Step]] — seeding on [[Step.Init]] (effectfully), advancing on
 * [[Step.Continue]], and it may finish ([[Step.Done]]) or restart ([[Step.Init]]). Because the domain is the
 * pending union, `next` is never handed a finished workflow — no phantom `Done` case to match.
 *
 * The `Workflow` is only the *behaviour*; how it is run — in memory, or durably and concurrency-safe — is a
 * [[Workflow.Runner]].
 *
 * @tparam R the environment each step needs
 * @tparam E the error a step may fail with
 * @tparam I the input a run seeds from
 * @tparam S the state advanced from step to step
 * @tparam O the output produced when the stepper finishes
 */
trait Workflow[-R, +E, I, S, +O]:

  /** A stable identity, used to namespace this workflow's state in a shared store. */
  def name: String

  /**
   * The transition: given a non-terminal cursor, produce the next [[Step]] — seed on [[Step.Init]], advance
   * on [[Step.Continue]], or terminate ([[Step.Done]]) / restart ([[Step.Init]]).
   *
   * @return the transition function; fails with `E` if the step fails
   */
  def next: Step.Pending[I, S] => ZIO[R, E, Step[I, S, O]]

  /**
   * Run the stepper in memory from `input` until [[next]] finishes, without persistence. Stack-safe (ZIO's
   * `flatMap` trampolines); the durable, resumable runner is [[Workflow.Runner.Default]].
   *
   * @param input the input to seed the run from
   * @return the output produced when the stepper finishes; fails with `E` if any step fails
   */
  def run(input: I): ZIO[R, E, O] = loop(Step.Init(input))

  /**
   * Step from `cursor` until [[next]] returns [[Step.Done]], purely in memory.
   *
   * @param cursor the non-terminal cursor to step from
   * @return the output; fails with `E` if any step fails
   */
  private def loop(cursor: Step.Pending[I, S]): ZIO[R, E, O] =
    next(cursor).flatMap {
      case Step.Init(input)     => loop(Step.Init(input))
      case Step.Continue(state) => loop(Step.Continue(state))
      case Step.Done(output)    => ZIO.succeed(output)
    }


object Workflow:

  /**
   * A step of a [[Workflow]]'s lifecycle: seed from an input, continue with a new state, or finish with an
   * output. [[Init]] makes seeding a first-class, *effectful* step (via [[Workflow.next]]) and doubles as the
   * restart signal; [[Done]] is terminal.
   *
   * @tparam I the input a run (or restart) seeds from
   * @tparam S the state advanced from step to step
   * @tparam O the output produced when the workflow finishes
   */
  enum Step[+I, +S, +O]:
    /** Seed (or re-seed) the workflow from `input`. */
    case Init[+I](input: I) extends Step[I, Nothing, Nothing]

    /** Advance the workflow, carrying a new `state`. */
    case Continue[+S](state: S) extends Step[Nothing, S, Nothing]

    /** Finish the workflow, yielding `output`. */
    case Done[+O](output: O) extends Step[Nothing, Nothing, O]

  object Step:
    /** The non-terminal cursors — the only things [[Workflow.next]] can step *from*. */
    type Pending[+I, +S] = Init[I] | Continue[S]

    /**
     * Smart constructor for [[Continue]] widened to [[Pending]], so a call site produces a cursor without
     * naming `I`.
     *
     * @param state the state to continue with
     * @tparam S the state type
     * @return the continue cursor
     */
    def continue[S](state: S): Pending[Nothing, S] = Continue(state)

    /**
     * Smart constructor for [[Init]] widened to [[Pending]], so a call site produces a cursor without naming
     * `S`.
     *
     * @param input the input to seed from
     * @tparam I the input type
     * @return the init cursor
     */
    def init[I](input: I): Pending[I, Nothing] = Init(input)

  /**
   * Build a [[Workflow]] from a name and a transition, without declaring a subtype.
   *
   * @param name the workflow's identity — namespaces its persisted state
   * @param next the transition: given a non-terminal cursor, produce the next [[Step]]
   * @tparam R the environment each step needs
   * @tparam E the error a step may fail with
   * @tparam I the input a run seeds from
   * @tparam S the state advanced from step to step
   * @tparam O the output produced when the stepper finishes
   * @return a workflow named `name` whose [[Workflow.next]] is `next`
   */
  def make[R, E, I, S, O](
    name: String,
  )(
    next: Step.Pending[I, S] => ZIO[R, E, Step[I, S, O]]
  ): Workflow[R, E, I, S, O] =
    // Capture so the same-named overrides below don't resolve to themselves.
    val workflowName = name
    val transition   = next
    new Workflow[R, E, I, S, O]:
      def name: String                                        = workflowName
      def next: Step.Pending[I, S] => ZIO[R, E, Step[I, S, O]] = transition

  /**
   * A string codec for a workflow's input (the store key) and state (the store value), used by
   * [[Runner.Default]] to persist and resume. Its round-trip law — `decode(encode(a)) == Right(a)` — is what
   * a durable runner relies on: it makes `encode` injective (so distinct inputs never collide on one slot)
   * and lets a checkpoint be read back.
   *
   * @tparam A the value encoded
   */
  trait Serde[A]:

    /**
     * Encode `value` to its stored form.
     *
     * @param value the value to encode
     * @return the encoded string
     */
    def encode(value: A): String

    /**
     * Decode a stored string back to a value.
     *
     * @param encoded the stored string
     * @return the value, or a `Left` describing why it could not be decoded
     */
    def decode(encoded: String): Either[String, A]

  object Serde:

    /**
     * Summon the in-scope `Serde` instance for `A`.
     *
     * @tparam A the value type
     * @return the given `Serde[A]`
     */
    def apply[A](using serde: Serde[A]): Serde[A] = serde

    /**
     * The error a failed [[Serde.decode]] becomes at the store boundary. A corrupt checkpoint is an
     * implementation/infrastructure fault, not a domain outcome, so it is both a `DecodingError` and an
     * `AdapterError`.
     *
     * @param cause a description of the decode failure
     * @return the adapter error carrying `cause`
     */
    def error(cause: String): DecodingError & AdapterError =
      new DecodingError with AdapterError:
        override def message: String = cause

  /**
   * How a [[Workflow]] is run. The behaviour is fixed; the runner decides whether the run is persisted and
   * serialised — `Err` is what that costs the error channel ([[Runner.Inline]] adds `Nothing`,
   * [[Runner.Default]] adds `AdapterError`). Both share one signature so callers can swap them freely.
   *
   * @tparam Err the error the runner itself may add, beyond the workflow's own `E`
   */
  trait Runner[Err]:

    /**
     * Run `workflow` for `input` to completion.
     *
     * @param workflow the workflow to run
     * @param input    the input that seeds — and, for a durable runner, keys — the run
     * @tparam R the environment each step needs
     * @tparam E the error a step may fail with
     * @tparam I the input a run seeds from
     * @tparam S the state advanced from step to step
     * @tparam O the output produced when the stepper finishes
     * @return the workflow's output; fails with `E` if a step fails, or `Err` for the runner's own failures
     */
    def run[R, E, I: Serde, S: Serde, O](
      workflow: Workflow[R, E, I, S, O],
      input: I,
    ): ZIO[Scope & R, E | Err, O]

  object Runner:

    /**
     * Build a [[Default]] runner over `store`, provisioning its own per-key [[KeyLock]].
     *
     * @param store the key-value store the runner checkpoints workflow state to
     * @return a durable, concurrency-safe runner backed by `store`
     */
    def make(store: KeyValueStore[(String, String), String]): UIO[Default] =
      KeyLock.make[(String, String)].map: keyLock =>
        val backing = store
        new Default:
          def store: KeyValueStore[(String, String), String] = backing
          def lock: KeyLock[(String, String)]                = keyLock

    /** In-memory: step to completion with no persistence or locking. The `Serde`s are ignored here. */
    trait Inline extends Runner[Nothing]:
      override def run[R, E, I: Serde, S: Serde, O](
        workflow: Workflow[R, E, I, S, O],
        input: I,
      ): ZIO[Scope & R, E, O] =
        workflow.run(input)

    /**
     * Durable and concurrency-safe: state is checkpointed to [[store]] under `(workflow.name, encoded input)`
     * after each step and deleted on completion, and [[lock]] serialises concurrent runs of the same key. The
     * lock is held for the whole run; the step loop threads the cursor in memory (no re-read, no re-lock). A
     * fresh run starts at [[Step.Init]] (derived from the empty store + `input`); a resumed one at
     * [[Step.Continue]] from the decoded state.
     */
    trait Default extends Runner[AdapterError]:

      /** The shared store checkpoints go to, keyed by `(workflow name, encoded input)`. */
      def store: KeyValueStore[(String, String), String]

      /** The per-key lock serialising concurrent runs of the same workflow instance. */
      def lock: KeyLock[(String, String)]

      /**
       * Run `workflow` for `input`, resuming from its checkpoint when one exists. Holds [[lock]] for the whole
       * run, loads the start cursor ([[Step.Init]] from an empty store, else [[Step.Continue]] from the
       * decoded checkpoint), and steps to completion via `loop`.
       *
       * @param workflow the workflow to run
       * @param input    the input that seeds and keys the run
       * @tparam R the environment each step needs
       * @tparam E the error a step may fail with
       * @tparam I the input a run seeds from
       * @tparam S the state advanced from step to step
       * @tparam O the output produced when the stepper finishes
       * @return the workflow output; fails with `E` if a step fails, or `AdapterError` if the store fails
       */
      override def run[R, E, I: Serde, S: Serde, O](
        workflow: Workflow[R, E, I, S, O],
        input: I,
      ): ZIO[Scope & R, E | AdapterError, O] =
        val entries = typed[I, S](workflow.name)
        lock.withPermit(workflow.name -> Serde[I].encode(input)):
          for
            found <- entries.get(input)
            cursor = found.fold(Step.init(input))(state => Step.continue(state))
            out   <- loop(workflow, entries, input, cursor)
          yield out

      /**
       * A typed, `name`-namespaced view of the shared [[store]]: the input `I` is the key and the state `S`
       * the value, with serialisation confined here. The key `Serde[I]` makes the composite store key
       * `(name, encode(input))` injective — its round-trip law (`decode ∘ encode == id`) forbids two distinct
       * inputs from colliding on one slot. A stored value that fails to decode surfaces as an `AdapterError`.
       *
       * @param name the workflow identity that namespaces its slots
       * @tparam I the input, encoded into the key
       * @tparam S the state, (de)serialised as the value
       * @return a typed store over this workflow's slice of the shared backend
       */
      private def typed[I: Serde, S: Serde](name: String): KeyValueStore[I, S] =
        new KeyValueStore[I, S]:
          private def slot(input: I): (String, String) = name -> Serde[I].encode(input)

          def get(input: I): IO[AdapterError, Option[S]] =
            store.get(slot(input)).flatMap {
              case None          => ZIO.none
              case Some(encoded) => ZIO.fromEither(Serde[S].decode(encoded).left.map(Serde.error)).map(Some(_))
            }

          def set(input: I, state: S): IO[AdapterError, Unit] = store.set(slot(input), Serde[S].encode(state))
          def delete(input: I): IO[AdapterError, Boolean]     = store.delete(slot(input))

      /**
       * Step `workflow` to completion from `cursor` under the already-held lock: checkpoint each new state to
       * `entries` and continue, delete it and return on [[Step.Done]], or re-seed on [[Step.Init]] (a restart
       * is not itself checkpointed — its re-seed is). Threads the cursor in memory and takes no lock.
       * Stack-safe via ZIO's trampoline.
       *
       * @param workflow the stepper to advance
       * @param entries  the typed store this run's state is checkpointed to
       * @param input    the run's input — the key every checkpoint is written under
       * @param cursor   the non-terminal cursor to step from
       * @tparam R the environment each step needs
       * @tparam E the error a step may fail with
       * @tparam I the input a step may restart from
       * @tparam S the state advanced from step to step
       * @tparam O the output produced when the stepper finishes
       * @return the workflow output; fails with `E` if a step fails, or `AdapterError` if the store fails
       */
      private def loop[R, E, I, S, O](
        workflow: Workflow[R, E, I, S, O],
        entries: KeyValueStore[I, S],
        input: I,
        cursor: Step.Pending[I, S],
      ): ZIO[R, E | AdapterError, O] =
        workflow.next(cursor).flatMap {
          case Step.Init(seed)      => loop(workflow, entries, input, Step.Init(seed))
          case Step.Continue(state) => entries.set(input, state) *> loop(workflow, entries, input, Step.Continue(state))
          case Step.Done(output)    => entries.delete(input).as(output)
        }
