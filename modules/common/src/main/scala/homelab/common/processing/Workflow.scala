package homelab.common.processing


import homelab.common.error.ApplicationError.AdapterError
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
 * The `Workflow` is only the *behaviour*. How it is run is layered on by combinators that each return
 * another `Workflow`: [[run]] alone steps in memory, [[persisted]] makes it durable and resumable,
 * [[serialised]] makes it concurrency-safe per input, and [[intercept]] / [[tap]] rewrite or observe its
 * steps. A decorated workflow is still a workflow, so what a caller receives is the same type whatever it
 * was given.
 *
 * @tparam R the environment each step needs
 * @tparam E the error a step may fail with
 * @tparam I the input a run seeds from
 * @tparam S the state advanced from step to step
 * @tparam O the output produced when the stepper finishes
 */
trait Workflow[-R, +E, I, S, +O]:
  self =>

  /** A stable identity, used to namespace this workflow's state in a shared store. */
  def name: String = getClass.getSimpleName

  /**
   * The transition: given a non-terminal cursor, produce the next [[Step]] — seed on [[Step.Init]], advance
   * on [[Step.Continue]], or terminate ([[Step.Done]]) / restart ([[Step.Init]]).
   *
   * @return the transition function; fails with `E` if the step fails
   */
  def next: Step.Pending[I, S] => ZIO[R, E, Step[I, S, O]]

  /**
   * Run the stepper in memory from `input` until [[next]] finishes, without persistence. Stack-safe (ZIO's
   * `flatMap` trampolines); for a durable, resumable run, decorate with [[persisted]] first.
   *
   * @param input the input to seed the run from
   * @return the output produced when the stepper finishes; fails with `E` if any step fails
   */
  def run(input: I): ZIO[R, E, O] = loop(Step.Init(input))

  /**
   * Wrap this workflow so every step it produces passes through `fn` before the runner acts on it. The
   * original [[next]] still decides; `fn` sees its [[Step]] and may rewrite it — continue with different
   * state, finish early, restart, or map a [[Step.Done]]'s output to a new type. Because `fn` receives the
   * *whole* step, `Done` included, the original output `O` is consumed rather than propagated and `O2` is
   * free of it; `E2` must admit `E` because the wrapped `next` still runs and can still fail.
   *
   * The [[name]] is carried over, so an intercepted workflow checkpoints to — and resumes from — the same
   * slots as the one it wraps.
   *
   * @param fn the interceptor: given what [[next]] produced, produce the step the runner acts on
   * @tparam R2 the environment `fn` needs, intersected with this workflow's own
   * @tparam E2 the error the wrapped pair may fail with — a supertype of `E`, since [[next]] still runs
   * @tparam O2 the output `fn` finishes with, replacing `O`
   * @return a workflow of the same name whose transition is this one followed by `fn`
   */
  def intercept[R2, E2 >: E, O2](
    fn: Step[I, S, O] => ZIO[R2, E2, Step[I, S, O2]]
  ): Workflow[R & R2, E2, I, S, O2] = new Workflow[R & R2, E2, I, S, O2] {
    override def name: String = self.name

    def next: Step.Pending[I, S] => ZIO[R & R2, E2, Step[I, S, O2]] =
      cursor => self.next(cursor).flatMap(fn)
  }

  /**
   * Observe every step this workflow produces without altering it — the read-only [[intercept]]: `fn` runs
   * for its effect and the original [[Step]] is passed on untouched, so `I`, `S` and `O` all survive and the
   * run goes exactly where it would have gone. The step for logging, metrics, and audit trails.
   *
   * `fn` is not fire-and-forget: it runs *before* the runner acts on the step (and, under a durable runner,
   * before the checkpoint is written), and its failure fails the step. An observer that must never break a
   * run should handle its own errors, leaving `E2` as `E`.
   *
   * @param fn the observer, run for its effect on each produced step
   * @tparam R2 the environment `fn` needs, intersected with this workflow's own
   * @tparam E2 the error the pair may fail with — a supertype of `E`, since a failing `fn` aborts the step
   * @return a workflow of the same name and shape, running `fn` on each step it produces
   */
  def tap[R2, E2 >: E](
    fn: Step[I, S, O] => ZIO[R2, E2, Unit]
  ): Workflow[R & R2, E2, I, S, O] = intercept(step => fn(step).as(step))

  /**
   * Make this workflow durable and resumable against `store`: [[run]] loads any checkpoint left under its
   * input and starts from there ([[Step.Continue]]) rather than seeding ([[Step.Init]]), writes each new
   * state as it is produced, and deletes the slot on [[Step.Done]]. A crashed run therefore leaves the last
   * good state behind, and re-running the same input replays only the step that failed.
   *
   * Persistence lives in [[run]], not in [[next]] — the transition is unchanged and still says nothing about
   * storage, so a persisted workflow keeps the same [[name]] and the same step semantics as the one it wraps.
   * A restart mid-run ([[Step.Init]]) re-seeds the *state* but keeps checkpointing under the original input,
   * so a run's slot is fixed for its whole life.
   *
   * This persists but does not serialise: nothing stops two concurrent runs of the same input from
   * interleaving their checkpoints. Where that matters, guard the run with a lock (see
   * [[homelab.common.flow.KeyLock]]) or use a store that does it.
   *
   * @param store the slots this workflow's state is checkpointed to, keyed by the run's input
   * @return the same workflow, checkpointing to — and resuming from — `store`; adds `AdapterError` to the
   *         error channel for the store's own failures
   */
  def persisted(store: KeyValueStore[I, S]): Workflow[R, E | AdapterError, I, S, O] =
    new Workflow[R, E | AdapterError, I, S, O] {
      override def name: String = self.name

      def next: Step.Pending[I, S] => ZIO[R, E | AdapterError, Step[I, S, O]] =
        self.next

      override def run(input: I): ZIO[R, E | AdapterError, O] =
        store.get(input).flatMap { found =>
          checkpointing(input, found.fold(Step.init(input))(Step.continue))
        }

      /**
       * Step to completion from `cursor`, checkpointing to `store` as it goes: write each new state, delete the
       * slot on [[Step.Done]], and carry a restart ([[Step.Init]]) without touching storage — its re-seeded state
       * is checkpointed by the step that follows. Every write goes under `input`, so a run's slot is fixed for
       * its whole life however the cursor moves. Threads the cursor in memory, so no step re-reads the store.
       *
       * The loop behind [[persisted]], kept here rather than inside it so the recursion is nameable and the
       * three things it depends on are stated rather than closed over.
       *
       * @param input  the run's input — the key every checkpoint is written under
       * @param cursor the non-terminal cursor to step from
       * @return the output; fails with `E` if a step fails, or `AdapterError` if the store does
       */
      private def checkpointing(input: I, cursor: Step.Pending[I, S]): ZIO[R, E | AdapterError, O] =
        next(cursor).flatMap {
          case Step.Init(seed)      => checkpointing(input, Step.Init(seed))
          case Step.Continue(state) => store.set(input, state) *> checkpointing(input, Step.Continue(state))
          case Step.Done(output)    => store.delete(input).as(output)
        }
    }

  /**
   * Serialise runs of this workflow by input: a run holds `lock`'s permit for its key from start to finish,
   * so two runs of the *same* input queue rather than interleave, while distinct inputs stay concurrent.
   * The error channel is untouched — a lock adds no failure of its own.
   *
   * Pair with [[persisted]] to get what a durable runner gives: without it, two concurrent runs of one input
   * both load the same checkpoint and race to overwrite it. **Order matters** — write
   * `wf.persisted(store).serialised(lock)`. [[persisted]] builds on [[next]] rather than [[run]], so a
   * `serialised` applied *first* would have its [[run]] bypassed and lock nothing.
   *
   * @param lock the per-key lock whose permit a run holds for its whole life
   * @return the same workflow, with runs of one input serialised against each other
   */
  def serialised(lock: KeyLock[I]): Workflow[R, E, I, S, O] =
    new Workflow[R, E, I, S, O] {
      override def name: String = self.name

      def next: Step.Pending[I, S] => ZIO[R, E, Step[I, S, O]] = self.next

      override def run(input: I): ZIO[R, E, O] = lock.withPermit(input)(self.run(input))
    }

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
  sealed trait Step[+I, +S, +O]

  object Step:

    /** 
     * The non-terminal cursors — the only things [[Workflow.next]] can step *from*. 
     */
    type Pending[+I, +S] = Init[I] | Continue[S]

    /**
     * Seed (or re-seed) the workflow from `input`.
     *
     * @param input the value used to seed the next effectful transition
     * @tparam I the input a run (or restart) seeds from
     */
    case class Init[+I](input: I) extends Step[I, Nothing, Nothing]

    /**
     * Advance the workflow, carrying a new `state`.
     *
     * @param state the current workflow state to be advanced by the next transition
     * @tparam S the state advanced from step to step
     */
    case class Continue[+S](state: S) extends Step[Nothing, S, Nothing]

    /**
     * Finish the workflow, yielding `output`.
     *
     * @param output the final result produced on completion
     * @tparam O the output produced when the workflow finishes
     */
    case class Done[+O](output: O) extends Step[Nothing, Nothing, O]

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

    /** [[Init]]'s companion — the case class constructor, plus its already-lifted form. */
    object Init:

      /**
       * [[Init]] lifted into an effect, so seeding and restarting read like the other two steps at a call
       * site — `Step.Init.succeed(id)` rather than `ZIO.succeed(Step.Init(id))`. Seeding is the step most
       * likely to do real work, so this is the exception rather than the rule here.
       *
       * @param value the input to seed (or re-seed) from
       * @tparam A the input type
       * @return the init step, already succeeded
       */
      def succeed[A](value: A): UIO[Init[A]] = ZIO.succeed(Init(value))

    /** [[Continue]]'s companion — the case class constructor, plus its already-lifted form. */
    object Continue:

      /**
       * [[Continue]] lifted into an effect, for the common transition that advances without doing any I/O —
       * `Step.Continue.succeed(n + 1)` rather than `ZIO.succeed(Step.Continue(n + 1))`.
       *
       * @param value the state to continue with
       * @tparam A the state type
       * @return the continue step, already succeeded
       */
      def succeed[A](value: A): UIO[Continue[A]] = ZIO.succeed(Continue(value))

    /** [[Done]]'s companion — the case class constructor, plus its already-lifted form. */
    object Done:

      /**
       * [[Done]] lifted into an effect, for the common transition that finishes without doing any I/O —
       * `Step.Done.succeed(total)` rather than `ZIO.succeed(Step.Done(total))`.
       *
       * @param value the output to finish with
       * @tparam A the output type
       * @return the done step, already succeeded
       */
      def succeed[A](value: A): UIO[Done[A]] = ZIO.succeed(Done(value))

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
    name: String
  )(
    next: Step.Pending[I, S] => ZIO[R, E, Step[I, S, O]]
  ): Workflow[R, E, I, S, O] =
    // Capture so the same-named overrides below don't resolve to themselves.
    val workflowName = name
    val transition   = next
    new Workflow[R, E, I, S, O]:
      override def name: String                                = workflowName
      def next: Step.Pending[I, S] => ZIO[R, E, Step[I, S, O]] = transition
