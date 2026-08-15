package homelab.common.processing


import homelab.common.error.ApplicationError
import homelab.common.messaging.*
import zio.*


/**
 * A [[Node]] with an input: it consumes `A` and emits however it likes — to zero, one, or many
 * [[Producer]]s held as its own fields. Emission is intentionally unspecified here; the common
 * 1-in/1-out case is [[Through]], which also supplies the run loop. A worker that fans out to several
 * outputs (success + dead-letter, say) extends `Processor` directly and writes its own `run`.
 *
 * Its failures are [[ApplicationError]]s: a processor is something a [[Graph]] runs, and a graph has one
 * error channel for everything in it. Wrap whatever a library throws at the adapter edge, as everything else
 * here does, rather than widening this.
 *
 * @tparam E the error processing aborts with
 * @tparam A the value consumed
 */
trait Processor[+E <: ApplicationError, A] {

  /**
   * This processor's identity to a [[Graph]]. A `val`, so one instance has one key for its whole life: a
   * graph that meets the same processor twice — registered directly and reached again as someone's child —
   * starts it once. Reference identity is the right notion here; two structurally equal processors over
   * different pipes are two processors.
   */
  private[processing] val key: Processor.Key = new Processor.Key
  
  /**
   * The intake this processor consumes from.
   *
   * @return the input consumer
   */
  def input: Consumer[E, A]

  /**
   * Transform a single input into a single output.
   *
   * @param value the input to transform
   * @return the output; aborts with `E` on failure
   */
  def process(value: A): IO[E, Unit]

  /**
   * Consume, transform, and emit in a loop until interrupted or the first failure, via
   * [[Processor.serial]].
   *
   * @return never completes successfully; aborts with `E` on failure
   */
  def run: ZIO[Scope, E, Nothing] = Processor.serial(input)(process)
}


object Processor {
  
  /** A processor's identity, compared by reference — see [[Processor.key]]. */
  private[processing] class Key

  /**
   * A [[Processor]] that handles values concurrently up to a configured `parallelism` limit.
   * Uses on-demand listener spawning to keep fiber count proportional to actual work.
   *
   * @tparam E the error processing aborts with
   * @tparam A the value consumed
   */
  trait Parallel[E <: ApplicationError, A] extends Processor[E, A] {

    /**
     * The concurrency cap: how many values may run `handle` concurrently. Must be positive.
     *
     * @return the maximum number of concurrent `handle` executions
     */
    def parallelism: Int

    /**
     * Consume, transform, and emit in a loop until interrupted or the first failure, via
     * [[Processor.parallel]].
     *
     * @return never completes successfully; aborts with `E` on failure
     */
    override def run: ZIO[Scope, E, Nothing] = Processor.parallel(input, parallelism)(process)
  }

  /**
   * A [[Processor]] whose intake delivers batches — its `input` is a [[Consumer.Batched]].
   *
   * @tparam E the error processing aborts with
   * @tparam A the element type of each consumed batch
   */
  trait Batched[E <: ApplicationError, A] extends Processor[E, List[A]] {

    /**
     * The batched intake this processor consumes from.
     *
     * @return the batched input consumer
     */
    def input: Consumer.Batched[E, A]
  }

  object Batched {

    /**
     * A [[Processor.Batched]] that handles batches concurrently up to a configured `parallelism` limit.
     * Combines batched consumption with concurrent processing via on-demand listener spawning.
     *
     * @tparam E the error processing aborts with
     * @tparam A the element type of each consumed batch
     */
    trait Parallel[E <: ApplicationError, A] extends Processor.Parallel[E, List[A]] with Processor.Batched[E, A] {

      /**
       * The batched intake this processor consumes from.
       *
       * @return the batched input consumer
       */
      def input: Consumer.Batched[E, A]
    }
  }

  /**
   * Consume one value at a time and run `handle` on it, looping until interrupted or the first failure.
   *
   * @param input  the intake to pull values from
   * @param handle the per-value effect, run inside the delivering consume call
   * @tparam E the error the loop aborts with
   * @tparam A the value consumed
   * @return never completes successfully; aborts with `E` on the first failure
   */
  def serial[E, A](input: Consumer[E, A])(handle: A => IO[E, Unit]): IO[E, Nothing] =
    input.consume(handle).forever

  /**
   * Process up to `parallelism` values concurrently by spawning consume calls *on demand*: exactly one
   * listener waits on `input` at a time, and the moment it receives a value (and a permit) it hands
   * listening over to a freshly spawned listener and runs `handle` inline. Fibers therefore scale with
   * actual work — an idle loop is a single parked fiber, and a backlog holds at most `parallelism`
   * in-flight `handle` runs plus one waiting listener, regardless of depth.
   *
   * Errors fail fast: the first failing `handle` (or `consume`) aborts the returned effect with that
   * error and interrupts all in-flight work. The spawner is forked into the ambient [[Scope]], so all
   * listeners are interrupted when it closes.
   *
   * @param input       the intake to pull values from
   * @param parallelism the concurrency cap — how many values may run `handle` at once; must be positive
   * @param handle      the per-value effect, run under a permit inside the delivering consume call
   * @tparam E the error the loop aborts with
   * @tparam A the value consumed
   * @return never completes successfully; aborts with `E` on the first failure; requires a [[Scope]] that
   *         bounds the spawned listeners' lifetime
   */
  def parallel[E, A](input: Consumer[E, A], parallelism: Int)(handle: A => IO[E, Unit]): ZIO[Scope, E, Nothing] =
    for
      scope   <- ZIO.scope
      sem     <- Semaphore.make(parallelism)
      failure <- Promise.make[E, Nothing]
      _       <- spawn(input, sem, failure)(handle).forever.forkIn(scope)
      never   <- failure.await
    yield never

  /**
   * One spawn step: fork a listener and park until it starts running `handle` — holding a value *and* a
   * permit — then return, so [[parallel]]'s `serial` spawns the next listener exactly then. Repeated,
   * this keeps one waiting listener and up to `parallelism` running ones, with fibers appearing only when
   * there is work. `handle` runs inside the listener's consume call, keeping any key held for its whole
   * duration. A listener's failure is captured inside its fiber into `failure` (it is otherwise
   * unobserved), which is what aborts [[parallel]].
   *
   * @param input   the intake the spawned listener consumes from
   * @param sem     caps concurrent `handle` runs at `parallelism`
   * @param failure the sink a listener's failure is reported to
   * @param handle  the per-value effect, run under a permit
   * @tparam E the error a listener aborts with
   * @tparam A the value consumed
   * @return noop once the spawned listener has started running `handle` (holding a value and a permit)
   */
  private def spawn[E, A](
    input: Consumer[E, A],
    sem: Semaphore,
    failure: Promise[E, Nothing],
  )(
    handle: A => IO[E, Unit]
  ): ZIO[Scope, Nothing, Unit] =
    for
      scope   <- ZIO.service[Scope]
      started <- Promise.make[Nothing, Unit]
      _       <- input
                   // Signal INSIDE the permit: the next listener spawns only once this value holds a
                   // permit, so claimed-but-waiting fibers cannot pile up under a deep backlog.
                   .consume: value =>
                     sem.withPermit(started.succeed(()) *> handle(value))
                   .catchAllCause(failure.failCause(_).unit)
                   .forkIn(scope)
      _       <- started.await
    yield ()
}
