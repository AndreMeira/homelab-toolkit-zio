package homelab.common.processing


import homelab.common.messaging.Producer
import zio.*


/**
 * The 1-input / 1-output [[Processor]]: consume `A`, transform, emit `B` on a single `output`.
 * Multi-output or output-less workers extend [[Processor]] (or [[Node]]) directly and manage
 * producers as fields.
 *
 * @tparam E the error processing aborts with
 * @tparam A the input value
 * @tparam B the output value
 */
trait Through[+E, +A, -B] extends Processor[E, A] {

  /**
   * The single output this through node emits to.
   *
   * @return the output producer
   */
  def output: Producer[E, B]
}


object Through {

  /**
   * A [[Through]] that transforms one input into one output at a time.
   *
   * @tparam E the error processing aborts with
   * @tparam A the input value
   * @tparam B the output value
   */
  trait PerItem[E, A, B] extends Through[E, A, B] {

    /**
     * Transform a single input into a single output.
     *
     * @param value the input to transform
     * @return the output; aborts with `E` on failure
     */
    def process(value: A): IO[E, B]

    /**
     * Consume, transform, and emit in a loop until interrupted or the first failure, via
     * [[Processor.serial]].
     *
     * @return never completes successfully; aborts with `E` on failure
     */
    def run: IO[E, Nothing] =
      Processor.serial(input): value =>
        process(value).flatMap(output.emit)
  }

  /**
   * A [[Through]] that processes up to `parallelism` values concurrently, transforming each and emitting
   * the result to `output`. The concurrency strategy — spawn-on-demand, fibers scaling with work,
   * fail-fast, keyed inputs kept serialised — is [[Processor.parallel]].
   *
   * @tparam E the error processing aborts with
   * @tparam A the value consumed
   * @tparam B the value emitted
   */
  trait Parallel[E, A, B] extends Through[E, A, B] {

    /**
     * The concurrency cap — how many values may process at once. Must be positive.
     *
     * @return the maximum number of concurrent `process` runs
     */
    def parallelism: Int

    /**
     * Transform a single input into a single output.
     *
     * @param value the input to transform
     * @return the output; aborts with `E` on failure
     */
    def process(value: A): IO[E, B]

    /**
     * Process values concurrently via [[Processor.parallel]], transforming each and emitting it to `output`.
     * Requires a [[Scope]]: the spawned listeners are bound to it and interrupted when it closes.
     *
     * @return never completes successfully; aborts with `E` on the first failure
     */
    override def run: ZIO[Scope, E, Nothing] =
      Processor.parallel(input, parallelism): value =>
        process(value).flatMap(output.emit)
  }

  /**
   * A [[Through]] that transforms a batch of inputs into a batch of outputs at a time. The transform is
   * free to change cardinality (filter, fan-out): `process` maps a `List[A]` to a `List[B]` of any
   * length.
   *
   * @tparam E the error processing aborts with
   * @tparam A the input element
   * @tparam B the output element
   */
  trait Batched[E, A, B] extends Processor.Batched[E, A] with Through[E, List[A], B] {

    /**
     * Transform a batch of inputs into a batch of outputs.
     *
     * @param values the input batch
     * @return the output batch (of any length); aborts with `E` on failure
     */
    def process(values: List[A]): IO[E, List[B]]

    /**
     * Consume a batch, transform it, and emit the results in a loop until interrupted or the first
     * failure, via [[Processor.serial]].
     *
     * @return never completes successfully; aborts with `E` on failure
     */
    def run: IO[E, Nothing] =
      Processor.serial(input): values =>
        process(values).flatMap(output.emitMany)
  }

  object Batched {

    /**
     * The batched counterpart of [[Through.Parallel]]: processes up to `parallelism` *batches*
     * concurrently, emitting each result batch to `output` with `emitMany`. The concurrency strategy is
     * [[Processor.parallel]] with its value type instantiated to the batch `List[A]`.
     *
     * @tparam E the error processing aborts with
     * @tparam A the input element
     * @tparam B the output element
     */
    trait Parallel[E, A, B] extends Processor.Batched[E, A] with Through[E, List[A], B] {

      /**
       * The concurrency cap — how many batches may process at once. Must be positive.
       *
       * @return the maximum number of concurrent `process` runs
       */
      def parallelism: Int

      /**
       * Transform a batch of inputs into a batch of outputs.
       *
       * @param values the input batch
       * @return the output batch (of any length); aborts with `E` on failure
       */
      def process(values: List[A]): IO[E, List[B]]

      /**
       * Process batches concurrently via [[Processor.parallel]], transforming each and emitting it to
       * `output` with `emitMany`. Requires a [[Scope]]: the spawned listeners are bound to it and
       * interrupted when it closes.
       *
       * @return never completes successfully; aborts with `E` on the first failure
       */
      override def run: ZIO[Scope, E, Nothing] =
        Processor.parallel(input, parallelism): values =>
          process(values).flatMap(output.emitMany)
    }
  }
}
