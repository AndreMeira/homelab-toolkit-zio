package homelab.common.messaging


import zio.*


/**
 * The 1-input / 1-output [[Processor]]: consume `A`, transform, emit `B` on a single `output`.
 * Multi-output or output-less workers extend [[Processor]] (or [[Worker]]) directly and manage
 * producers as fields.
 *
 * @tparam E the error processing aborts with
 * @tparam A the input value
 * @tparam B the output value
 */
trait Pipe[+E, +A, -B] extends Processor[E, A] {

  /**
   * The single output this pipe emits to.
   *
   * @return the output producer
   */
  def output: Producer[E, B]
}


object Pipe {

  /**
   * A [[Pipe]] that transforms one input into one output at a time.
   *
   * @tparam E the error processing aborts with
   * @tparam A the input value
   * @tparam B the output value
   */
  trait PerItem[E, A, B] extends Pipe[E, A, B] {

    /**
     * Transform a single input into a single output.
     *
     * @param value the input to transform
     * @return the output; aborts with `E` on failure
     */
    def process(value: A): IO[E, B]

    /**
     * Consume, transform, and emit in a loop until interrupted or the first failure.
     *
     * @return never completes successfully; aborts with `E` on failure
     */
    def run: IO[E, Nothing] = input.consume(value => process(value).flatMap(output.emit)).forever
  }

  /**
   * A [[Pipe]] that transforms a batch of inputs into a batch of outputs at a time. The transform is
   * free to change cardinality (filter, fan-out): `process` maps a `List[A]` to a `List[B]` of any
   * length.
   *
   * @tparam E the error processing aborts with
   * @tparam A the input element
   * @tparam B the output element
   */
  trait Batched[E, A, B] extends Processor.Batched[E, A] with Pipe[E, List[A], B] {

    /**
     * Transform a batch of inputs into a batch of outputs.
     *
     * @param values the input batch
     * @return the output batch (of any length); aborts with `E` on failure
     */
    def process(values: List[A]): IO[E, List[B]]

    /**
     * Consume a batch, transform it, and emit the results in a loop until interrupted or the first
     * failure.
     *
     * @return never completes successfully; aborts with `E` on failure
     */
    def run: IO[E, Nothing] = input.consume(values => process(values).flatMap(output.emitMany)).forever
  }
}
