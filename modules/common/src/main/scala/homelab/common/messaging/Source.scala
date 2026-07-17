package homelab.common.messaging

import zio.*


/**
 * A [[Worker]] with an output and no input: it generates values and emits them. The generation loop
 * is supplied by a sub-trait — [[Source.Repeat]] emits as fast as it can, [[Source.Tick]] emits on a
 * schedule.
 *
 * @tparam E the error generation aborts with
 * @tparam A the value produced
 */
trait Source[+E, -A] extends Worker[E] {

  /**
   * The destination for generated values.
   *
   * @return the output producer
   */
  def output: Producer[E, A]
}


object Source {

  /**
   * A source that generates and emits back-to-back, as fast as `generate` yields values.
   *
   * @tparam E the error generation aborts with
   * @tparam A the value produced
   */
  trait Repeat[E, A] extends Source[E, A] {

    /**
     * Produce the next value.
     *
     * @return the next value; aborts with `E` on failure
     */
    def generate: IO[E, A]

    /**
     * Generate and emit in a tight loop until interrupted or the first failure.
     *
     * @return never completes successfully; aborts with `E` on failure
     */
    def run: IO[E, Nothing] = generate.flatMap(output.emit).forever
  }

  /**
   * A source that emits on a schedule: `initial` delay before the first value, then one every
   * `interval`.
   *
   * @tparam E the error generation aborts with
   * @tparam A the value produced
   */
  trait Tick[E, A] extends Source[E, A] {

    /**
     * The delay before the first value.
     *
     * @return the initial delay
     */
    def initial: Duration

    /**
     * The gap between successive values.
     *
     * @return the inter-value interval
     */
    def interval: Duration

    /**
     * Produce the next value.
     *
     * @return the next value; aborts with `E` on failure
     */
    def generate: IO[E, A]

    /**
     * Wait `initial`, then generate-and-emit once every `interval`, until interrupted or the first
     * failure. `repeat(spaced(interval))` runs the first tick immediately (after the initial delay)
     * and spaces every tick thereafter.
     *
     * @return never completes successfully; aborts with `E` on failure
     */
    def run: IO[E, Nothing] =
      val tick = generate.flatMap(output.emit)
      (ZIO.sleep(initial) *> tick.repeat(Schedule.spaced(interval))).forever
  }
}
