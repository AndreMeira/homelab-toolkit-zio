package homelab.common.messaging


/**
 * A [[Worker]] with an input: it consumes `A` and emits however it likes — to zero, one, or many
 * [[Producer]]s held as its own fields. Emission is intentionally unspecified here; the common
 * 1-in/1-out case is [[Pipe]], which also supplies the run loop. A worker that fans out to several
 * outputs (success + dead-letter, say) extends `Processor` directly and writes its own `run`.
 *
 * @tparam E the error processing aborts with
 * @tparam A the value consumed
 */
trait Processor[+E, +A] extends Worker[E] {

  /**
   * The intake this processor consumes from.
   *
   * @return the input consumer
   */
  def input: Consumer[E, A]
}


object Processor {

  /**
   * A [[Processor]] whose intake delivers batches — its `input` is a [[Consumer.Batched]].
   *
   * @tparam E the error processing aborts with
   * @tparam A the element type of each consumed batch
   */
  trait Batched[E, A] extends Processor[E, List[A]] {

    /**
     * The batched intake this processor consumes from.
     *
     * @return the batched input consumer
     */
    def input: Consumer.Batched[E, A]
  }
}
