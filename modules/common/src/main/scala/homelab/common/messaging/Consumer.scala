package homelab.common.messaging


import zio.*


/**
 * The intake side of a topology: a source of values of type `A` that hands each to caller-supplied
 * `logic`. The adapter wraps `logic` with the substrate's commit/ack boundary, so offset and
 * acknowledgement handling never surface here. Because a message may be redelivered on failure,
 * '''`logic` must be idempotent'''.
 *
 * @tparam E the error consuming aborts with
 * @tparam A the value consumed
 */
trait Consumer[+E, +A] { self =>

  /**
   * Take the next value (or batch, for [[Consumer.Batched]]) and run `logic` on it, within the
   * adapter's commit boundary. One call processes one unit; a run loop calls it repeatedly.
   *
   * @param logic processes one consumed value
   * @tparam E2 the widened error, admitting `logic`'s failures
   * @return unit once the value is processed and committed; aborts with `E2` on failure
   */
  def consume[E2 >: E](logic: A => IO[E2, Unit]): IO[E2, Unit]

  /**
   * Adapt this consumer to deliver `B` by mapping each consumed `A`.
   *
   * @param f maps a consumed `A` to the delivered `B`
   * @tparam B the adapted output type
   * @return a consumer that delivers `f(a)` to its logic
   */
  def map[B](f: A => B): Consumer[E, B] = new Consumer[E, B] {
    def consume[E2 >: E](logic: B => IO[E2, Unit]): IO[E2, Unit] = self.consume(a => logic(f(a)))
  }
}


object Consumer {

  /**
   * A consumer that delivers messages in batches — one `consume` call processes up to a whole
   * [[List]] of `A`. The batch size is fixed where the adapter constructs it, not at the call site;
   * the batching shape is carried by the type, not by a parameter.
   *
   * @tparam E the error consuming aborts with
   * @tparam A the element type of each delivered batch
   */
  trait Batched[+E, +A] extends Consumer[E, List[A]]

  /**
   * A consumer that runs its logic once with `()` and never fails — a no-op intake.
   *
   * @return a consumer that delivers a single `Unit`
   */
  val unit: Consumer[Nothing, Unit] = new Consumer[Nothing, Unit] {
    def consume[E2 >: Nothing](logic: Unit => IO[E2, Unit]): IO[E2, Unit] = logic(())
  }
}
