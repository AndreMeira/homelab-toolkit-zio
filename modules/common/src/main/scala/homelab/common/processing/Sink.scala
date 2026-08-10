package homelab.common.processing


import homelab.common.messaging.Producer


/**
 * A [[Through]] that emits nowhere: its [[output]] is [[Producer.noop]], so the node consumes and processes
 * `A` for its effects alone and produces nothing downstream — the terminal node of a topology. The output
 * type is therefore fixed to `Unit`. A concrete sink adds a run loop by extending one of the sub-traits
 * ([[Sink.PerItem]], [[Sink.Parallel]], [[Sink.Batched]]).
 *
 * @tparam E the error processing aborts with
 * @tparam A the value consumed
 */
trait Sink[+E, +A] extends Through[E, A, Unit]:

  /**
   * The discarding output: every processed value is emitted to [[Producer.noop]] and dropped.
   *
   * @return the no-op producer
   */
  override def output: Producer[E, Unit] = Producer.noop


object Sink {

  /**
   * A [[Sink]] that consumes and processes one value at a time — a [[Through.PerItem]] whose output is
   * discarded. Implement `process` for its effect; it yields `Unit`.
   *
   * @tparam E the error processing aborts with
   * @tparam A the value consumed
   */
  trait PerItem[E, A] extends Through.PerItem[E, A, Unit] with Sink[E, A]

  /**
   * A [[Sink]] that processes up to `parallelism` values concurrently — a [[Through.Parallel]] whose output
   * is discarded.
   *
   * @tparam E the error processing aborts with
   * @tparam A the value consumed
   */
  trait Parallel[E, A] extends Through.Parallel[E, A, Unit] with Sink[E, A]

  /**
   * A [[Sink]] that consumes a batch at a time — a [[Through.Batched]] whose output is discarded. Its
   * `process` returns `List[Unit]` to satisfy [[Through.Batched]]; those units are dropped by [[output]].
   *
   * @tparam E the error processing aborts with
   * @tparam A the element consumed
   */
  trait Batched[E, A] extends Through.Batched[E, A, Unit] with Sink[E, List[A]]

  object Batched {

    /**
     * A [[Sink]] that processes up to `parallelism` batches concurrently — a [[Through.Batched.Parallel]]
     * whose output is discarded.
     *
     * @tparam E the error processing aborts with
     * @tparam A the element consumed
     */
    trait Parallel[E, A] extends Through.Batched.Parallel[E, A, Unit] with Sink[E, List[A]]
  }
}
