package homelab.common.messaging


import zio.*


/**
 * The emission side of a topology: somewhere to send values of type `A`. An adapter binds it to a
 * destination (an in-memory queue, a broker subject/topic) and, where the substrate partitions,
 * derives the partition key from the message itself — a pure `A => K` fixed at construction — so
 * `emit` never carries a key.
 *
 * @tparam E the error an emission aborts with
 * @tparam A the value emitted
 */
trait Producer[+E, -A] { self =>

  /**
   * Emit a single value.
   *
   * @param value the value to emit
   * @return unit once emitted; aborts with `E` on failure
   */
  def emit(value: A): IO[E, Unit]

  /**
   * Emit each value in order. Uninterruptible, so a partial emission cannot be torn.
   *
   * @param values the values to emit, in order
   * @return unit once all are emitted; aborts with `E` on the first failure
   */
  def emitMany(values: List[A]): IO[E, Unit] =
    ZIO.uninterruptible(ZIO.foreachDiscard(values)(emit))

  /**
   * Adapt this producer to accept `B` by mapping each `B` to an `A` before emitting.
   *
   * @param f maps an incoming `B` to the emitted `A`
   * @tparam B the adapted input type
   * @return a producer of `B` that emits `f(b)` through this one
   */
  def contramap[B](f: B => A): Producer[E, B] = value => self.emit(f(value))
}


object Producer {

  /**
   * A producer that discards every value and never fails — a no-op sink.
   *
   * @return a producer of `Unit` that does nothing
   */
  val unit: Producer[Nothing, Unit] = _ => ZIO.unit

  /**
   * A producer over the empty type — nothing can ever be emitted through it.
   *
   * @return a producer of `Nothing`
   */
  val nothing: Producer[Nothing, Nothing] = _ => ZIO.unit
}
