package homelab.common.messaging


import zio.*


/**
 * A [[Producer]] that fans out: every emitted value goes to all subscribed `producers`. Emission is
 * uninterruptible, so a value reaches every subscriber or the fan-out fails as a whole.
 *
 * @tparam E the error a fan-out aborts with
 * @tparam A the value fanned out
 */
trait Hub[E, A] extends Producer[E, A] {

  /**
   * The producers this hub fans out to.
   *
   * @return the subscribed producers
   */
  def producers: Set[Producer[E, A]]

  /**
   * Emit `value` to every subscribed producer.
   *
   * @param value the value to fan out
   * @return unit once every producer has received it; aborts with `E` on the first failure
   */
  def emit(value: A): IO[E, Unit] =
    ZIO.uninterruptible(ZIO.foreachDiscard(producers)(_.emit(value)))
}


object Hub {

  /**
   * Fan out to a fixed set of producers.
   *
   * @param producers the producers to fan out to
   * @tparam E the error a fan-out aborts with
   * @tparam A the value fanned out
   * @return a hub over `producers`
   */
  def collect[E, A](producers: Set[Producer[E, A]]): Hub[E, A] = Simple(producers)

  /** A hub holding a fixed producer set. */
  private final case class Simple[E, A](producers: Set[Producer[E, A]]) extends Hub[E, A]
}
