package homelab.common.messaging

import zio.*


/**
 * A [[Producer]] that dispatches each value to a downstream producer chosen per value by `route`.
 * Emission becomes routing: `emit(v)` sends `v` (possibly rewritten) to the producer `route` picks.
 *
 * @tparam E the error emission aborts with
 * @tparam A the value routed
 */
trait Router[E, A] extends Producer[E, A] {

  /**
   * Choose the output value and target producer for an input.
   *
   * @return a function from an input to its `(value, producer)` route
   */
  def route: A => Router.Route[E, A]

  /**
   * Emit by routing: pick the route for `value`, then emit the routed value through its producer.
   *
   * @param value the value to route and emit
   * @return unit once emitted through the chosen producer; aborts with `E` on failure
   */
  def emit(value: A): IO[E, Unit] =
    route(value) match
      case (output, producer) => producer.emit(output)
}


object Router {

  /** A routing decision: the value to emit and the producer to emit it through. */
  type Route[E, A] = (A, Producer[E, A])
}
