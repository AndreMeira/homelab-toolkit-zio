package homelab.incubator.messaging.nats.v2


/**
 * Typed failures for the NATS adapter sketch (v2). Standalone while exploring; on graduation these map
 * onto the toolkit's `ApplicationError` hierarchy.
 */
enum NatsError:
  /** Establishing/closing the make, or (un)subscribing, failed. */
  case Connect(cause: Throwable)

  /** Publishing a message failed. */
  case Publish(cause: Throwable)

  /** A received payload could not be decoded into the domain type. */
  case Decode(reason: String)
