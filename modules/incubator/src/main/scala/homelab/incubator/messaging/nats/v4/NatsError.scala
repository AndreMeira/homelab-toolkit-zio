package homelab.incubator.messaging.nats.v4


/**
 * Typed failures for the unified NATS adapter sketch (v4). Standalone while exploring; on graduation
 * these map onto the toolkit's `ApplicationError` hierarchy. `Ack` only arises on the JetStream path
 * (Core NATS has no acknowledgement).
 */
enum NatsError:
  /** Connecting, stream/consumer setup, subscribing, or receiving failed. */
  case Connect(cause: Throwable)

  /** Publishing a message failed. */
  case Publish(cause: Throwable)

  /** Acknowledging a JetStream message (ack / nak / term) failed. */
  case Ack(cause: Throwable)

  /** A received payload could not be decoded into the domain type. */
  case Decode(reason: String)
