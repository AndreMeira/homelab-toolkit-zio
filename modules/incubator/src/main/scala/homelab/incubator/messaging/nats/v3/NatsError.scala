package homelab.incubator.messaging.nats.v3


/**
 * Typed failures for the JetStream adapter sketch (v3). Standalone while exploring; on graduation these
 * map onto the toolkit's `ApplicationError` hierarchy.
 */
enum NatsError:
  /** Connecting, stream/consumer setup, or receiving failed. */
  case Connect(cause: Throwable)

  /** A durable publish failed (no `PublishAck`). */
  case Publish(cause: Throwable)

  /** Acknowledging a message (ack / nak / term) failed. */
  case Ack(cause: Throwable)

  /** A received payload could not be decoded into the domain type. */
  case Decode(reason: String)
