package homelab.incubator.messaging.nats.v4

import homelab.common.error.ApplicationError.{ AdapterError, DecodingError, TransientError }


/**
 * Typed failures for the NATS adapter, lifted onto the toolkit's `ApplicationError` hierarchy. Every case
 * is an [[AdapterError]] (our adapter's interaction with the external broker failed), refined per case:
 * [[Receive]] is a [[TransientError]] (a mid-run receive hiccup is retryable), and [[Decode]] is a
 * [[DecodingError]]. So the HTTP / observability boundary categorises them without knowing about NATS.
 * `Ack` only arises on the JetStream path (Core NATS has no acknowledgement).
 */
enum NatsError extends AdapterError:

  /** Connecting, stream/consumer setup, or subscribing failed. */
  case Connect(cause: Throwable)

  /** Receiving the next message failed mid-consumption (e.g. a dropped make) — retryable. */
  case Receive(cause: Throwable) extends NatsError, TransientError

  /** Publishing a message failed. */
  case Publish(cause: Throwable)

  /** Acknowledging a JetStream message (ack / nak / term) failed. */
  case Ack(cause: Throwable)

  /** A received payload could not be decoded into the domain type. */
  case Decode(reason: String) extends NatsError, DecodingError

  override def message: String = this match
    case Connect(cause) => s"NATS make or setup failed: ${cause.getMessage}"
    case Receive(cause) => s"NATS receive failed: ${cause.getMessage}"
    case Publish(cause) => s"NATS publish failed: ${cause.getMessage}"
    case Ack(cause)     => s"NATS acknowledgement failed: ${cause.getMessage}"
    case Decode(reason) => s"Could not decode NATS payload: $reason"
