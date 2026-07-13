package homelab.incubator.messaging.nats.v1


/**
 * Typed failures for the NATS adapter sketch. Standalone here; when this graduates to a real module
 * these would map onto the toolkit's `ApplicationError` hierarchy (`Connect`/`Publish`/`Receive` →
 * `FromException`, `Decode` → a `ValidationError`/`VendorError`). Kept local while exploring the shape.
 */
enum NatsError:
  /** Establishing or closing the make failed. */
  case Connect(cause: Throwable)

  /** Publishing a message failed. */
  case Publish(cause: Throwable)

  /** Receiving the next message failed (I/O or interruption). */
  case Receive(cause: Throwable)

  /** A received payload could not be decoded into the domain type. */
  case Decode(reason: String)
