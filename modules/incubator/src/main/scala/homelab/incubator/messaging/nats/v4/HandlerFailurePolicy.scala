package homelab.incubator.messaging.nats.v4


/**
 * What a JetStream consumer does when the handler fails on a decoded message. The default is [[Redeliver]]
 * (`nak`) — the idiomatic at-least-once retry, also non-destructive. [[DeadLetter]] gives up on the
 * message; [[Surface]] fails `consume` with the handler's own error.
 */
enum HandlerFailurePolicy:
  /** `nak` the message — retry via redelivery (default). */
  case Redeliver

  /** `term` the message — stop redelivering it. */
  case DeadLetter

  /** Fail `consume` with the handler's error. */
  case Surface
