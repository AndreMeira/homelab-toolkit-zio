package homelab.nats


/**
 * What a JetStream consumer does when processing a message fails. The default is [[Redeliver]] (`nak`) — the
 * idiomatic at-least-once retry, also non-destructive. [[Discard]] gives up on the message; [[Surface]] fails
 * `consume` with the handler's own error.
 *
 * It covers decoding too: a decoder is layered *over* a consumer of messages, so by the time a malformed
 * payload is noticed it is already inside the handler, and it settles the same way anything else that fails
 * does.
 */
enum HandlerFailurePolicy:
  /** `nak` the message — retry via redelivery (the default). */
  case Redeliver

  /** `term` the message — stop redelivering it. */
  case Discard

  /** Fail `consume` with the handler's error, without settling — the message stays un-acked and is
    * redelivered after `ackWait`, so it survives for reprocessing. */
  case Surface
