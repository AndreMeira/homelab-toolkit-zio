package homelab.incubator.messaging.nats.v5


object FailurePolicy:

  /**
   * What a consumer does when a payload can't be decoded. A decode failure is disproportionately a *code* bug
   * (schema drift, a wrong `Serde`) rather than bad data, so the default is [[Surface]] — non-destructive on
   * JetStream (the un-acked message stays for reprocessing once the bug is fixed) and loud on Core. [[Discard]]
   * drops the message and keeps consuming. Shared by the JetStream and Core consumers; the *action* differs by
   * substrate (see each case).
   */
  enum DecodeFailurePolicy:
    /** Fail `consume` with [[NatsError.Decode]] — on JetStream the message is left un-acked for reprocessing;
     * on Core (no ack) the loop simply stops (default). */
    case Surface

    /** Drop the message and keep consuming — `term` (stop redelivery) on JetStream, skip it on Core. */
    case Discard

  /**
   * What a JetStream consumer does when the handler fails on a decoded message. The default is [[Redeliver]]
   * (`nak`) — the idiomatic at-least-once retry, also non-destructive. [[Discard]] gives up on the
   * message; [[Surface]] fails `consume` with the handler's own error.
   */
  enum HandlerFailurePolicy:
    /** `nak` the message — retry via redelivery (default). */
    case Redeliver

    /** `term` the message — stop redelivering it. */
    case Discard

    /** Fail `consume` with the handler's error, without settling — the message is redelivered (like
     * [[DecodeFailurePolicy.Surface]], it survives for reprocessing). */
    case Surface
