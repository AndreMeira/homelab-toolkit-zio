package homelab.nats


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
