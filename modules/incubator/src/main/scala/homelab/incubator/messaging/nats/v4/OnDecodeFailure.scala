package homelab.incubator.messaging.nats.v4


/**
 * What a JetStream consumer does when a payload can't be decoded. A decode failure is disproportionately a
 * *code* bug (schema drift, a wrong `Serde`) rather than bad data — so the default is [[Surface]], which is
 * '''non-destructive''': the un-acked message stays in the stream and is redelivered once the bug is fixed.
 * [[DeadLetter]] (`term`) permanently stops its redelivery, so recovery needs a manual replay.
 */
enum OnDecodeFailure:
  /** Fail `consume` with [[NatsError.Decode]] without settling — the message survives for reprocessing (default). */
  case Surface

  /** `term` the message — stop redelivering it (recover via manual replay). */
  case DeadLetter
