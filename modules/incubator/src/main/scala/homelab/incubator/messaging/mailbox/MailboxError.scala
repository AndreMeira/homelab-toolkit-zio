package homelab.incubator.messaging.mailbox


/**
 * Typed failures for the mailbox prototype. [[Decode]] is the only failure the in-memory backend raises — a
 * delivered payload the awaiting side can't decode. A real broker adapter would add `Connect`/`Publish` and
 * lift these onto the toolkit's `ApplicationError` hierarchy (as `NatsError` does).
 */
enum MailboxError:

  /** A delivered payload could not be decoded into the expected reply type. */
  case Decode(reason: String)
