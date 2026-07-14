package homelab.incubator.messaging.mailbox


/**
 * A substrate-neutral reply address — an opaque routing token (`<: String`) that an adapter serializes its
 * internal address into (a NATS inbox subject, an in-memory UUID string, a Postgres `pod.msg` pair, …) and
 * parses back on `deliver`. The domain carries this stable type on a request's `replyTo`, never a
 * broker-specific one, so switching substrate leaves request DTOs untouched.
 */
type Address = Address.Type

object Address:
  opaque type Type <: String = String

  /** Wrap a raw token as an address. */
  def apply(value: String): Type = value
