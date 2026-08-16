package homelab.nats.mailbox


import homelab.common.processing.Mailbox as MailboxContract
import homelab.common.processing.Mailbox.Address
import homelab.nats.Codec
import io.nats.client.impl.NatsMessage


/**
 * The wire mapping between a [[MailboxContract.Message]] and a NATS message: the address is the subject, the
 * payload the body. Nothing else — a subject already *is* an address, so there is no envelope to invent.
 *
 * Kept in its own object rather than beside the adapter that uses them: a `given` resolved from inside the
 * same object that defines it puts the compiler in a completion cycle ([[Mailbox]]'s outgoing half needs the
 * encoder to type its `producer`, and completing the encoder needs the enclosing scope), which surfaces as a
 * *cyclic reference* error under some compilation orders and not others. Both are named for the same reason
 * — an anonymous `given` has its name synthesised from its type, which is one more thing to complete.
 */
object MessageCodec {

  /**
   * A mailbox message as a NATS message, addressed to the subject its address names.
   *
   * @return an encoder building a publishable NATS message
   */
  given mailboxMessageEncoder: Codec.Encoder[MailboxContract.Message] = message =>
    NatsMessage.builder().subject(message.address).data(message.payload).build()

  /**
   * A NATS message as a mailbox message. The subject it arrived on becomes the address, which is how a reply
   * is matched to the expectation waiting at that droppoff. Total — a delivered message always has both.
   *
   * @return a decoder that never fails
   */
  given mailboxMessageDecoder: Codec.Decoder[MailboxContract.Message] = message =>
    Right(MailboxContract.Message(Address(message.getSubject), message.getData))
}
