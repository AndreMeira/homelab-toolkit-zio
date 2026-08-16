package homelab.nats


import io.nats.client.Message
import io.nats.client.impl.NatsMessage

import java.nio.charset.StandardCharsets


/**
 * The seam between a domain value and a NATS message — split by direction, resolved implicitly at each
 * producer and consumer.
 *
 * Both halves speak `io.nats.client.Message` rather than raw bytes, which is what makes the seam wide enough
 * to be useful on this substrate: a decoder can read headers, the subject a wildcard matched, or a reply-to;
 * an encoder can set headers and, necessarily, the subject — a NATS message cannot exist without one, so
 * choosing where a value goes is part of encoding it, not a separate function handed to the producer.
 *
 * The two are separate typeclasses because call sites are one-directional: a producer only writes, a consumer
 * only reads, and asking for both would make each demand a capability it never uses.
 */
object Codec {

  /**
   * Turning a value into the message that carries it, subject included.
   *
   * @tparam A the domain value carried
   */
  trait Encoder[A] {

    /**
     * Build the message for a value.
     *
     * @param value the value to publish
     * @return the message to publish, addressed to its subject
     */
    def encode(value: A): Message
  }

  object Encoder {

    /**
     * Summon the `Encoder[A]` in scope.
     *
     * @tparam A the domain value
     * @return the resolved encoder
     */
    def apply[A](using encoder: Encoder[A]): Encoder[A] = encoder

    /**
     * UTF-8 text onto a subject derived from the text itself — the keying convention, as an encoder. A plain
     * instance to pass explicitly (`using Codec.Encoder.utf8(...)`), not an ambient given.
     *
     * @param subjectOf derives a value's subject (its partition key)
     * @return an encoder writing UTF-8 bytes to that subject
     */
    def utf8(subjectOf: String => String): Encoder[String] = value =>
      NatsMessage.builder().subject(subjectOf(value)).data(value.getBytes(StandardCharsets.UTF_8)).build()
  }

  /**
   * Reading a value back out of a received message.
   *
   * @tparam A the domain value carried
   */
  trait Decoder[A] {

    /**
     * Decode a received message.
     *
     * @param message the received message, headers and subject included
     * @return the decoded value, or a `Left` reason if the message is malformed
     */
    def decode(message: Message): Either[String, A]
  }

  object Decoder {

    /**
     * Summon the `Decoder[A]` in scope.
     *
     * @tparam A the domain value
     * @return the resolved decoder
     */
    def apply[A](using decoder: Decoder[A]): Decoder[A] = decoder

    /** A UTF-8 text decoder — a plain instance to pass explicitly (`using Codec.Decoder.utf8`). */
    val utf8: Decoder[String] = message => Right(new String(message.getData, StandardCharsets.UTF_8))
  }
}
