package homelab.incubator.messaging.nats.v4

import java.nio.charset.StandardCharsets


/**
 * The serialization seam between a domain value and the wire bytes — a typeclass, split by direction and
 * resolved implicitly at each producer/consumer.
 *
 * @tparam A the domain value carried
 */
trait Serde[A]:

  /**
   * Encode a value to its wire bytes.
   *
   * @param value the value to encode
   * @return the encoded payload
   */
  def encode(value: A): Array[Byte]

  /**
   * Decode wire bytes back into a value.
   *
   * @param bytes the received payload
   * @return the decoded value, or a `Left` reason if the payload is malformed
   */
  def decode(bytes: Array[Byte]): Either[String, A]


object Serde:

  /**
   * Summon the `Serde[A]` in scope.
   *
   * @tparam A the domain value
   * @return the resolved codec
   */
  def apply[A](using serde: Serde[A]): Serde[A] = serde

  /** A UTF-8 string codec — a plain instance to pass explicitly (`using Serde.utf8`), not an ambient given. */
  val utf8: Serde[String] = new Serde[String]:
    def encode(value: String): Array[Byte]                 = value.getBytes(StandardCharsets.UTF_8)
    def decode(bytes: Array[Byte]): Either[String, String] = Right(new String(bytes, StandardCharsets.UTF_8))
