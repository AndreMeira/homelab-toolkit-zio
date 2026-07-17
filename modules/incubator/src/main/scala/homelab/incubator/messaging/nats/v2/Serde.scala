package homelab.incubator.messaging.nats.v2

import java.nio.charset.StandardCharsets


/**
 * The serialization seam between a domain value and the wire bytes — split by direction, adapter-provided.
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
   * A UTF-8 string codec used to exercise the topology without a real serialization library.
   *
   * @return a `Serde` for `String`
   */
  val utf8: Serde[String] = new Serde[String]:
    def encode(value: String): Array[Byte]                 = value.getBytes(StandardCharsets.UTF_8)
    def decode(bytes: Array[Byte]): Either[String, String] = Right(new String(bytes, StandardCharsets.UTF_8))
