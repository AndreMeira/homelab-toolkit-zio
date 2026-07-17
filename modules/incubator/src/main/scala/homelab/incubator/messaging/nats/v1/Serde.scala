package homelab.incubator.messaging.nats.v1

import java.nio.charset.StandardCharsets


/**
 * The serialization seam between a domain value and the bytes on the wire. This is the codec port the
 * messaging design calls for — split by direction (`encode`/`decode`) and provided per adapter. A real
 * version would likely be backed by zio-json or zio-schema and carry headers; kept minimal here to
 * explore where the seam sits, not how it's implemented.
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
   * A UTF-8 string codec — the trivial seam used to exercise the topology end-to-end without pulling
   * in a real serialization library.
   *
   * @return a `Serde` for `String`
   */
  val utf8: Serde[String] = new Serde[String]:
    def encode(value: String): Array[Byte]                 = value.getBytes(StandardCharsets.UTF_8)
    def decode(bytes: Array[Byte]): Either[String, String] = Right(new String(bytes, StandardCharsets.UTF_8))
