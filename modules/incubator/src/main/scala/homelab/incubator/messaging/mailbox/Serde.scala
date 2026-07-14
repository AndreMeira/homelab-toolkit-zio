package homelab.incubator.messaging.mailbox


import java.nio.charset.StandardCharsets


/**
 * Minimal codec seam for the mailbox prototype — the same shape as the NATS adapter's `Serde`. A real
 * promotion would share one `Serde` across Pub/Sub and Mailbox rather than duplicate it here.
 *
 * @tparam A the domain value carried
 */
trait Serde[A]:

  /** Encode a value to its wire bytes. */
  def encode(value: A): Array[Byte]

  /** Decode wire bytes back into a value, or a `Left` reason if malformed. */
  def decode(bytes: Array[Byte]): Either[String, A]


object Serde:

  /** Summon the `Serde[A]` in scope. */
  def apply[A](using serde: Serde[A]): Serde[A] = serde

  /** A UTF-8 string codec — passed explicitly (`using Serde.utf8`), not an ambient given. */
  val utf8: Serde[String] = new Serde[String]:
    def encode(value: String): Array[Byte]                 = value.getBytes(StandardCharsets.UTF_8)
    def decode(bytes: Array[Byte]): Either[String, String] = Right(new String(bytes, StandardCharsets.UTF_8))
