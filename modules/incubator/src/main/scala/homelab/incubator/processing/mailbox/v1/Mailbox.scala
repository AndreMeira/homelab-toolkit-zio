package homelab.incubator.processing.mailbox.v1


import homelab.common.error.ApplicationError
import homelab.common.messaging.{ Consumer, Producer }
import homelab.common.processing.Processor
import homelab.common.store.KeyValueStore
import homelab.incubator.messaging.mailbox.{ Address, Mailbox }
import Mailbox.Serde
import zio.{ Duration, IO, Promise }

import java.nio.charset.StandardCharsets


trait Mailbox[E <: ApplicationError] extends Processor[E, Array[Byte]] {
  def expect[B: Serde](timeout: Duration): IO[E, Mailbox.Receipt[E, B]]
  def deliver[B: Serde](address: Mailbox.Address, message: B): IO[E, Unit]
}


object Mailbox {
  class Live[E <: ApplicationError](
    inbound: Consumer[E, Array[Byte]],
    outbound: Producer[E, (Mailbox.Address, Array[Byte])],
    pending: KeyValueStore[Mailbox.Address, Mailbox.Pending[E]],
  ) extends Mailbox[E | ApplicationError.AdapterError] {
    private type Signal = Promise[E, Array[Byte]]

    override def expect[B: Serde](timeout: Duration): IO[E, Receipt[E, B]] =
      ???

    override def deliver[B: Serde](address: Address, message: B): IO[E, Unit] =
      outbound.emit((address, Serde[B].encode(message)))

    /**
     * The intake this processor consumes from.
     *
     * @return the input consumer
     */
    override def input: Consumer[E, Array[Byte]] = inbound

    /**
     * Transform a single input into a single output.
     *
     * @param value the input to transform
     * @return the output; aborts with `E` on failure
     */
    override def process(value: Array[Byte]): IO[E, Unit] = ???

    private def receipt[B: Serde](
      addr: Address,
      promise: Signal,
      timeout: Duration,
    ): Receipt[E | ApplicationError.AdapterError, B] =
      new Receipt[E | ApplicationError.AdapterError, B] {
        override def address: Address                                        = addr
        override def await: IO[E | ApplicationError.AdapterError, Option[B]] =
          promise.await
            .map(bytes => Serde[B].decode(bytes).fold(_ => None, Some(_)))
            .timeout(timeout)
            .map(_.flatten)
            .flatMap(result => pending.delete(addr).as(result))
      }
  }

  case class Pending[E](promise: Promise[E, Array[Byte]], deadline: Long)

  type Address = Address.Type

  object Address:
    opaque type Type <: String = String

    /** Wrap a raw token as an address. */
    def apply(value: String): Type = value

  /**
   * The handle returned by [[Mailbox.expect]]: a serializable [[Address]] that travels on the wire, and a
   * local `await` (a promise) that the matching [[Mailbox.deliver]] completes. This asymmetry — address out,
   * promise local — is what makes the distributed case work.
   *
   * @tparam E the error `await` aborts with
   * @tparam B the awaited reply type
   */
  trait Receipt[+E, +B]:

    /** The serializable address to embed in an outgoing request. */
    def address: Address

    /**
     * Block (as a fiber) until the reply is delivered or the receipt's timeout elapses.
     *
     * @return `Some(reply)` if it arrived in time, or `None` if the timeout elapsed first (the expectation is
     *         then reaped); aborts with `E` if a delivered payload can't be decoded
     */
    def await: IO[E, Option[B]]

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
      def encode(value: String): Array[Byte] = value.getBytes(StandardCharsets.UTF_8)

      def decode(bytes: Array[Byte]): Either[String, String] = Right(new String(bytes, StandardCharsets.UTF_8))

}
