package homelab.nats.core


import homelab.common.messaging.Producer as ProducerContract
import homelab.nats.Codec.Encoder
import homelab.nats.NatsError
import io.nats.client.{ Connection, Message }
import zio.*


/**
 * A Core NATS producer over messages — '''fire-and-forget''': `emit` buffers and returns, with no broker ack
 * and no delivery guarantee (a message with no live subscriber is lost).
 *
 * It emits `Message`, not a domain value: encoding is layered on top by the `make[A]` factory, so this class
 * has no codec. The subject travels with the message, which is why `emit` carries no key.
 *
 * @param connection the live connection
 */
final class Producer(connection: Connection) extends ProducerContract[NatsError, Message]:

  /**
   * Publish a message to the subject it carries.
   *
   * @param value the message to publish
   * @return noop once buffered for send; aborts with [[NatsError.Publish]] if publishing fails
   */
  override def emit(value: Message): IO[NatsError, Unit] =
    ZIO
      .attemptBlocking(connection.publish(value))
      .mapError(NatsError.Publish(_))


object Producer:

  /**
   * Build a Core NATS message producer.
   *
   * @param connection the live connection
   * @return the message producer
   */
  def apply(connection: Connection): ProducerContract[NatsError, Message] =
    new Producer(connection)

  /**
   * A producer of domain values: the message producer with `A`'s encoder layered under it. The encoder
   * addresses each value to its subject, so keying stays a pure function of the value.
   *
   * @param connection the live connection
   * @tparam A the value published, with an [[Encoder]] in scope
   * @return the producer
   */
  def make[A: Encoder](connection: Connection): ProducerContract[NatsError, A] =
    apply(connection).contramap(Encoder[A].encode)
