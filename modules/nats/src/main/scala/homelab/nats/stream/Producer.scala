package homelab.nats.stream


import homelab.common.messaging.Producer as ProducerContract
import homelab.nats.Codec.Encoder
import homelab.nats.NatsError
import io.nats.client.{ Connection, JetStream, Message }
import zio.*


/**
 * A durable JetStream producer over messages — `emit` waits for the server's `PublishAck`, so the message is
 * persisted (or the emit fails). A stream must already capture the message's subject for the publish to be
 * accepted.
 *
 * It emits `Message`, not a domain value: encoding is layered on top by the `make[A]` factory, so this class
 * has no codec.
 *
 * @param jetStream the JetStream context
 */
final class Producer(jetStream: JetStream) extends ProducerContract[NatsError, Message]:

  /**
   * Publish a message to the subject it carries, waiting for the server's `PublishAck`.
   *
   * @param value the message to publish
   * @return noop once persisted; aborts with [[NatsError.Publish]] if publishing fails (e.g. no stream
   *         captures the subject)
   */
  override def emit(value: Message): IO[NatsError, Unit] =
    ZIO
      .attemptBlocking(jetStream.publish(value))
      .mapError(NatsError.Publish(_))
      .unit


object Producer:

  /**
   * Build a durable JetStream message producer.
   *
   * @param connection the live connection
   * @return the message producer; aborts with [[NatsError.Connect]] if the JetStream context can't be
   *         obtained
   */
  def apply(connection: Connection): IO[NatsError, ProducerContract[NatsError, Message]] =
    ZIO
      .attemptBlocking(connection.jetStream())
      .mapError(NatsError.Connect(_))
      .map(jetStream => new Producer(jetStream))

  /**
   * A producer of domain values: the message producer with `A`'s encoder layered under it. The encoder
   * addresses each value to its subject, so keying stays a pure function of the value.
   *
   * @param connection the live connection
   * @tparam A the value published, with an [[Encoder]] in scope
   * @return the producer; aborts with [[NatsError.Connect]] if the JetStream context can't be obtained
   */
  def make[A: Encoder](connection: Connection): IO[NatsError, ProducerContract[NatsError, A]] =
    apply(connection).map(_.contramap(Encoder[A].encode))
