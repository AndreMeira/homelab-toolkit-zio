package homelab.incubator.messaging.nats.v4


import homelab.common.messaging.Producer
import io.nats.client.Connection
import zio.*


/**
 * A Core NATS [[Producer]] — '''fire-and-forget''': `publish` buffers and returns, with no broker ack and
 * no delivery guarantee (a message with no live subscriber is lost). The subject is the partition key,
 * derived from the message by `subjectOf`.
 *
 * @param connection the live connection
 * @param subjectOf  derives a message's subject (its partition key)
 * @tparam A the value published
 */
private[v4] final class CoreProducer[A](connection: Connection, subjectOf: A => String)(using serde: Serde[A])
    extends Producer[NatsError, A]:

  override def emit(value: A): IO[NatsError, Unit] =
    ZIO
      .attemptBlocking(connection.publish(subjectOf(value), serde.encode(value)))
      .mapError(NatsError.Publish(_))


object CoreProducer:

  /**
   * Build a Core NATS producer keying each message onto a subject via `subjectOf`.
   *
   * @param connection the live connection
   * @param subjectOf  derives a message's subject
   * @tparam A the value published
   * @return the producer
   */
  def make[A](connection: Connection)(subjectOf: A => String)(using Serde[A]): Producer[NatsError, A] =
    new CoreProducer(connection, subjectOf)
