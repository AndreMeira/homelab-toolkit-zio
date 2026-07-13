package homelab.incubator.messaging.nats.v4


import homelab.common.messaging.Producer
import io.nats.client.{ Connection, JetStream }
import zio.*


/**
 * A durable JetStream [[Producer]] — `publish` waits for the server's `PublishAck`, so the message is
 * persisted (or the emit fails). The subject is the partition key, derived from the message by
 * `subjectOf`; a JetStream stream must capture that subject (see [[NatsConnection.stream]]).
 *
 * @param jetStream the JetStream context
 * @param subjectOf derives a message's subject (its partition key)
 * @tparam A the value published
 */
private[v4] final class JetStreamProducer[A](jetStream: JetStream, subjectOf: A => String)(using serde: Serde[A])
    extends Producer[NatsError, A]:

  override def emit(value: A): IO[NatsError, Unit] =
    ZIO
      .attemptBlocking(jetStream.publish(subjectOf(value), serde.encode(value)))
      .mapError(NatsError.Publish(_))
      .unit


object JetStreamProducer:

  /**
   * Build a durable producer keying each message onto a subject via `subjectOf`.
   *
   * @param connection the live make
   * @param subjectOf  derives a message's subject
   * @tparam A the value published
   * @return the producer; aborts with [[NatsError.Connect]] if the JetStream context can't be obtained
   */
  def make[A](connection: Connection)(subjectOf: A => String)(using Serde[A]): IO[NatsError, Producer[NatsError, A]] =
    ZIO
      .attemptBlocking(connection.jetStream())
      .mapError(NatsError.Connect(_))
      .map(jetStream => new JetStreamProducer(jetStream, subjectOf))
