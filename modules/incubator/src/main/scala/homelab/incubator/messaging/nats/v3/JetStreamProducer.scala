package homelab.incubator.messaging.nats.v3


import homelab.common.messaging.Producer
import io.nats.client.{ Connection, JetStream }
import zio.*


/**
 * A durable [[Producer]] that publishes through JetStream. Unlike Core NATS, `publish` waits for the
 * server's `PublishAck` — the message is persisted (or the emit fails). The '''subject is the partition
 * key''': `subjectOf` derives it from the message (a pure `A => String` fixed at construction), so `emit`
 * stays keyless.
 *
 * @param jetStream the JetStream context
 * @param subjectOf derives a message's subject (its partition key)
 * @tparam A the value published
 */
final class JetStreamProducer[A](
  jetStream: JetStream,
  subjectOf: A => String,
)(using serde: Serde[A])
    extends Producer[NatsError, A]:

  override def emit(value: A): IO[NatsError, Unit] =
    ZIO
      .attemptBlocking(jetStream.publish(subjectOf(value), serde.encode(value)))
      .mapError(NatsError.Publish(_))
      .unit


object JetStreamProducer:

  /**
   * Build a durable producer over `connection`, keying each message onto a subject via `subjectOf`.
   *
   * @param connection the live connection
   * @param subjectOf  derives a message's subject (its partition key)
   * @tparam A the value published
   * @return the producer; aborts with [[NatsError.Connect]] if the JetStream context can't be obtained
   */
  def make[A](connection: Connection)(subjectOf: A => String)(using Serde[A]): IO[NatsError, JetStreamProducer[A]] =
    ZIO
      .attemptBlocking(connection.jetStream())
      .mapError(NatsError.Connect(_))
      .map(jetStream => new JetStreamProducer(jetStream, subjectOf))
