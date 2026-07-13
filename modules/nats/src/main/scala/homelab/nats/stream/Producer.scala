package homelab.nats.stream


import homelab.common.messaging.Producer as ProducerContract
import homelab.nats.{ NatsError, Serde }
import io.nats.client.{ Connection, JetStream }
import zio.*


/**
 * A durable JetStream producer — `emit` waits for the server's `PublishAck`, so the message is persisted (or
 * the emit fails). The subject is the partition key, derived from the message by `subjectOf`; a JetStream
 * stream must already capture that subject for the publish to be accepted.
 *
 * @param jetStream the JetStream context
 * @param subjectOf derives a message's subject (its partition key)
 * @tparam A the value published
 */
final class Producer[A: Serde](jetStream: JetStream, subjectOf: A => String) extends ProducerContract[NatsError, A]:

  /**
   * Publish `value` to its derived subject, waiting for the server's `PublishAck`.
   *
   * @param value the value to publish
   * @return unit once persisted; aborts with [[NatsError.Publish]] if publishing fails (e.g. no stream
   *         captures the subject)
   */
  override def emit(value: A): IO[NatsError, Unit] =
    ZIO
      .attemptBlocking(jetStream.publish(subjectOf(value), Serde[A].encode(value)))
      .mapError(NatsError.Publish(_))
      .unit


object Producer:

  /**
   * Build a durable JetStream producer keying each message onto a subject via `subjectOf`.
   *
   * @param connection the live connection
   * @param subjectOf  derives a message's subject
   * @tparam A the value published
   * @return the producer; aborts with [[NatsError.Connect]] if the JetStream context can't be obtained
   */
  def make[A: Serde](connection: Connection)(subjectOf: A => String): IO[NatsError, ProducerContract[NatsError, A]] =
    ZIO
      .attemptBlocking(connection.jetStream())
      .mapError(NatsError.Connect(_))
      .map(jetStream => new Producer(jetStream, subjectOf))
