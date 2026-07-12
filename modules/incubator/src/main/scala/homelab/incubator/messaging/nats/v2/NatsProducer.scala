package homelab.incubator.messaging.nats.v2


import homelab.common.messaging.Producer
import io.nats.client.Connection
import zio.*


/**
 * A [[Producer]] that publishes to NATS. The '''subject is the partition key''': `subjectOf` derives it
 * from the message itself (a pure `A => String` fixed at construction), so `emit` stays keyless.
 * Unchanged from v1 — the producer side isn't what v2 reworks.
 *
 * @param connection the live NATS connection
 * @param subjectOf  derives a message's subject (its partition key)
 * @param serde      encodes a message to wire bytes
 * @tparam A the value published
 */
final class NatsProducer[A](
  connection: Connection,
  subjectOf: A => String,
  serde: Serde[A],
) extends Producer[NatsError, A]:

  override def emit(value: A): IO[NatsError, Unit] =
    ZIO
      .attemptBlocking(connection.publish(subjectOf(value), serde.encode(value)))
      .mapError(NatsError.Publish(_))


object NatsProducer:

  /**
   * Build a producer over `connection`, keying each message onto a subject via `subjectOf`.
   *
   * @param connection the live NATS connection
   * @param serde      encodes a message to wire bytes
   * @param subjectOf  derives a message's subject (its partition key)
   * @tparam A the value published
   * @return the producer
   */
  def make[A](connection: Connection, serde: Serde[A])(subjectOf: A => String): NatsProducer[A] =
    new NatsProducer(connection, subjectOf, serde)
