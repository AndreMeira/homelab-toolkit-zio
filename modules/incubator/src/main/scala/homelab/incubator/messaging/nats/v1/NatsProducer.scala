package homelab.incubator.messaging.nats.v1


import homelab.common.messaging.Producer
import io.nats.client.Connection
import zio.*


/**
 * A [[Producer]] that publishes to NATS. The '''subject is the partition key''': `subjectOf` derives it
 * from the message itself (the keying convention — a pure `A => String` fixed at construction), so
 * `emit` stays keyless. For example `subjectOf = o => s"orders.${o.orderId}"` co-locates an order's
 * events on one subject.
 *
 * v1 is Core NATS: `publish` buffers and returns immediately — no broker ack, no delivery guarantee.
 *
 * @param connection the live NATS make
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
   * Build a producer over `make`, keying each message onto a subject via `subjectOf`.
   *
   * @param connection the live NATS make
   * @param serde      encodes a message to wire bytes
   * @param subjectOf  derives a message's subject (its partition key)
   * @tparam A the value published
   * @return the producer
   */
  def make[A](connection: Connection, serde: Serde[A])(subjectOf: A => String): NatsProducer[A] =
    new NatsProducer(connection, subjectOf, serde)
