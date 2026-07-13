package homelab.incubator.messaging.nats.v5.core


import homelab.common.messaging.Producer as ProducerContract
import homelab.incubator.messaging.nats.v5.{ NatsError, Serde }
import io.nats.client.Connection
import zio.*


/**
 * A Core NATS producer — '''fire-and-forget''': `emit` buffers and returns, with no broker ack and no
 * delivery guarantee (a message with no live subscriber is lost). The subject is the partition key,
 * derived from the message by `subjectOf` (the keying convention — no key on `emit`).
 *
 * @param connection the live connection
 * @param subjectOf  derives a message's subject (its partition key)
 * @tparam A the value published
 */
class Producer[A: Serde](connection: Connection, subjectOf: A => String) extends ProducerContract[NatsError, A]:

  override def emit(value: A): IO[NatsError, Unit] =
    ZIO
      .attemptBlocking(connection.publish(subjectOf(value), Serde[A].encode(value)))
      .mapError(NatsError.Publish(_))


object Producer:

  /**
   * Build a Core NATS producer keying each message onto a subject via `subjectOf`.
   *
   * @param connection the live connection
   * @param subjectOf  derives a message's subject
   * @tparam A the value published
   * @return the producer
   */
  def make[A: Serde](connection: Connection)(subjectOf: A => String): ProducerContract[NatsError, A] =
    new Producer(connection, subjectOf)
