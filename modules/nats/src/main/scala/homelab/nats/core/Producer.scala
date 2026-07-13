package homelab.nats.core


import homelab.common.messaging.Producer as ProducerContract
import homelab.nats.{ NatsError, Serde }
import io.nats.client.Connection
import zio.*


/**
 * A Core NATS producer — '''fire-and-forget''': `emit` buffers and returns, with no broker ack and no
 * delivery guarantee (a message with no live subscriber is lost). The subject is the partition key, derived
 * from the message by `subjectOf` (the keying convention — no key on `emit`).
 *
 * @param connection the live connection
 * @param subjectOf  derives a message's subject (its partition key)
 * @tparam A the value published
 */
final class Producer[A: Serde](connection: Connection, subjectOf: A => String) extends ProducerContract[NatsError, A]:

  /**
   * Publish `value` to its derived subject.
   *
   * @param value the value to publish
   * @return unit once buffered for send; aborts with [[NatsError.Publish]] if publishing fails
   */
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
