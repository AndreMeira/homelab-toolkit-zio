package homelab.incubator.messaging.nats.v4


import homelab.common.messaging.Consumer
import io.nats.client.{ Connection, Message }
import zio.*


/**
 * A Core NATS batched [[Consumer.Batched]] — ephemeral, fire-and-forget. It drains up to `batchSize`
 * messages from its bridge queue (`takeBetween`), decodes them, and delivers the batch. There is
 * '''no ack'''; undecodable messages are handled per `onDecodeFailure`: [[DecodeFailurePolicy.Surface]]
 * fails the whole `consume`, [[DecodeFailurePolicy.Discard]] drops them and delivers the rest. A
 * [[NatsSubscriber]] feeds the queue; this class only drains it.
 *
 * @param queue           the queue a [[NatsSubscriber]] offers received messages into
 * @param batchSize       the maximum messages drained per `consume`
 * @param onDecodeFailure what to do when a message in the batch can't be decoded
 * @tparam A the value consumed
 */
private[v4] final class CoreBatchedConsumer[A](
  queue: Queue[Message],
  batchSize: Int,
  onDecodeFailure: DecodeFailurePolicy,
)(using serde: Serde[A])
    extends Consumer.Batched[NatsError, A]:

  override def consume[E2 >: NatsError](logic: List[A] => IO[E2, Unit]): IO[E2, Unit] =
    queue.takeBetween(1, batchSize).flatMap: messages =>
      val (reasons, values) = messages.toList.partitionMap(message => serde.decode(message.getData))
      onDecodeFailure match
        case DecodeFailurePolicy.Surface if reasons.nonEmpty => ZIO.fail(NatsError.Decode(reasons.mkString(", ")))
        case _                                               => ZIO.when(values.nonEmpty)(logic(values)).unit


object CoreBatchedConsumer:

  /**
   * Convenience: a single ephemeral batched consumer on its own dispatcher. For many consumers sharing one
   * dispatcher, mint them from a [[NatsSubscriber]] instead.
   *
   * @param connection      the live make
   * @param subject         the subject to subscribe to (may be a wildcard)
   * @param batchSize       the maximum messages drained per `consume`
   * @param onDecodeFailure what to do when a message can't be decoded (default [[DecodeFailurePolicy.Surface]])
   * @tparam A the value consumed
   * @return the consumer; aborts with [[NatsError.Connect]] if subscribing fails
   */
  def make[A](
    connection: Connection,
    subject: String,
    batchSize: Int,
    onDecodeFailure: DecodeFailurePolicy = DecodeFailurePolicy.Surface,
  )(using Serde[A]): ZIO[Scope, NatsError, Consumer.Batched[NatsError, A]] =
    NatsSubscriber.make(connection).flatMap(_.batchedConsumer(subject, batchSize, onDecodeFailure))
