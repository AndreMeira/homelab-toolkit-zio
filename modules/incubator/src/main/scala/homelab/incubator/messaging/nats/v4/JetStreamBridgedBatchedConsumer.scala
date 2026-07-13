package homelab.incubator.messaging.nats.v4


import homelab.common.messaging.Consumer
import io.nats.client.{ Connection, Message }
import zio.*


/**
 * A JetStream batched [[Consumer.Batched]] that '''bridges''': the async `consume` callback pushes
 * messages into a ZIO [[Queue]], and each `consume` drains up to `batchSize` of them (`takeBetween`) and
 * settles the batch with explicit ack. Fiber-based, O(1) threads. '''Handlers must be idempotent'''.
 *
 * Unlike [[JetStreamPollingBatchedConsumer]], `takeBetween` returns '''immediately''' with whatever is
 * already buffered (1..`batchSize`) — so this is low-latency and batches opportunistically, where the
 * polling variant waits up to `maxWait` to fill the batch (higher latency, better packing under load).
 *
 * @param queue     the bridge queue the async delivery offers received messages into
 * @param batchSize the maximum messages drained per `consume`
 * @tparam A the value consumed
 */
private[v4] final class JetStreamBridgedBatchedConsumer[A: Serde](
  queue: Queue[Message],
  batchSize: Int,
  onDecodeFailure: DecodeFailurePolicy,
  onHandlerFailure: HandlerFailurePolicy,
  heartbeat: Option[Duration],
) extends JetStreamBatchedConsumer[A](onDecodeFailure, onHandlerFailure, heartbeat):

  override def consume[E2 >: NatsError](logic: List[A] => IO[E2, Unit]): IO[E2, Unit] =
    queue.takeBetween(1, batchSize).flatMap(messages => settleBatch(messages.toList, logic))


object JetStreamBridgedBatchedConsumer:

  /**
   * Tuning for a batched bridged consumer.
   *
   * @param batchSize        the maximum messages drained per `consume`
   * @param ackWait          how long the server waits for an ack before redelivering
   * @param maxAckPending    the backpressure bound on un-acked in-flight messages
   * @param onDecodeFailure  what to do when a message can't be decoded
   * @param onHandlerFailure what to do when the handler fails on the batch
   * @param heartbeat        `inProgress()` keepalive interval while the handler runs, or `None` to disable
   */
  final case class Config(
    batchSize: Int = 100,
    ackWait: Duration = 30.seconds,
    maxAckPending: Int = 256,
    onDecodeFailure: DecodeFailurePolicy = DecodeFailurePolicy.Surface,
    onHandlerFailure: HandlerFailurePolicy = HandlerFailurePolicy.Redeliver,
    heartbeat: Option[Duration] = None,
  )

  /**
   * Attach a durable consumer to an existing `stream`, bridge its async delivery into a queue, and expose
   * it as a [[Consumer.Batched]]; the delivery is torn down on scope close.
   *
   * @param connection the live make
   * @param stream     the (existing) stream name
   * @param durable    the durable consumer name
   * @param subject    the subject filter
   * @param config     batch / ack / backpressure tuning
   * @tparam A the value consumed
   * @return the consumer; aborts with [[NatsError.Connect]] if the consumer can't be set up
   */
  def make[A](
    connection: Connection,
    stream: String,
    durable: String,
    subject: String,
    config: Config = Config(),
  )(using Serde[A]): ZIO[Scope, NatsError, Consumer.Batched[NatsError, A]] =
    for
      context <- NatsConnection.durableConsumer(connection, stream, durable, subject, config.ackWait, config.maxAckPending)
      queue   <- JetStreamBridge.deliveryQueue(context)
    yield new JetStreamBridgedBatchedConsumer(queue, config.batchSize, config.onDecodeFailure, config.onHandlerFailure, config.heartbeat)
