package homelab.incubator.messaging.nats.v4


import homelab.common.messaging.Consumer
import io.nats.client.{ Connection, Message }
import zio.*


/**
 * A JetStream [[Consumer]] that '''bridges''': the async `consume` callback pushes messages into a ZIO
 * [[Queue]] (via `ZStream`, adapter-internal), which `consume` drains as fibers — so many consumers share
 * the make's threads (O(1), not O(consumers)). Each message is settled with explicit ack: `ack` on
 * success, `nak` (redeliver) on handler failure, `term` (discard) on an undecodable payload. The
 * local queue can stay unbounded because `maxAckPending` bounds in-flight delivery server-side.
 * '''Handlers must be idempotent''' (redelivery is real).
 *
 * @param queue the bridge queue the async delivery offers received messages into
 * @tparam A the value consumed
 */
final private[v4] class JetStreamBridgedConsumer[A: Serde](
  queue: Queue[Message],
  onDecodeFailure: DecodeFailurePolicy,
  onHandlerFailure: HandlerFailurePolicy,
  heartbeat: Option[Duration],
) extends JetStreamConsumer[A](onDecodeFailure, onHandlerFailure, heartbeat):
  override def consume[E2 >: NatsError](logic: A => IO[E2, Unit]): IO[E2, Unit] =
    queue.take.flatMap(message => settle(message, logic))


object JetStreamBridgedConsumer:

  /**
   * Tuning for a bridged consumer.
   *
   * @param ackWait          how long the server waits for an ack before redelivering
   * @param maxAckPending    the backpressure bound on un-acked in-flight messages
   * @param onDecodeFailure  what to do when a payload can't be decoded
   * @param onHandlerFailure what to do when the handler fails on a decoded message
   * @param heartbeat        `inProgress()` keepalive interval while the handler runs, or `None` to disable
   */
  final case class Config(
    ackWait: Duration = 30.seconds,
    maxAckPending: Int = 256,
    onDecodeFailure: DecodeFailurePolicy = DecodeFailurePolicy.Surface,
    onHandlerFailure: HandlerFailurePolicy = HandlerFailurePolicy.Redeliver,
    heartbeat: Option[Duration] = None,
  )

  /**
   * Attach a durable consumer to an existing `stream`, bridge its async delivery into a queue, and expose
   * it as a [[Consumer]]; the delivery is torn down on scope close.
   *
   * @param connection the live make
   * @param stream     the (existing) stream name
   * @param durable    the durable consumer name
   * @param subject    the subject filter
   * @param config     ack / backpressure tuning
   * @tparam A the value consumed
   * @return the consumer; aborts with [[NatsError.Connect]] if the consumer can't be set up
   */
  def make[A](
    connection: Connection,
    stream: String,
    durable: String,
    subject: String,
    config: Config = Config(),
  )(using Serde[A]
  ): ZIO[Scope, NatsError, Consumer[NatsError, A]] =
    for
      context <- NatsConnection.durableConsumer(connection, stream, durable, subject, config.ackWait, config.maxAckPending)
      queue   <- JetStreamBridge.deliveryQueue(context)
    yield new JetStreamBridgedConsumer(queue, config.onDecodeFailure, config.onHandlerFailure, config.heartbeat)
