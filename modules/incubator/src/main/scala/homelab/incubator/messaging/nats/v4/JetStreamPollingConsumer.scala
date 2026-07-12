package homelab.incubator.messaging.nats.v4


import homelab.common.messaging.Consumer
import io.nats.client.api.{ AckPolicy, ConsumerConfiguration }
import io.nats.client.{ Connection, ConsumerContext, Message }
import zio.*


/**
 * A JetStream [[Consumer]] that '''polls''': a durable pull consumer drained one message at a time by a
 * blocking `next` (interruptible, so scope close cancels it). Demand-driven and simple, but parks one
 * thread per concurrent `consume` — prefer for few subscriptions. Each message is settled with explicit
 * ack: `ack` on success, `nak` (redeliver) on handler failure, `term` (dead-letter) on an undecodable
 * payload. Redelivery is real, so '''handlers must be idempotent'''.
 *
 * @param context     the durable pull consumer
 * @param pollTimeout each blocking `next`'s wait before retrying
 * @tparam A the value consumed
 */
final private[v4] class JetStreamPollingConsumer[A: Serde](
  context: ConsumerContext,
  pollTimeout: Duration,
  onDecodeFailure: DecodeFailurePolicy,
  onHandlerFailure: HandlerFailurePolicy,
) extends JetStreamConsumer[A](onDecodeFailure, onHandlerFailure):

  override def consume[E2 >: NatsError](logic: A => IO[E2, Unit]): IO[E2, Unit] =
    receive.flatMap(message => settle(message, logic))

  /**
   * Block for the next message, retrying across `pollTimeout` expiries.
   *
   * @return the next message; aborts with [[NatsError.Connect]] on receive failure
   */
  private def receive: IO[NatsError, Message] =
    ZIO
      .attemptBlockingInterrupt(Option(context.next(pollTimeout)))
      .mapError(NatsError.Connect(_))
      .flatMap {
        case Some(message) => ZIO.succeed(message)
        case None          => receive
      }


object JetStreamPollingConsumer:

  /**
   * Tuning for a polling consumer.
   *
   * @param ackWait          how long the server waits for an ack before redelivering
   * @param maxAckPending    the backpressure bound on un-acked in-flight messages
   * @param pollTimeout      how long each blocking `next` waits before retrying
   * @param onDecodeFailure  what to do when a payload can't be decoded
   * @param onHandlerFailure what to do when the handler fails on a decoded message
   */
  final case class Config(
    ackWait: Duration = 30.seconds,
    maxAckPending: Int = 256,
    pollTimeout: Duration = 1.second,
    onDecodeFailure: DecodeFailurePolicy = DecodeFailurePolicy.Surface,
    onHandlerFailure: HandlerFailurePolicy = HandlerFailurePolicy.Redeliver,
  )

  /**
   * Attach a durable pull consumer to an existing `stream` and expose it as a [[Consumer]].
   *
   * @param connection the live connection
   * @param stream     the (existing) stream name
   * @param durable    the durable consumer name (shared progress across restarts)
   * @param subject    the subject filter
   * @param config     ack / backpressure / poll tuning
   * @tparam A the value consumed
   * @return the consumer; aborts with [[NatsError.Connect]] if the consumer can't be created
   */
  def make[A](
    connection: Connection,
    stream: String,
    durable: String,
    subject: String,
    config: Config = Config(),
  )(using Serde[A]
  ): IO[NatsError, Consumer[NatsError, A]] =
    ZIO
      .attemptBlocking {
        val configuration = ConsumerConfiguration
          .builder()
          .durable(durable)
          .filterSubject(subject)
          .ackPolicy(AckPolicy.Explicit)
          .ackWait(config.ackWait)
          .maxAckPending(config.maxAckPending.toLong)
          .build()
        connection.getStreamContext(stream).createOrUpdateConsumer(configuration)
      }
      .mapError(NatsError.Connect(_))
      .map(context =>
        new JetStreamPollingConsumer(context, config.pollTimeout, config.onDecodeFailure, config.onHandlerFailure)
      )
