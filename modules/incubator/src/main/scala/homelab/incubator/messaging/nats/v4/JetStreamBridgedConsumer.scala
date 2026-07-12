package homelab.incubator.messaging.nats.v4


import homelab.common.messaging.Consumer
import io.nats.client.api.{ AckPolicy, ConsumerConfiguration }
import io.nats.client.{ Connection, ConsumerContext, Message }
import zio.*
import zio.stream.ZStream


/**
 * A JetStream [[Consumer]] that '''bridges''': the async `consume` callback pushes messages into a ZIO
 * [[Queue]] (via `ZStream`, adapter-internal), which `consume` drains as fibers — so many consumers share
 * the connection's threads (O(1), not O(consumers)). Each message is settled with explicit ack: `ack` on
 * success, `nak` (redeliver) on handler failure, `term` (dead-letter) on an undecodable payload. The
 * local queue can stay unbounded because `maxAckPending` bounds in-flight delivery server-side.
 * '''Handlers must be idempotent''' (redelivery is real).
 *
 * @param queue the bridge queue the async delivery offers received messages into
 * @tparam A the value consumed
 */
final private[v4] class JetStreamBridgedConsumer[A: Serde](
  queue: Queue[Message],
  onDecodeFailure: OnDecodeFailure,
  onHandlerFailure: OnHandlerFailure,
) extends JetStreamConsumer[A](onDecodeFailure, onHandlerFailure):
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
   */
  final case class Config(
    ackWait: Duration = 30.seconds,
    maxAckPending: Int = 256,
    onDecodeFailure: OnDecodeFailure = OnDecodeFailure.Surface,
    onHandlerFailure: OnHandlerFailure = OnHandlerFailure.Redeliver,
  )

  /**
   * Attach a durable consumer to an existing `stream`, bridge its async delivery into a queue, and expose
   * it as a [[Consumer]]; the delivery is torn down on scope close.
   *
   * @param connection the live connection
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
      context <- consumerContext(connection, stream, durable, subject, config)
      queue   <- Queue.unbounded[Message]
      started <- Promise.make[NatsError, Unit]
      _       <- deliveries(context, started).runForeach(queue.offer).forkScoped
      _       <- started.await // don't return until delivery is wired up
    yield new JetStreamBridgedConsumer(queue, config.onDecodeFailure, config.onHandlerFailure)

  /**
   * Create (or attach to) the explicit-ack durable consumer.
   *
   * @param connection the live connection
   * @param stream     the (existing) stream name
   * @param durable    the durable consumer name
   * @param subject    the subject filter
   * @param config     ack / backpressure tuning
   * @return its context; aborts with [[NatsError.Connect]] on failure
   */
  private def consumerContext(
    connection: Connection,
    stream: String,
    durable: String,
    subject: String,
    config: Config,
  ): IO[NatsError, ConsumerContext] =
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

  /**
   * The consumer's messages as a stream: `context.consume` delivers to a `ZStream.asyncScoped`; the
   * `MessageConsumer` is stopped when the stream finalizes, and `started` completes once delivery is live.
   *
   * @param context the consumer context to attach delivery to
   * @param started completed once the async delivery is wired up
   * @return the delivered messages (adapter-internal; never surfaced)
   */
  private def deliveries(
    context: ConsumerContext,
    started: Promise[NatsError, Unit],
  ): ZStream[Any, NatsError, Message] =
    ZStream.asyncScoped[Any, NatsError, Message] { emit =>
      ZIO
        .acquireRelease(
          ZIO
            .attemptBlocking(context.consume(message => emit(ZIO.succeed(Chunk.single(message)))))
            .mapError(NatsError.Connect(_))
        )(consumer => ZIO.attemptBlocking(consumer.stop()).ignore)
        .tapError(started.fail(_))
        .zipRight(started.succeed(()))
    }
