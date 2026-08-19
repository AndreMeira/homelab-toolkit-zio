package homelab.nats.stream


import homelab.common.messaging.Consumer as ConsumerContract
import homelab.nats.Codec.Decoder
import homelab.nats.{ HandlerFailurePolicy, NatsError }
import io.nats.client.{ Connection, ConsumerContext, FetchConsumeOptions, Message }
import zio.*


/**
 * A durable JetStream batched [[ConsumerContract.Batched]] over messages, pulled a batch at a time. Fetches
 * up to `batchSize` messages when it is ready to work on them, runs `logic` on them (under the heartbeat),
 * and settles them
 * '''together''': `ackAll` on success, else `onFailure` (`nak` / `term` / surface). Redelivery is real, so
 * '''handlers must be idempotent'''.
 *
 * It consumes `Message`, not decoded values: decoding is layered on top by the `make[A]` factory. Settling
 * happens below the decoder and per batch, so a malformed payload reaches this class as a failing handler and
 * takes its whole batch with it. See [[BatchConsumer.make]].
 *
 * @param batchSize the maximum messages fetched per `consume`, and so the whole in-flight set
 * @param expiry    how long the server holds an unfilled fetch open before returning what it has
 * @param heartbeat `inProgress()` keepalive interval while the handler runs, or `None` to disable
 * @param context   the durable consumer to fetch from
 * @param onFailure what to do when processing a batch fails
 */
final class BatchConsumer(
  batchSize: Int,
  expiry: Duration,
  heartbeat: Option[Duration],
  context: ConsumerContext,
  onFailure: HandlerFailurePolicy,
) extends ConsumerContract.Batched[NatsError, Message]:

  /**
   * Drain a batch, run `logic` on it (under the heartbeat), and settle it. One call processes one batch; a run
   * loop calls it repeatedly.
   *
   * @param logic processes one batch of consumed messages
   * @tparam E2 the widened error, admitting `logic`'s failures
   * @return noop once the batch is settled; aborts with `E2` if `logic` fails under Surface, or with
   *         [[NatsError.Ack]] if a settlement call fails
   */
  override def consume[E2 >: NatsError](logic: List[Message] => IO[E2, Unit]): IO[E2, Unit] =
    fetch.flatMap {
      case Nil      => ZIO.unit
      case messages =>
        Heartbeat
          .wrap(heartbeat, messages)(logic(messages).either)
          .flatMap(handleResult(messages, _))
    }

  /**
   * Fetch up to `batchSize` messages, asking again until at least one arrives.
   *
   * The batch is requested when we are ready to work on it, which is what bounds the in-flight set: only
   * `batchSize` messages are ever outstanding, and their `ackWait` clocks start when we take them rather
   * than while they wait in a buffer. The blocking calls are interruptible, so a closing scope does not
   * wait out the fetch's expiry.
   *
   * @return the fetched messages (at least one); aborts with [[NatsError.Receive]] if the fetch fails
   */
  private def fetch: IO[NatsError, List[Message]] =
    ZIO
      .attemptBlockingInterrupt {
        val options  = FetchConsumeOptions.builder().maxMessages(batchSize).expiresIn(expiry.toMillis).build()
        val consumer = context.fetch(options)
        try
          Iterator
            .continually(consumer.nextMessage())
            .takeWhile(_ != null)
            .toList
        finally consumer.close()
      }
      .mapError(NatsError.Receive(_))
      .flatMap(messages => if messages.isEmpty then fetch else ZIO.succeed(messages))

  /**
   * Settle the batch by the handler outcome: `ackAll` on success, else `onFailure`.
   *
   * @param messages the messages to settle
   * @param outcome  the handler's result — `Right` on success, `Left` on failure
   * @tparam E2 the handler's error
   * @return noop once settled; aborts with `E2` under Surface (re-raising the handler error), or with
   *         [[NatsError.Ack]] if an ack/nak/term call fails
   */
  private def handleResult[E2](messages: List[Message], outcome: Either[E2, Unit]): IO[NatsError | E2, Unit] =
    outcome match
      case Right(_)    => ackAll(messages)
      case Left(error) =>
        onFailure match
          case HandlerFailurePolicy.Discard   => dismissAll(messages)
          case HandlerFailurePolicy.Redeliver => nackAll(messages)
          case HandlerFailurePolicy.Surface   => ZIO.fail(error)

  /**
   * `ack` every message.
   *
   * @param messages the messages to acknowledge
   * @return noop once all are acked; aborts with [[NatsError.Ack]] on the first failure
   */
  private def ackAll(messages: List[Message]): IO[NatsError, Unit] =
    ZIO.foreachDiscard(messages): message =>
      ZIO.attemptBlocking(message.ack()).mapError(NatsError.Ack(_))

  /**
   * `nak` every message (redeliver).
   *
   * @param messages the messages to nak
   * @return noop once all are naked; aborts with [[NatsError.Ack]] on the first failure
   */
  private def nackAll(messages: List[Message]): IO[NatsError, Unit] =
    ZIO.foreachDiscard(messages): message =>
      ZIO.attemptBlocking(message.nak()).mapError(NatsError.Ack(_))

  /**
   * `term` every message (stop redelivery).
   *
   * @param messages the messages to terminate
   * @return noop once all are termed; aborts with [[NatsError.Ack]] on the first failure
   */
  private def dismissAll(messages: List[Message]): IO[NatsError, Unit] =
    ZIO.foreachDiscard(messages): message =>
      ZIO.attemptBlocking(message.term()).mapError(NatsError.Ack(_))


object BatchConsumer:

  /**
   * Tuning for a JetStream batched consumer.
   *
   * @param batchSize     the maximum messages fetched per `consume`, and so the whole in-flight set
   * @param expiry        how long the server holds an unfilled fetch open before returning what it has
   * @param ackWait       how long the server waits for an ack before redelivering
   * @param maxAckPending the server's ceiling on un-acked in-flight messages — a safety net, since a fetching
   *                      consumer is already bounded by `batchSize`
   * @param heartbeat     `inProgress()` keepalive interval while the handler runs, or `None` to disable
   * @param onFailure     what to do when processing a batch fails — decoding included, see [[make]]
   */
  final case class Config(
    batchSize: Int = 100,
    expiry: Duration = 30.seconds,
    ackWait: Duration = 30.seconds,
    maxAckPending: Int = 256,
    heartbeat: Option[Duration] = None,
    onFailure: HandlerFailurePolicy = HandlerFailurePolicy.Redeliver,
  )

  /**
   * Convenience: a durable batched message consumer on its own connection-backed subscriber. For fan-out,
   * build a [[JetStreamSubscriber]] once and use the subscriber overload.
   *
   * @param connection the live connection
   * @param stream     the (existing) stream name
   * @param durable    the durable consumer name (shared progress across restarts)
   * @param subject    the subject filter
   * @param config     batch / ack / backpressure / heartbeat / failure tuning
   * @return the message consumer; aborts with [[NatsError.Connect]] if the consumer can't be set up
   */
  def apply(
    connection: Connection,
    stream: String,
    durable: String,
    subject: String,
    config: Config = Config(),
  ): ZIO[Scope, NatsError, ConsumerContract.Batched[NatsError, Message]] =
    apply(JetStreamSubscriber.make(connection), stream, durable, subject, config)

  /**
   * Attach a durable batched message consumer through an existing [[JetStreamSubscriber]]. The durable
   * consumer is created (or attached to) here; nothing is delivered until a `consume` fetches.
   *
   * @param subscriber the subscriber to attach the durable consumer through
   * @param stream     the (existing) stream name
   * @param durable    the durable consumer name
   * @param subject    the subject filter
   * @param config     batch / ack / backpressure / heartbeat / failure tuning
   * @return the message consumer
   */
  def apply(
    subscriber: JetStreamSubscriber,
    stream: String,
    durable: String,
    subject: String,
    config: Config,
  ): ZIO[Scope, NatsError, ConsumerContract.Batched[NatsError, Message]] =
    subscriber
      .attach(ContextConfig(stream, durable, subject, config.ackWait, config.maxAckPending))
      .map(context => new BatchConsumer(config.batchSize, config.expiry, config.heartbeat, context, config.onFailure))

  /**
   * A batched consumer of decoded values: the message consumer with `A`'s decoder layered over it.
   *
   * Because settling happens below the decoder and covers the whole batch, one undecodable payload settles
   * '''its batch-mates with it''' — `nak` under Redeliver, `term` under Discard, or surfaced (leaving the
   * batch un-acked, so redelivered after `ackWait`) under Surface. That blast radius is the price of batching;
   * the per-item [[Consumer]] confines it to the one message.
   *
   * @param connection the live connection
   * @param stream     the (existing) stream name
   * @param durable    the durable consumer name
   * @param subject    the subject filter
   * @param config     batch / ack / backpressure / heartbeat / failure tuning
   * @tparam A the value consumed, with a [[Decoder]] in scope
   * @return the consumer; aborts with [[NatsError.Connect]] if the consumer can't be set up
   */
  def make[A: Decoder](
    connection: Connection,
    stream: String,
    durable: String,
    subject: String,
    config: Config = Config(),
  ): ZIO[Scope, NatsError, ConsumerContract.Batched[NatsError, A]] =
    apply(connection, stream, durable, subject, config).map(decoded[A])

  /**
   * A batched consumer of decoded values through an existing [[JetStreamSubscriber]].
   *
   * @param subscriber the subscriber to attach the durable consumer through
   * @param stream     the (existing) stream name
   * @param durable    the durable consumer name
   * @param subject    the subject filter
   * @param config     batch / ack / backpressure / heartbeat / failure tuning
   * @tparam A the value consumed, with a [[Decoder]] in scope
   * @return the consumer
   */
  def make[A: Decoder](
    subscriber: JetStreamSubscriber,
    stream: String,
    durable: String,
    subject: String,
    config: Config,
  ): ZIO[Scope, NatsError, ConsumerContract.Batched[NatsError, A]] =
    apply(subscriber, stream, durable, subject, config).map(decoded[A])

  /**
   * Layer a decoder over a batched message consumer, keeping the batched shape that `mapZIO` erases.
   *
   * @param consumer the batched message consumer to decode for
   * @tparam A the value consumed, with a [[Decoder]] in scope
   * @return a batched consumer of decoded values
   */
  private def decoded[A: Decoder](
    consumer: ConsumerContract.Batched[NatsError, Message]
  ): ConsumerContract.Batched[NatsError, A] =
    val values = consumer.mapZIO(decode[A])
    new ConsumerContract.Batched[NatsError, A]:
      override def consume[E2 >: NatsError](logic: List[A] => IO[E2, Unit]): IO[E2, Unit] = values.consume(logic)

  /**
   * Decode a batch, lifting the first malformed payload into the error channel so the batch is settled.
   *
   * @param messages the received messages
   * @tparam A the value decoded, with a [[Decoder]] in scope
   * @return the decoded values; aborts with [[NatsError.Decode]] on the first malformed payload
   */
  private def decode[A: Decoder](messages: List[Message]): IO[NatsError, List[A]] =
    ZIO.foreach(messages)(message => ZIO.fromEither(Decoder[A].decode(message)).mapError(NatsError.Decode(_)))
