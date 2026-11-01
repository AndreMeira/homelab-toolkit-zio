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
 * '''together''': `ackAll` on success, else `onFailure` (`nak` after `retryAfter` / `term` / surface).
 * A handler that dies also has its batch `nak`ed after `retryAfter`; one that was interrupted, at once.
 * Redelivery is real, so '''handlers must be idempotent'''.
 *
 * It consumes `Message`, not decoded values: decoding is layered on top by the `make[A]` factory. Settling
 * happens below the decoder and per batch, so a malformed payload reaches this class as a failing handler and
 * takes its whole batch with it. See [[BatchConsumer.make]].
 *
 * Messages received and not yet settled when the consumer's scope closes are `nak`ed, so they are redelivered
 * at once. That includes those whose ack, nak or term call failed, and those a failed call never reached, which
 * stay held. The scope must therefore outlive the `consume` calls running on it, or a batch could be handed
 * back while its handler still works on it.
 *
 * @param context the durable consumer to fetch from
 * @param config  how much it fetches and for how long, keeps a slow handler alive, and settles what failed
 * @param held    the messages received and not yet settled
 */
final class BatchConsumer private (
  context: ConsumerContext,
  config: BatchConsumer.Config,
  held: Ref[Set[Message]],
) extends ConsumerContract.Batched[NatsError, Message]:

  private val options = FetchConsumeOptions
    .builder()
    .maxMessages(config.batchSize)
    .expiresIn(config.expiry.toMillis)
    .build()

  /**
   * Fetch once and, if anything came back, run `logic` on the batch (under the heartbeat) and settle it. One
   * call makes one fetch and processes at most one batch, returning without calling `logic` when the fetch
   * expired empty; a run loop calls it repeatedly.
   *
   * Only the fetch and `logic` are interruptible; settling is not. An interrupted `logic` has its whole batch
   * `nak`ed at once, whatever `onFailure` says, and a `logic` that completed just as the interrupt arrived is
   * still settled by its outcome. An interrupt landing as the fetch returns drops that batch, which is
   * redelivered after `ackWait`.
   *
   * @param logic processes one batch of consumed messages
   * @tparam E2 the widened error, admitting `logic`'s failures
   * @return noop once the batch is settled, or once the fetch expired empty; aborts with `E2` if `logic`
   *         fails under Surface, with [[NatsError.Receive]] if the fetch fails, or with [[NatsError.Ack]] if
   *         a settlement call fails
   */
  override def consume[E2 >: NatsError](logic: Chunk[Message] => IO[E2, Unit]): IO[E2, Unit] =
    ZIO.uninterruptibleMask: restore =>
      restore(fetch).flatMap { messages =>
        (held.update(_ ++ messages) *>
          restore(Heartbeat.wrap(config.heartbeat, messages)(logic(messages))).exit
            .flatMap(settle(messages, _)))
          .unless(messages.isEmpty)
          .unit
      }

  /**
   * Settle a batch by how its handler ended: by outcome when it succeeded or failed, `nak` at once when it
   * was interrupted, and `nak` after `retryAfter` when it died.
   *
   * @param messages the messages to settle
   * @param exit     how the handler ended
   * @tparam E2 the handler's error
   * @return noop once settled; re-raises the interruption or defect after settling, aborts with `E2` under
   *         Surface, or with [[NatsError.Ack]] if an ack/nak/term call fails
   */
  private def settle[E2](messages: Chunk[Message], exit: Exit[E2, Unit]): IO[NatsError | E2, Unit] =
    exit match
      case Exit.Success(_)     => handleResult(messages, Right(()))
      case Exit.Failure(cause) =>
        cause.failureOption match
          case Some(error)                     => handleResult(messages, Left(error))
          case None if cause.isInterruptedOnly => nackAll(messages) *> ZIO.refailCause(cause)
          case None                            => retryAll(messages) *> ZIO.refailCause(cause)

  /**
   * Fetch once: up to `batchSize` messages, or none if the stream stayed idle for `expiry`.
   *
   * The batch is requested when we are ready to work on it, which is what bounds the in-flight set: only
   * `batchSize` messages are ever outstanding, and their `ackWait` clocks start when we take them rather
   * than while they wait in a buffer. It is not asked again here, so a caller that stops calling `consume`
   * stops fetching within `expiry`. The blocking calls are interruptible, so a closing scope does not wait
   * out the fetch's expiry.
   *
   * The fetch consumer is opened, drained and closed within the one blocking call, so it outlives nothing
   * and belongs to no scope. `nextMessage` answers an `InterruptedException`, which is what carries an
   * interrupt through to the close.
   *
   * @return the fetched messages, empty when the fetch expired with nothing; aborts with
   *         [[NatsError.Receive]] if the fetch fails
   */
  private def fetch: IO[NatsError, Chunk[Message]] =
    ZIO
      .attemptBlockingInterrupt:
        val consumer = context.fetch(options)
        val data     = Iterator.continually(consumer.nextMessage()).takeWhile(_ != null)
        try Chunk.fromIterator(data)
        finally consumer.close()
      .mapError(NatsError.Receive(_))

  /**
   * Settle the batch by the handler outcome: `ackAll` on success, else `onFailure`.
   *
   * @param messages the messages to settle
   * @param outcome  the handler's result — `Right` on success, `Left` on failure
   * @tparam E2 the handler's error
   * @return noop once settled; aborts with `E2` under Surface (re-raising the handler error), or with
   *         [[NatsError.Ack]] if an ack/nak/term call fails
   */
  private def handleResult[E2](messages: Chunk[Message], outcome: Either[E2, Unit]): IO[NatsError | E2, Unit] =
    outcome match
      case Right(_)    => ackAll(messages)
      case Left(error) =>
        config.onFailure match
          case HandlerFailurePolicy.Discard   => dismissAll(messages)
          case HandlerFailurePolicy.Redeliver => retryAll(messages)
          case HandlerFailurePolicy.Surface   => stopHolding(messages) *> ZIO.fail(error)

  /**
   * `ack` every message.
   *
   * @param messages the messages to acknowledge
   * @return noop once all are acked; aborts with [[NatsError.Ack]] on the first failure
   */
  private def ackAll(messages: Chunk[Message]): IO[NatsError, Unit] =
    ZIO.foreachDiscard(messages): message =>
      ZIO.attemptBlocking(message.ack()).mapError(NatsError.Ack(_)) *> stopHolding(Chunk(message))

  /**
   * `nak` every message (redeliver).
   *
   * @param messages the messages to nak
   * @return noop once all are naked; aborts with [[NatsError.Ack]] on the first failure
   */
  private def nackAll(messages: Chunk[Message]): IO[NatsError, Unit] =
    ZIO.foreachDiscard(messages): message =>
      ZIO.attemptBlocking(message.nak()).mapError(NatsError.Ack(_)) *> stopHolding(Chunk(message))

  /**
   * `nak` every message for redelivery after `retryAfter`: work that ran and did not answer backs off.
   *
   * @param messages the messages to nak
   * @return noop once all are naked; aborts with [[NatsError.Ack]] on the first failure
   */
  private def retryAll(messages: Chunk[Message]): IO[NatsError, Unit] =
    if config.retryAfter.isZero then nackAll(messages)
    else
      ZIO.foreachDiscard(messages): message =>
        ZIO.attemptBlocking(message.nakWithDelay(config.retryAfter.toMillis)).mapError(NatsError.Ack(_)) *>
          stopHolding(Chunk(message))

  /**
   * Stop holding messages whose fate is decided: settled, or deliberately left to `ackWait`. A message whose
   * settle call failed, or that a failed call never reached, is not, so the scope's release `nak`s it again.
   *
   * @param messages the messages no longer held
   * @return noop
   */
  private def stopHolding(messages: Chunk[Message]): UIO[Unit] = held.update(_ -- messages)

  /**
   * `nak` every message still held, so each is redelivered at once rather than after `ackWait`; the release of
   * the scope this consumer was acquired in. A `nak` that fails leaves its message to `ackWait`.
   *
   * @return noop once every `nak` has been sent, or has failed
   */
  private def handBackAll: UIO[Unit] =
    held.getAndSet(Set.empty).flatMap(ZIO.foreachDiscard(_)(message => nackAll(Chunk(message)).ignore))

  /**
   * `term` every message (stop redelivery).
   *
   * @param messages the messages to terminate
   * @return noop once all are termed; aborts with [[NatsError.Ack]] on the first failure
   */
  private def dismissAll(messages: Chunk[Message]): IO[NatsError, Unit] =
    ZIO.foreachDiscard(messages): message =>
      ZIO.attemptBlocking(message.term()).mapError(NatsError.Ack(_)) *> stopHolding(Chunk(message))


object BatchConsumer:

  /**
   * Tuning for a JetStream batched consumer.
   *
   * @param batchSize     the maximum messages fetched per `consume`, and so the whole in-flight set
   * @param expiry        how long the server holds an unfilled fetch open before returning what it has; also
   *                      the longest a draining shutdown waits for an open fetch, so keep it within the
   *                      deployment's shutdown grace period
   * @param ackWait       how long the server waits for an ack before redelivering
   * @param maxAckPending the server's ceiling on un-acked in-flight messages — a safety net, since a fetching
   *                      consumer is already bounded by `batchSize`
   * @param heartbeat     `inProgress()` keepalive interval while the handler runs, or `None` to disable
   * @param onFailure     what to do when processing a batch fails — decoding included, see [[make]]
   * @param retryAfter    how long a batch whose handler failed or died waits before it is delivered again
   */
  final case class Config(
    batchSize: Int = 100,
    expiry: Duration = 30.seconds,
    ackWait: Duration = 30.seconds,
    maxAckPending: Int = 256,
    heartbeat: Option[Duration] = None,
    onFailure: HandlerFailurePolicy = HandlerFailurePolicy.Redeliver,
    retryAfter: Duration = Duration.Zero,
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
    for
      conf     = ContextConfig(stream, durable, subject, config.ackWait, config.maxAckPending)
      context <- subscriber.attach(conf)
      held    <- Ref.make(Set.empty[Message])
      consumer = new BatchConsumer(context, config, held)
      _       <- ZIO.addFinalizer(consumer.handBackAll)
    yield consumer

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
    consumer.mapZIO(decode[A])

  /**
   * Decode a batch, lifting the first malformed payload into the error channel so the batch is settled.
   *
   * @param messages the received messages
   * @tparam A the value decoded, with a [[Decoder]] in scope
   * @return the decoded values; aborts with [[NatsError.Decode]] on the first malformed payload
   */
  private def decode[A: Decoder](messages: Chunk[Message]): IO[NatsError, Chunk[A]] =
    ZIO.foreach(messages)(message => ZIO.fromEither(Decoder[A].decode(message)).mapError(NatsError.Decode(_)))
