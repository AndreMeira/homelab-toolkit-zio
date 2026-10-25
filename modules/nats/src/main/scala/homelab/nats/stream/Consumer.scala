package homelab.nats.stream


import homelab.common.messaging.Consumer as ConsumerContract
import homelab.nats.Codec.Decoder
import homelab.nats.{ HandlerFailurePolicy, NatsError }
import io.nats.client.{ Connection, ConsumerContext, Message }
import zio.*


/**
 * A durable JetStream [[ConsumerContract]] over messages, pulled one at a time. Each message is settled
 * with '''explicit ack''': `ack` when `logic` succeeds, and on failure whatever `onFailure` dictates — `nak`
 * after `retryAfter` (redeliver), `term` (discard), or surface the error. A handler that dies is also `nak`ed
 * after `retryAfter`; one that was interrupted is `nak`ed at once. Redelivery is real, so '''handlers must be
 * idempotent'''. An optional `heartbeat` pings `inProgress()` while the handler runs so slow work isn't
 * redelivered mid-flight.
 *
 * It consumes `Message`, not a decoded value: decoding is layered on top by the `make[A]` factory. That is
 * why there is a single failure policy here — settling happens below the decoder, so a malformed payload
 * reaches this class as a failing handler and is settled the same way. See [[Consumer.make]].
 *
 * A message received and not yet settled when the consumer's scope closes is `nak`ed, so it is redelivered at
 * once. That includes one whose ack, nak or term call failed, which stays held. The scope must therefore
 * outlive the `consume` calls running on it, or a message could be handed back while its handler still works
 * on it.
 *
 * @param context the durable consumer to pull from
 * @param config  how it pulls, keeps a slow handler alive, and settles what failed
 * @param held    the messages received and not yet settled
 */
final class Consumer private (
  context: ConsumerContext,
  config: Consumer.Config,
  held: Ref[Set[Message]],
) extends ConsumerContract[NatsError, Message]:

  /**
   * Take the next message, run `logic` on it (under the heartbeat), and settle by the outcome. One call
   * processes one message; a run loop calls it repeatedly.
   *
   * Only the pull and `logic` are interruptible; settling is not. An interrupted `logic` is `nak`ed at once,
   * whatever `onFailure` says, and a `logic` that completed just as the interrupt arrived is still settled by
   * its outcome. An interrupt landing as the pull returns drops that message, which is redelivered after
   * `ackWait`.
   *
   * @param logic processes one consumed message
   * @tparam E2 the widened error, admitting `logic`'s failures
   * @return noop once the message is settled; aborts with `E2` if `logic` fails under Surface, or with
   *         [[NatsError.Ack]] if a settlement call fails
   */
  override def consume[E2 >: NatsError](logic: Message => IO[E2, Unit]): IO[E2, Unit] =
    ZIO.uninterruptibleMask: restore =>
      restore(next).flatMap: message =>
        held.update(_ + message) *>
          restore(Heartbeat.wrap(config.heartbeat, Chunk(message))(logic(message))).exit
            .flatMap(settle(message, _))

  /**
   * Settle a message by how its handler ended: by outcome when it succeeded or failed, `nak` at once when it
   * was interrupted, and `nak` after `retryAfter` when it died.
   *
   * @param message the message to settle
   * @param exit    how the handler ended
   * @tparam E2 the handler's error
   * @return noop once settled; re-raises the interruption or defect after settling, aborts with `E2` under
   *         Surface, or with [[NatsError.Ack]] if the ack/nak/term call fails
   */
  private def settle[E2](message: Message, exit: Exit[E2, Unit]): IO[NatsError | E2, Unit] =
    exit match
      case Exit.Success(_)     => handleResult(message, Right(()))
      case Exit.Failure(cause) =>
        cause.failureOption match
          case Some(error)                     => handleResult(message, Left(error))
          case None if cause.isInterruptedOnly => nack(message) *> ZIO.refailCause(cause)
          case None                            => retry(message) *> ZIO.refailCause(cause)

  /**
   * Pull the next message, asking again until one arrives.
   *
   * Nothing is delivered that was not asked for, which is where this consumer's backpressure comes from: the
   * server holds the request open for `expiry` and returns nothing if the stream is idle. The blocking call
   * is interruptible, so a closing scope does not wait out the expiry.
   *
   * @return the next message; aborts with [[NatsError.Receive]] if the pull fails
   */
  private def next: IO[NatsError, Message] =
    ZIO
      .attemptBlockingInterrupt(context.next(config.expiry))
      .mapError(NatsError.Receive(_))
      .flatMap(message => if message == null then next else ZIO.succeed(message))

  /**
   * Settle a message by its handler outcome: `ack` on success, else `onFailure`.
   *
   * @param message the message to settle
   * @param outcome the handler's result — `Right` on success, `Left` on failure
   * @tparam E2 the handler's error
   * @return noop once settled; aborts with `E2` under Surface (re-raising the handler error), or with
   *         [[NatsError.Ack]] if the ack/nak/term call fails
   */
  private def handleResult[E2](message: Message, outcome: Either[E2, Unit]): IO[NatsError | E2, Unit] =
    outcome match
      case Right(_)    => ack(message)
      case Left(error) =>
        config.onFailure match
          case HandlerFailurePolicy.Surface   => stopHolding(message) *> ZIO.fail(error)
          case HandlerFailurePolicy.Discard   => dismiss(message)
          case HandlerFailurePolicy.Redeliver => retry(message)

  /**
   * Acknowledge successful processing (`ack`).
   *
   * @param message the message to acknowledge
   * @return noop once acked; aborts with [[NatsError.Ack]] on failure
   */
  private def ack(message: Message): IO[NatsError, Unit] =
    ZIO.attemptBlocking(message.ack()).mapError(NatsError.Ack(_)) *> stopHolding(message)

  /**
   * Negatively acknowledge for redelivery (`nak`).
   *
   * @param message the message to nak
   * @return noop once naked; aborts with [[NatsError.Ack]] on failure
   */
  private def nack(message: Message): IO[NatsError, Unit] =
    ZIO.attemptBlocking(message.nak()).mapError(NatsError.Ack(_)) *> stopHolding(message)

  /**
   * Negatively acknowledge for redelivery after `retryAfter`: work that ran and did not answer backs off.
   *
   * @param message the message to nak
   * @return noop once naked; aborts with [[NatsError.Ack]] on failure
   */
  private def retry(message: Message): IO[NatsError, Unit] =
    if config.retryAfter.isZero then nack(message)
    else ZIO.attemptBlocking(message.nakWithDelay(config.retryAfter.toMillis)).mapError(NatsError.Ack(_)) *> stopHolding(message)

  /**
   * Stop holding a message whose fate is decided: settled, or deliberately left to `ackWait`. A message
   * whose settle call failed is not, so the scope's release `nak`s it again.
   *
   * @param message the message no longer held
   * @return noop
   */
  private def stopHolding(message: Message): UIO[Unit] = held.update(_ - message)

  /**
   * `nak` every message still held, so each is redelivered at once rather than after `ackWait`; the release of
   * the scope this consumer was acquired in. A `nak` that fails leaves its message to `ackWait`.
   *
   * @return noop once every `nak` has been sent, or has failed
   */
  private def handBackAll: UIO[Unit] =
    held.getAndSet(Set.empty).flatMap(ZIO.foreachDiscard(_)(message => nack(message).ignore))

  /**
   * Terminally drop the message, stopping redelivery (`term`).
   *
   * @param message the message to terminate
   * @return noop once termed; aborts with [[NatsError.Ack]] on failure
   */
  private def dismiss(message: Message): IO[NatsError, Unit] =
    ZIO.attemptBlocking(message.term()).mapError(NatsError.Ack(_)) *> stopHolding(message)


object Consumer:

  /**
   * Tuning for a JetStream consumer.
   *
   * @param ackWait       how long the server waits for an ack before redelivering
   * @param maxAckPending the server's ceiling on un-acked in-flight messages — a safety net, since a pulling
   *                      consumer only ever holds the one it asked for
   * @param heartbeat     `inProgress()` keepalive interval while the handler runs, or `None` to disable
   * @param onFailure     what to do when processing a message fails — decoding included, see [[make]]
   * @param expiry        how long the server holds an unanswered pull open before returning nothing
   * @param retryAfter    how long a message whose handler failed or died waits before it is delivered again
   */
  final case class Config(
    ackWait: Duration = 30.seconds,
    maxAckPending: Int = 256,
    heartbeat: Option[Duration] = None,
    onFailure: HandlerFailurePolicy = HandlerFailurePolicy.Redeliver,
    expiry: Duration = 30.seconds,
    retryAfter: Duration = Duration.Zero,
  )

  /**
   * Convenience: a durable message consumer on its own connection-backed subscriber. For fan-out, build a
   * [[JetStreamSubscriber]] once and use the subscriber overload.
   *
   * @param connection the live connection
   * @param stream     the (existing) stream name
   * @param durable    the durable consumer name (shared progress across restarts)
   * @param subject    the subject filter
   * @param config     ack / backpressure / heartbeat / failure tuning
   * @return the message consumer; aborts with [[NatsError.Connect]] if the consumer can't be set up
   */
  def apply(
    connection: Connection,
    stream: String,
    durable: String,
    subject: String,
    config: Config = Config(),
  ): ZIO[Scope, NatsError, ConsumerContract[NatsError, Message]] =
    apply(JetStreamSubscriber.make(connection), stream, durable, subject, config)

  /**
   * Attach a durable message consumer through an existing [[JetStreamSubscriber]]. The durable consumer is
   * created (or attached to) here; nothing is delivered until a `consume` pulls.
   *
   * @param subscriber the subscriber to attach the durable consumer through
   * @param stream     the (existing) stream name
   * @param durable    the durable consumer name
   * @param subject    the subject filter
   * @param config     ack / backpressure / heartbeat / failure tuning
   * @return the message consumer
   */
  def apply(
    subscriber: JetStreamSubscriber,
    stream: String,
    durable: String,
    subject: String,
    config: Config,
  ): ZIO[Scope, NatsError, ConsumerContract[NatsError, Message]] =
    for
      conf     = ContextConfig(stream, durable, subject, config.ackWait, config.maxAckPending)
      context <- subscriber.attach(conf)
      held    <- Ref.make(Set.empty[Message])
      consumer = new Consumer(context, config, held)
      _       <- ZIO.addFinalizer(consumer.handBackAll)
    yield consumer

  /**
   * A consumer of decoded values: the message consumer with `A`'s decoder layered over it.
   *
   * Because settling happens below the decoder, an undecodable payload is settled exactly like a failing
   * handler — `nak` under Redeliver, `term` under Discard, or surfaced (and left un-acked, so redelivered
   * after `ackWait`) under Surface.
   *
   * @param connection the live connection
   * @param stream     the (existing) stream name
   * @param durable    the durable consumer name
   * @param subject    the subject filter
   * @param config     ack / backpressure / heartbeat / failure tuning
   * @tparam A the value consumed, with a [[Decoder]] in scope
   * @return the consumer; aborts with [[NatsError.Connect]] if the consumer can't be set up
   */
  def make[A: Decoder](
    connection: Connection,
    stream: String,
    durable: String,
    subject: String,
    config: Config = Config(),
  ): ZIO[Scope, NatsError, ConsumerContract[NatsError, A]] =
    apply(connection, stream, durable, subject, config).map(_.mapZIO(decode[A]))

  /**
   * A consumer of decoded values through an existing [[JetStreamSubscriber]].
   *
   * @param subscriber the subscriber to attach the durable consumer through
   * @param stream     the (existing) stream name
   * @param durable    the durable consumer name
   * @param subject    the subject filter
   * @param config     ack / backpressure / heartbeat / failure tuning
   * @tparam A the value consumed, with a [[Decoder]] in scope
   * @return the consumer
   */
  def make[A: Decoder](
    subscriber: JetStreamSubscriber,
    stream: String,
    durable: String,
    subject: String,
    config: Config,
  ): ZIO[Scope, NatsError, ConsumerContract[NatsError, A]] =
    apply(subscriber, stream, durable, subject, config).map(_.mapZIO(decode[A]))

  /**
   * Decode a message, lifting a malformed payload into the error channel so the consumer settles it.
   *
   * @param message the received message
   * @tparam A the value decoded, with a [[Decoder]] in scope
   * @return the decoded value; aborts with [[NatsError.Decode]] if the payload is malformed
   */
  private def decode[A: Decoder](message: Message): IO[NatsError, A] =
    ZIO.fromEither(Decoder[A].decode(message)).mapError(NatsError.Decode(_))
