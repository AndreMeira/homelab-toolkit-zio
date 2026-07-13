package homelab.nats.stream


import homelab.common.messaging.Consumer as ConsumerContract
import homelab.nats.{ DecodeFailurePolicy, HandlerFailurePolicy, NatsError, Serde }
import io.nats.client.{ Connection, Message }
import zio.*


/**
 * A durable JetStream [[ConsumerContract]] over a [[StreamPoll]]. Each message is settled with '''explicit
 * ack''': `ack` on handler success, and on failure whatever `onHandlerFailure` dictates — `nak` (redeliver),
 * `term` (discard), or surface the error. An undecodable payload is settled per `onDecodeFailure`: Surface
 * leaves it un-acked (redelivered after `ackWait`, non-destructive) and fails `consume`; Discard `term`s it.
 * Redelivery is real, so '''handlers must be idempotent'''. An optional `heartbeat` pings `inProgress()`
 * while the handler runs so slow work isn't redelivered mid-flight.
 *
 * @param poll             the message source (subscribes lazily on first `consume`)
 * @param heartbeat        `inProgress()` keepalive interval while the handler runs, or `None` to disable
 * @param onDecodeFailure  what to do when a payload can't be decoded
 * @param onHandlerFailure what to do when the handler fails on a decoded message
 * @tparam A the value consumed
 */
final class Consumer[A: Serde](
  poll: StreamPoll,
  heartbeat: Option[Duration],
  onDecodeFailure: DecodeFailurePolicy,
  onHandlerFailure: HandlerFailurePolicy,
) extends ConsumerContract[NatsError, A]:

  /**
   * Take the next message, decode it, run `logic` (under the heartbeat), and settle. A message termed during
   * decode (Discard) is fully handled there and not settled again. One call processes one message; a run loop
   * calls it repeatedly.
   *
   * @param logic processes one consumed value
   * @tparam E2 the widened error, admitting `logic`'s failures
   * @return unit once the message is settled; aborts with [[NatsError.Decode]] under Surface on an
   *         undecodable payload, with `E2` if `logic` fails under Surface, or with [[NatsError.Ack]] if a
   *         settlement call fails
   */
  override def consume[E2 >: NatsError](logic: A => IO[E2, Unit]): IO[E2, Unit] =
    for
      message <- poll.one
      decoded <- decode(message)
      _       <- decoded.fold(ZIO.unit): value =>
                   Heartbeat
                     .wrap(heartbeat, message :: Nil)(logic(value).either)
                     .flatMap(handleResult(message, _))
    yield ()

  /**
   * Decode a message, applying `onDecodeFailure` when it can't be decoded.
   *
   * @param message the received message
   * @return `Some(value)` when decodable, or `None` when termed under Discard; aborts with
   *         [[NatsError.Decode]] under Surface, or [[NatsError.Ack]] if the `term` fails
   */
  private def decode(message: Message): IO[NatsError, Option[A]] =
    Serde[A].decode(message.getData) match
      case Right(value) => ZIO.succeed(Some(value))
      case Left(error)  =>
        onDecodeFailure match
          case DecodeFailurePolicy.Surface => ZIO.fail(NatsError.Decode(error))
          case DecodeFailurePolicy.Discard => dismiss(message).as(None)

  /**
   * Settle a decoded message by its handler outcome: `ack` on success, else `onHandlerFailure`.
   *
   * @param message the message to settle
   * @param outcome the handler's result — `Right` on success, `Left` on failure
   * @tparam E2 the handler's error
   * @return unit once settled; aborts with `E2` under Surface (re-raising the handler error), or with
   *         [[NatsError.Ack]] if the ack/nak/term call fails
   */
  private def handleResult[E2](message: Message, outcome: Either[E2, Unit]): IO[NatsError | E2, Unit] =
    outcome match
      case Right(_)    => ack(message)
      case Left(error) =>
        onHandlerFailure match
          case HandlerFailurePolicy.Surface   => ZIO.fail(error)
          case HandlerFailurePolicy.Discard   => dismiss(message)
          case HandlerFailurePolicy.Redeliver => nack(message)

  /**
   * Acknowledge successful processing (`ack`).
   *
   * @param message the message to acknowledge
   * @return unit once acked; aborts with [[NatsError.Ack]] on failure
   */
  private def ack(message: Message): IO[NatsError, Unit] =
    ZIO.attemptBlocking(message.ack()).mapError(NatsError.Ack(_))

  /**
   * Negatively acknowledge for redelivery (`nak`).
   *
   * @param message the message to nak
   * @return unit once naked; aborts with [[NatsError.Ack]] on failure
   */
  private def nack(message: Message): IO[NatsError, Unit] =
    ZIO.attemptBlocking(message.nak()).mapError(NatsError.Ack(_))

  /**
   * Terminally drop the message, stopping redelivery (`term`).
   *
   * @param message the message to terminate
   * @return unit once termed; aborts with [[NatsError.Ack]] on failure
   */
  private def dismiss(message: Message): IO[NatsError, Unit] =
    ZIO.attemptBlocking(message.term()).mapError(NatsError.Ack(_))


object Consumer:

  /**
   * Tuning for a JetStream consumer.
   *
   * @param ackWait          how long the server waits for an ack before redelivering
   * @param maxAckPending    the backpressure bound on un-acked in-flight messages
   * @param heartbeat        `inProgress()` keepalive interval while the handler runs, or `None` to disable
   * @param onDecodeFailure  what to do when a payload can't be decoded
   * @param onHandlerFailure what to do when the handler fails on a decoded message
   */
  final case class Config(
    ackWait: Duration = 30.seconds,
    maxAckPending: Int = 256,
    heartbeat: Option[Duration] = None,
    onDecodeFailure: DecodeFailurePolicy = DecodeFailurePolicy.Surface,
    onHandlerFailure: HandlerFailurePolicy = HandlerFailurePolicy.Redeliver,
  )

  /**
   * Convenience: a durable consumer on its own connection-backed subscriber. For fan-out, build a
   * [[JetStreamSubscriber]] once and use the subscriber overload.
   *
   * @param connection the live connection
   * @param stream     the (existing) stream name
   * @param durable    the durable consumer name (shared progress across restarts)
   * @param subject    the subject filter
   * @param config     ack / backpressure / heartbeat / failure tuning
   * @tparam A the value consumed
   * @return the consumer; aborts with [[NatsError.Connect]] if the consumer can't be set up
   */
  def make[A: Serde](
    connection: Connection,
    stream: String,
    durable: String,
    subject: String,
    config: Config = Config(),
  ): ZIO[Scope, NatsError, ConsumerContract[NatsError, A]] =
    make[A](JetStreamSubscriber.make(connection), stream, durable, subject, config)

  /**
   * Attach a durable consumer through an existing [[JetStreamSubscriber]]. The subscription is established
   * lazily on the first `consume`.
   *
   * @param subscriber the subscriber to attach the durable consumer through
   * @param stream     the (existing) stream name
   * @param durable    the durable consumer name
   * @param subject    the subject filter
   * @param config     ack / backpressure / heartbeat / failure tuning
   * @tparam A the value consumed
   * @return the consumer
   */
  def make[A: Serde](
    subscriber: JetStreamSubscriber,
    stream: String,
    durable: String,
    subject: String,
    config: Config,
  ): ZIO[Scope, NatsError, ConsumerContract[NatsError, A]] =
    StreamPoll
      .make(subscriber, ContextConfig(stream, durable, subject, config.ackWait, config.maxAckPending))
      .map(poll => new Consumer(poll, config.heartbeat, config.onDecodeFailure, config.onHandlerFailure))
