package homelab.nats.stream


import homelab.common.messaging.Consumer as ConsumerContract
import homelab.nats.{ DecodeFailurePolicy, HandlerFailurePolicy, NatsError, Serde }
import io.nats.client.{ Connection, Message }
import zio.*

import scala.util.chaining.scalaUtilChainingOps


/**
 * A durable JetStream batched [[ConsumerContract.Batched]] over a [[StreamPoll]]. Drains up to `batchSize`
 * buffered messages, decodes them, runs `logic` on the decoded batch (under the heartbeat), and settles the
 * '''decodable''' messages together: `ackAll` on success, else `onHandlerFailure` (`nak` / `term` / surface).
 * Undecodable messages are settled per `onDecodeFailure` before the handler runs — Discard `term`s them and
 * excludes them from the batch; Surface leaves the whole batch un-acked and fails `consume`. Redelivery is
 * real, so '''handlers must be idempotent'''.
 *
 * @param batchSize        the maximum messages drained per `consume`
 * @param heartbeat        `inProgress()` keepalive interval while the handler runs, or `None` to disable
 * @param poll             the message source (subscribes lazily on first `consume`)
 * @param onDecodeFailure  what to do when a payload can't be decoded
 * @param onHandlerFailure what to do when the handler fails on the batch
 * @tparam A the value consumed
 */
final class BatchConsumer[A: Serde](
  batchSize: Int,
  heartbeat: Option[Duration],
  poll: StreamPoll,
  onDecodeFailure: DecodeFailurePolicy,
  onHandlerFailure: HandlerFailurePolicy,
) extends ConsumerContract.Batched[NatsError, A]:

  /**
   * Drain a batch, decode it, run `logic` on the decoded values (under the heartbeat), and settle only those
   * messages. Undecodable messages are already terminally handled in [[decode]]. One call processes one
   * batch; a run loop calls it repeatedly.
   *
   * @param logic processes one batch of consumed values
   * @tparam E2 the widened error, admitting `logic`'s failures
   * @return unit once the batch is settled; aborts with [[NatsError.Decode]] under Surface on an undecodable
   *         payload, with `E2` if `logic` fails under Surface, or with [[NatsError.Ack]] if a settlement call
   *         fails
   */
  override def consume[E2 >: NatsError](logic: List[A] => IO[E2, Unit]): IO[E2, Unit] =
    poll.many(batchSize).flatMap(decode).flatMap {
      case Nil   => ZIO.unit // all undecodable and termed in decode (Discard) — nothing to settle
      case pairs =>
        val (validMessages, values) = pairs.unzip
        Heartbeat.wrap(heartbeat, validMessages)(logic(values).either).flatMap(handleResult(validMessages, _))
    }

  /**
   * Split the batch into decodable `(message, value)` pairs and undecodable ones, settling the undecodable
   * per `onDecodeFailure`: Discard `term`s them and returns the decodable rest; Surface fails the whole batch.
   *
   * @param messages the received messages
   * @return the decodable message/value pairs (empty if none decode); aborts with [[NatsError.Decode]] under
   *         Surface when any message is undecodable, or [[NatsError.Ack]] if a `term` fails
   */
  private def decode(messages: List[Message]): IO[NatsError, List[(Message, A)]] = {
    val (failed, valid) = messages.partitionMap: message =>
      Serde[A].decode(message.getData) match
        case Right(value) => Right(message -> value)
        case Left(error)  => Left(message -> error)

    val (invalid, errors) = failed.unzip
      .pipe((messages, errors) => messages -> errors.mkString(", "))

    onDecodeFailure match
      case DecodeFailurePolicy.Discard if invalid.nonEmpty => dismissAll(invalid) *> ZIO.succeed(valid)
      case DecodeFailurePolicy.Surface if invalid.nonEmpty => ZIO.fail(NatsError.Decode(errors))
      case _                                               => ZIO.succeed(valid)
  }

  /**
   * Settle the decodable messages by the handler outcome: `ackAll` on success, else `onHandlerFailure`.
   *
   * @param messages the decodable messages to settle
   * @param outcome  the handler's result — `Right` on success, `Left` on failure
   * @tparam E2 the handler's error
   * @return unit once settled; aborts with `E2` under Surface (re-raising the handler error), or with
   *         [[NatsError.Ack]] if an ack/nak/term call fails
   */
  private def handleResult[E2](messages: List[Message], outcome: Either[E2, Unit]): IO[NatsError | E2, Unit] =
    outcome match
      case Right(_)    => ackAll(messages)
      case Left(error) =>
        onHandlerFailure match
          case HandlerFailurePolicy.Discard   => dismissAll(messages)
          case HandlerFailurePolicy.Redeliver => nackAll(messages)
          case HandlerFailurePolicy.Surface   => ZIO.fail(error)

  /**
   * `ack` every message.
   *
   * @param messages the messages to acknowledge
   * @return unit once all are acked; aborts with [[NatsError.Ack]] on the first failure
   */
  private def ackAll(messages: List[Message]): IO[NatsError, Unit] =
    ZIO.foreachDiscard(messages)(message => ZIO.attemptBlocking(message.ack()).mapError(NatsError.Ack(_)))

  /**
   * `nak` every message (redeliver).
   *
   * @param messages the messages to nak
   * @return unit once all are naked; aborts with [[NatsError.Ack]] on the first failure
   */
  private def nackAll(messages: List[Message]): IO[NatsError, Unit] =
    ZIO.foreachDiscard(messages)(message => ZIO.attemptBlocking(message.nak()).mapError(NatsError.Ack(_)))

  /**
   * `term` every message (stop redelivery).
   *
   * @param messages the messages to terminate
   * @return unit once all are termed; aborts with [[NatsError.Ack]] on the first failure
   */
  private def dismissAll(messages: List[Message]): IO[NatsError, Unit] =
    ZIO.foreachDiscard(messages)(message => ZIO.attemptBlocking(message.term()).mapError(NatsError.Ack(_)))


object BatchConsumer:

  /**
   * Tuning for a JetStream batched consumer.
   *
   * @param batchSize        the maximum messages drained per `consume`
   * @param ackWait          how long the server waits for an ack before redelivering
   * @param maxAckPending    the backpressure bound on un-acked in-flight messages
   * @param heartbeat        `inProgress()` keepalive interval while the handler runs, or `None` to disable
   * @param onDecodeFailure  what to do when a payload can't be decoded
   * @param onHandlerFailure what to do when the handler fails on the batch
   */
  final case class Config(
    batchSize: Int = 100,
    ackWait: Duration = 30.seconds,
    maxAckPending: Int = 256,
    heartbeat: Option[Duration] = None,
    onDecodeFailure: DecodeFailurePolicy = DecodeFailurePolicy.Surface,
    onHandlerFailure: HandlerFailurePolicy = HandlerFailurePolicy.Redeliver,
  )

  /**
   * Convenience: a durable batched consumer on its own connection-backed subscriber. For fan-out, build a
   * [[JetStreamSubscriber]] once and use the subscriber overload.
   *
   * @param connection the live connection
   * @param stream     the (existing) stream name
   * @param durable    the durable consumer name (shared progress across restarts)
   * @param subject    the subject filter
   * @param config     batch / ack / backpressure / heartbeat / failure tuning
   * @tparam A the value consumed
   * @return the consumer; aborts with [[NatsError.Connect]] if the consumer can't be set up
   */
  def make[A: Serde](
    connection: Connection,
    stream: String,
    durable: String,
    subject: String,
    config: Config = Config(),
  ): ZIO[Scope, NatsError, ConsumerContract.Batched[NatsError, A]] =
    make[A](JetStreamSubscriber.make(connection), stream, durable, subject, config)

  /**
   * Attach a durable batched consumer through an existing [[JetStreamSubscriber]]. The subscription is
   * established lazily on the first `consume`.
   *
   * @param subscriber the subscriber to attach the durable consumer through
   * @param stream     the (existing) stream name
   * @param durable    the durable consumer name
   * @param subject    the subject filter
   * @param config     batch / ack / backpressure / heartbeat / failure tuning
   * @tparam A the value consumed
   * @return the consumer
   */
  def make[A: Serde](
    subscriber: JetStreamSubscriber,
    stream: String,
    durable: String,
    subject: String,
    config: Config,
  ): ZIO[Scope, NatsError, ConsumerContract.Batched[NatsError, A]] =
    StreamPoll
      .make(subscriber, ContextConfig(stream, durable, subject, config.ackWait, config.maxAckPending))
      .map(poll => new BatchConsumer(config.batchSize, config.heartbeat, poll, config.onDecodeFailure, config.onHandlerFailure))
