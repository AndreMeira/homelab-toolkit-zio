package homelab.incubator.messaging.nats.v5.stream


import homelab.common.messaging.Consumer as ConsumerContract
import homelab.incubator.messaging.nats.v5.FailurePolicy.{ DecodeFailurePolicy, HandlerFailurePolicy }
import homelab.incubator.messaging.nats.v5.config.ContextConfig
import homelab.incubator.messaging.nats.v5.{ NatsError, Serde }
import io.nats.client.{ Connection, Message }
import zio.*

import scala.util.chaining.scalaUtilChainingOps


class BatchConsumer[A: Serde](
  batchSize: Int,
  heartbeat: Option[Duration],
  poll: StreamPoll,
  onDecodeFailure: DecodeFailurePolicy,
  onHandlerFailure: HandlerFailurePolicy,
) extends ConsumerContract.Batched[NatsError, A] {

  /**
   * Take the next value (or batch, for [[Consumer.Batched]]) and run `logic` on it, within the
   * adapter's commit boundary. One call processes one noop; a run loop calls it repeatedly.
   *
   * @param logic processes one consumed value
   * @tparam E2 the widened error, admitting `logic`'s failures
   * @return noop once the value is processed and committed; aborts with `E2` on failure
   */
  override def consume[E2 >: NatsError](logic: List[A] => IO[E2, Unit]): IO[E2, Unit] =
    for
      messages <- poll.many(batchSize)
      decoded  <- decode(messages)
      _        <- decoded match
                    case Nil   => ZIO.unit // all undecodable and termed in decode (Discard) — nothing to settle
                    case pairs =>
                      val (validMessages, values) = pairs.unzip
                      StreamPoll.heartbeat(heartbeat, validMessages)(logic(values).either).flatMap(handleResult(validMessages, _))
    yield ()

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

  private def handleResult[E2](messages: List[Message], result: Either[E2, Unit]): IO[NatsError | E2, Unit] =
    result match {
      case Right(_)    => ackAll(messages)
      case Left(error) =>
        onHandlerFailure match
          case HandlerFailurePolicy.Discard   => dismissAll(messages)
          case HandlerFailurePolicy.Redeliver => nackAll(messages)
          case HandlerFailurePolicy.Surface   => ZIO.fail(error)
    }

  private def ackAll(messages: List[Message]): IO[NatsError, Unit] =
    ZIO.foreachDiscard(messages): message =>
      ZIO.attemptBlocking(message.ack()).mapError(NatsError.Ack(_))

  private def nackAll(messages: List[Message]): IO[NatsError, Unit] =
    ZIO.foreachDiscard(messages): message =>
      ZIO.attemptBlocking(message.nak()).mapError(NatsError.Ack(_))

  private def dismissAll(messages: List[Message]): IO[NatsError, Unit] =
    ZIO.foreachDiscard(messages): message =>
      ZIO.attemptBlocking(message.term()).mapError(NatsError.Ack(_))
}


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
    make[A](JetStreamSubscriber(connection), stream, durable, subject, config)

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
