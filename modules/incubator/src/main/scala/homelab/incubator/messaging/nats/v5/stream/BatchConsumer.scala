package homelab.incubator.messaging.nats.v5.stream


import homelab.common.messaging.Consumer as ConsumerContract
import homelab.incubator.messaging.nats.v5.FailurePolicy.{ DecodeFailurePolicy, HandlerFailurePolicy }
import homelab.incubator.messaging.nats.v5.{ NatsError, Serde }
import io.nats.client.Message
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
   * adapter's commit boundary. One call processes one unit; a run loop calls it repeatedly.
   *
   * @param logic processes one consumed value
   * @tparam E2 the widened error, admitting `logic`'s failures
   * @return unit once the value is processed and committed; aborts with `E2` on failure
   */
  override def consume[E2 >: NatsError](logic: List[A] => IO[E2, Unit]): IO[E2, Unit] =
    for
      messages <- poll.many(batchSize)
      decoded  <- decode(messages)
      result   <- decoded match
                    case Nil    => ZIO.succeed(Right(()))
                    case values => StreamPoll.heartbeat(heartbeat, messages)(logic(values).either)
      _        <- handleResult(messages, result)
    yield ()

  private def decode(messages: List[Message]): IO[NatsError, List[A]] = {
    val (failed, valid) = messages.partitionMap: message =>
      Serde[A].decode(message.getData).left.map(error => message -> error)

    val (invalid, errors) = failed.unzip
      .pipe((messages, errors) => messages -> errors.mkString(", "))

    onDecodeFailure match
      case DecodeFailurePolicy.Discard if invalid.nonEmpty => dismissAll(invalid) *> ZIO.succeed(valid)
      case DecodeFailurePolicy.Surface if invalid.nonEmpty => ZIO.fail(NatsError.Decode(errors))
      case DecodeFailurePolicy.Surface                     => ZIO.succeed(valid)
      case DecodeFailurePolicy.Discard                     => ZIO.succeed(valid)
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
