package homelab.incubator.messaging.nats.v5.stream


import homelab.common.messaging.Consumer as ConsumerContract
import homelab.incubator.messaging.nats.v5.FailurePolicy.{ DecodeFailurePolicy, HandlerFailurePolicy }
import homelab.incubator.messaging.nats.v5.{ NatsError, Serde }
import io.nats.client.Message
import zio.{ Duration, IO, ZIO }


class Consumer[A: Serde](
  poll: StreamPoll,
  heartbeat: Option[Duration],
  onDecodeFailure: DecodeFailurePolicy,
  onHandlerFailure: HandlerFailurePolicy,
) extends ConsumerContract[NatsError, A] {

  /**
   * Take the next value (or batch, for [[Consumer.Batched]]) and run `logic` on it, within the
   * adapter's commit boundary. One call processes one unit; a run loop calls it repeatedly.
   *
   * @param logic processes one consumed value
   * @tparam E2 the widened error, admitting `logic`'s failures
   * @return unit once the value is processed and committed; aborts with `E2` on failure
   */
  override def consume[E2 >: NatsError](logic: A => IO[E2, Unit]): IO[E2, Unit] =
    for
      message <- poll.one
      decoded <- decode(message)
      result  <- decoded match
                   case None        => ZIO.succeed(Right(()))
                   case Some(value) => StreamPoll.heartbeat(heartbeat, message :: Nil)(logic(value).either)
      _       <- handleResult(message, result)
    yield ()

  private def decode(message: Message): IO[NatsError, Option[A]] =
    Serde[A].decode(message.getData) match {
      case Right(value) => ZIO.succeed(Some(value))
      case Left(error)  =>
        onDecodeFailure match
          case DecodeFailurePolicy.Surface => ZIO.fail(NatsError.Decode(error))
          case DecodeFailurePolicy.Discard => dismiss(message).as(None)
    }

  private def handleResult[E2](message: Message, value: Either[E2, Unit]): IO[NatsError | E2, Unit] =
    value match {
      case Right(_)    => ack(message)
      case Left(error) =>
        onHandlerFailure match
          case HandlerFailurePolicy.Surface   => ZIO.fail(error)
          case HandlerFailurePolicy.Discard   => dismiss(message)
          case HandlerFailurePolicy.Redeliver => nack(message)
    }

  private def ack(message: Message): IO[NatsError, Unit] =
    ZIO.attemptBlocking(message.ack()).mapError(NatsError.Ack(_))

  private def nack(message: Message): IO[NatsError, Unit] =
    ZIO.attemptBlocking(message.nak()).mapError(NatsError.Ack(_))

  private def dismiss(message: Message): IO[NatsError, Unit] =
    ZIO.attemptBlocking(message.term()).mapError(NatsError.Ack(_))
}
