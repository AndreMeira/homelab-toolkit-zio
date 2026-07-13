package homelab.incubator.messaging.nats.v5.core


import homelab.common.messaging.Consumer as ConsumerContract
import homelab.incubator.messaging.nats.v5.FailurePolicy.{ DecodeFailurePolicy, HandlerFailurePolicy }
import homelab.incubator.messaging.nats.v5.{ NatsError, Serde }
import io.nats.client.Message
import zio.*


class Consumer[A: Serde](
  poll: CorePoll,
  onDecodeFailure: DecodeFailurePolicy,
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
    for {
      message <- poll.one
      decoded <- decode(message)
      _       <- decoded match
                   case None        => ZIO.unit
                   case Some(value) => logic(value)
    } yield ()

  private def decode(message: Message): IO[NatsError, Option[A]] =
    Serde[A].decode(message.getData) match {
      case Right(value) => ZIO.succeed(Some(value))
      case Left(error)  =>
        onDecodeFailure match
          case DecodeFailurePolicy.Surface => ZIO.fail(NatsError.Decode(error))
          case DecodeFailurePolicy.Discard => ZIO.succeed(None)
    }
}
