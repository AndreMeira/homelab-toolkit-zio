package homelab.incubator.messaging.nats.v5.core


import homelab.common.messaging.Consumer as ConsumerContract
import homelab.incubator.messaging.nats.v5.FailurePolicy.DecodeFailurePolicy
import homelab.incubator.messaging.nats.v5.{ NatsError, Serde }
import io.nats.client.Message
import zio.{ IO, ZIO }


class BatchConsumer[A: Serde](
  batchSize: Int,
  poll: CorePoll,
  onDecodeFailure: DecodeFailurePolicy
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
      _        <- decoded match
                    case Nil    => ZIO.unit
                    case values => logic(values)
    yield ()

  private def decode(messages: List[Message]): IO[NatsError, List[A]] = {
    val (errors, valid) = messages.partitionMap(message => Serde[A].decode(message.getData))
    onDecodeFailure match
      case DecodeFailurePolicy.Surface if errors.nonEmpty => ZIO.fail(NatsError.Decode(errors.head))
      case DecodeFailurePolicy.Surface                    => ZIO.succeed(valid)
      case DecodeFailurePolicy.Discard                    => ZIO.succeed(valid)
  }
}
