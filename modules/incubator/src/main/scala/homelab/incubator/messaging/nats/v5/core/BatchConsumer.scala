package homelab.incubator.messaging.nats.v5.core


import homelab.common.messaging.Consumer as ConsumerContract
import homelab.incubator.messaging.nats.v5.FailurePolicy.DecodeFailurePolicy
import homelab.incubator.messaging.nats.v5.{ NatsError, Serde }
import io.nats.client.{ Connection, Message }
import zio.*


class BatchConsumer[A: Serde](
  batchSize: Int,
  poll: CorePoll,
  onDecodeFailure: DecodeFailurePolicy,
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


object BatchConsumer:

  /**
   * Tuning for a core batched consumer.
   *
   * @param batchSize       the maximum messages drained per `consume`
   * @param onDecodeFailure what to do when a payload can't be decoded (default [[DecodeFailurePolicy.Surface]])
   */
  final case class Config(
    batchSize: Int = 100,
    onDecodeFailure: DecodeFailurePolicy = DecodeFailurePolicy.Surface,
  )

  /**
   * Convenience: a single ephemeral batched consumer on its own dispatcher. For many consumers sharing one
   * dispatcher, build a [[CoreSubscriber]] once and use the subscriber overload.
   *
   * @param connection the live connection
   * @param subject    the subject to subscribe to (may be a wildcard, e.g. `orders.*`)
   * @param config     batch / decode-failure tuning
   * @tparam A the value consumed
   * @return the consumer; aborts with [[NatsError.Connect]] if the dispatcher can't be created
   */
  def make[A: Serde](
    connection: Connection,
    subject: String,
    config: Config = Config(),
  ): ZIO[Scope, NatsError, ConsumerContract.Batched[NatsError, A]] =
    CoreSubscriber.make(connection).flatMap(make[A](_, subject, config))

  /**
   * Mint a batched consumer on an existing shared [[CoreSubscriber]]. The subscription is established
   * lazily on the first `consume`.
   *
   * @param subscriber the shared subscriber to subscribe through
   * @param subject    the subject to subscribe to
   * @param config     batch / decode-failure tuning
   * @tparam A the value consumed
   * @return the consumer
   */
  def make[A: Serde](
    subscriber: CoreSubscriber,
    subject: String,
    config: Config,
  ): ZIO[Scope, NatsError, ConsumerContract.Batched[NatsError, A]] =
    CorePoll.make(subscriber, subject).map(poll => new BatchConsumer(config.batchSize, poll, config.onDecodeFailure))
