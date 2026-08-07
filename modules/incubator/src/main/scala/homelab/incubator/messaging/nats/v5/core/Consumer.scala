package homelab.incubator.messaging.nats.v5.core


import homelab.common.messaging.Consumer as ConsumerContract
import homelab.incubator.messaging.nats.v5.FailurePolicy.{ DecodeFailurePolicy, HandlerFailurePolicy }
import homelab.incubator.messaging.nats.v5.{ NatsError, Serde }
import io.nats.client.{ Connection, Message }
import zio.*


class Consumer[A: Serde](
  poll: CorePoll,
  onDecodeFailure: DecodeFailurePolicy,
) extends ConsumerContract[NatsError, A] {

  /**
   * Take the next value (or batch, for [[Consumer.Batched]]) and run `logic` on it, within the
   * adapter's commit boundary. One call processes one noop; a run loop calls it repeatedly.
   *
   * @param logic processes one consumed value
   * @tparam E2 the widened error, admitting `logic`'s failures
   * @return noop once the value is processed and committed; aborts with `E2` on failure
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


object Consumer:

  /**
   * Tuning for a core consumer.
   *
   * @param onDecodeFailure what to do when a payload can't be decoded (default [[DecodeFailurePolicy.Surface]])
   */
  final case class Config(onDecodeFailure: DecodeFailurePolicy = DecodeFailurePolicy.Surface)

  /**
   * Convenience: a single ephemeral consumer on its own dispatcher. For many consumers sharing one
   * dispatcher (O(1) delivery threads), build a [[CoreSubscriber]] once and use the subscriber overload.
   *
   * @param connection the live connection
   * @param subject    the subject to subscribe to (may be a wildcard, e.g. `orders.*`)
   * @param config     decode-failure tuning
   * @tparam A the value consumed
   * @return the consumer; aborts with [[NatsError.Connect]] if the dispatcher can't be created
   */
  def make[A: Serde](
    connection: Connection,
    subject: String,
    config: Config = Config(),
  ): ZIO[Scope, NatsError, ConsumerContract[NatsError, A]] =
    CoreSubscriber.make(connection).flatMap(make[A](_, subject, config))

  /**
   * Mint a consumer on an existing shared [[CoreSubscriber]] — the fan-out form (N consumers, one
   * dispatcher). The subscription is established lazily on the first `consume`.
   *
   * @param subscriber the shared subscriber to subscribe through
   * @param subject    the subject to subscribe to
   * @param config     decode-failure tuning
   * @tparam A the value consumed
   * @return the consumer
   */
  def make[A: Serde](
    subscriber: CoreSubscriber,
    subject: String,
    config: Config,
  ): ZIO[Scope, NatsError, ConsumerContract[NatsError, A]] =
    CorePoll.make(subscriber, subject).map(poll => new Consumer(poll, config.onDecodeFailure))
