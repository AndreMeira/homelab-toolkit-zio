package homelab.nats.core


import homelab.common.messaging.Consumer as ConsumerContract
import homelab.nats.{ DecodeFailurePolicy, NatsError, Serde }
import io.nats.client.{ Connection, Message }
import zio.*


/**
 * A Core NATS [[ConsumerContract]] — ephemeral, fire-and-forget. A plain drain over a [[CorePoll]]: take the
 * next message, decode it, run `logic`. There is '''no ack''' (Core delivers once and forgets), so a handler
 * failure surfaces to the caller — the message is already gone, nothing to retry. An undecodable payload is
 * handled per `onDecodeFailure`: [[DecodeFailurePolicy.Surface]] fails `consume`,
 * [[DecodeFailurePolicy.Discard]] skips it and delivers the next.
 *
 * @param poll            the message source (subscribes lazily on first `consume`)
 * @param onDecodeFailure what to do when a payload can't be decoded
 * @tparam A the value consumed
 */
final class Consumer[A: Serde](poll: CorePoll, onDecodeFailure: DecodeFailurePolicy) extends ConsumerContract[NatsError, A]:

  /**
   * Take the next message, decode it, and run `logic` on the value (or skip it, under Discard). One call
   * processes one message; a run loop calls it repeatedly.
   *
   * @param logic processes one consumed value
   * @tparam E2 the widened error, admitting `logic`'s failures
   * @return unit once the message is processed; aborts with [[NatsError.Decode]] under Surface on an
   *         undecodable payload, or with `E2` if `logic` fails
   */
  override def consume[E2 >: NatsError](logic: A => IO[E2, Unit]): IO[E2, Unit] =
    for
      message <- poll.one
      decoded <- decode(message)
      _       <- decoded match
                   case None        => ZIO.unit // undecodable under Discard — skip and take the next
                   case Some(value) => logic(value)
    yield ()

  /**
   * Decode a message, applying `onDecodeFailure` when it can't be decoded.
   *
   * @param message the received message
   * @return `Some(value)` when decodable, or `None` to skip it (Discard); aborts with [[NatsError.Decode]]
   *         under Surface
   */
  private def decode(message: Message): IO[NatsError, Option[A]] =
    Serde[A].decode(message.getData) match
      case Right(value) => ZIO.succeed(Some(value))
      case Left(error)  =>
        onDecodeFailure match
          case DecodeFailurePolicy.Surface => ZIO.fail(NatsError.Decode(error))
          case DecodeFailurePolicy.Discard => ZIO.succeed(None)


object Consumer:

  /**
   * Tuning for a Core consumer.
   *
   * @param onDecodeFailure what to do when a payload can't be decoded (default [[DecodeFailurePolicy.Surface]])
   */
  final case class Config(onDecodeFailure: DecodeFailurePolicy = DecodeFailurePolicy.Surface)

  /**
   * Convenience: a single ephemeral consumer on its own dispatcher. For many consumers sharing one dispatcher
   * (O(1) delivery threads), build a [[CoreSubscriber]] once and use the subscriber overload.
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
