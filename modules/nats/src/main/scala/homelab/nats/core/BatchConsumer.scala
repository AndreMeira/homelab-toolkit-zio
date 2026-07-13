package homelab.nats.core


import homelab.common.messaging.Consumer as ConsumerContract
import homelab.nats.{ DecodeFailurePolicy, NatsError, Serde }
import io.nats.client.{ Connection, Message }
import zio.*


/**
 * A Core NATS batched [[ConsumerContract.Batched]] — ephemeral, fire-and-forget. Drains up to `batchSize`
 * buffered messages from a [[CorePoll]], decodes them, and runs `logic` on the decoded batch. No ack (Core
 * delivers once); undecodable messages are handled per `onDecodeFailure` — [[DecodeFailurePolicy.Surface]]
 * fails `consume` on the first bad payload, [[DecodeFailurePolicy.Discard]] drops the bad ones and delivers
 * the rest.
 *
 * @param batchSize       the maximum messages drained per `consume`
 * @param poll            the message source (subscribes lazily on first `consume`)
 * @param onDecodeFailure what to do when a payload can't be decoded
 * @tparam A the value consumed
 */
final class BatchConsumer[A: Serde](
  batchSize: Int,
  poll: CorePoll,
  onDecodeFailure: DecodeFailurePolicy,
) extends ConsumerContract.Batched[NatsError, A]:

  /**
   * Drain up to `batchSize` messages, decode them, and run `logic` on the decoded batch (skipping the run
   * when nothing decodes). One call processes one batch; a run loop calls it repeatedly.
   *
   * @param logic processes one batch of consumed values
   * @tparam E2 the widened error, admitting `logic`'s failures
   * @return unit once the batch is processed; aborts with [[NatsError.Decode]] under Surface on an
   *         undecodable payload, or with `E2` if `logic` fails
   */
  override def consume[E2 >: NatsError](logic: List[A] => IO[E2, Unit]): IO[E2, Unit] =
    for
      messages <- poll.many(batchSize)
      decoded  <- decode(messages)
      _        <- decoded match
                    case Nil    => ZIO.unit
                    case values => logic(values)
    yield ()

  /**
   * Decode a batch, applying `onDecodeFailure` to any undecodable messages.
   *
   * @param messages the received messages
   * @return the decodable values (empty if none decode); aborts with [[NatsError.Decode]] under Surface on
   *         the first undecodable payload
   */
  private def decode(messages: List[Message]): IO[NatsError, List[A]] =
    val (errors, valid) = messages.partitionMap(message => Serde[A].decode(message.getData))
    onDecodeFailure match
      case DecodeFailurePolicy.Surface if errors.nonEmpty => ZIO.fail(NatsError.Decode(errors.head))
      case _                                              => ZIO.succeed(valid)


object BatchConsumer:

  /**
   * Tuning for a Core batched consumer.
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
   * Mint a batched consumer on an existing shared [[CoreSubscriber]]. The subscription is established lazily
   * on the first `consume`.
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
