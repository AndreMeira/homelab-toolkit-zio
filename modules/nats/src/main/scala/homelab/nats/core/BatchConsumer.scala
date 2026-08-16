package homelab.nats.core


import homelab.common.messaging.Consumer as ConsumerContract
import homelab.nats.Codec.Decoder
import homelab.nats.NatsError
import io.nats.client.{ Connection, Message }
import zio.*


/**
 * A Core NATS batched [[ConsumerContract.Batched]] over messages — ephemeral, fire-and-forget. Drains up to
 * `batchSize` buffered messages from a [[CorePoll]] and runs `logic` on them. No ack (Core delivers once), so
 * a handler failure surfaces to the caller.
 *
 * It consumes `Message`, not decoded values: decoding is layered on top by the `make[A]` factory, so this
 * class has no codec and no decode-failure policy. See [[BatchConsumer.make]].
 *
 * @param batchSize the maximum messages drained per `consume`
 * @param poll      the message source (subscribes lazily on first `consume`)
 */
final class BatchConsumer(batchSize: Int, poll: CorePoll) extends ConsumerContract.Batched[NatsError, Message]:

  /**
   * Drain up to `batchSize` messages and run `logic` on them, skipping the run when nothing arrived. One call
   * processes one batch; a run loop calls it repeatedly.
   *
   * @param logic processes one batch of consumed messages
   * @tparam E2 the widened error, admitting `logic`'s failures
   * @return noop once the batch is processed; aborts with `E2` if `logic` fails
   */
  override def consume[E2 >: NatsError](logic: List[Message] => IO[E2, Unit]): IO[E2, Unit] =
    poll.many(batchSize).flatMap {
      case Nil      => ZIO.unit
      case messages => logic(messages)
    }


object BatchConsumer:

  /**
   * Tuning for a Core batched consumer.
   *
   * @param batchSize the maximum messages drained per `consume`
   */
  final case class Config(batchSize: Int = 100)

  /**
   * Convenience: a single ephemeral batched message consumer on its own dispatcher. For many consumers
   * sharing one dispatcher, build a [[CoreSubscriber]] once and use the subscriber overload.
   *
   * @param connection the live connection
   * @param subject    the subject to subscribe to (may be a wildcard, e.g. `orders.*`)
   * @param config     batch tuning
   * @return the message consumer; aborts with [[NatsError.Connect]] if the dispatcher can't be created
   */
  def apply(
    connection: Connection,
    subject: String,
    config: Config = Config(),
  ): ZIO[Scope, NatsError, ConsumerContract.Batched[NatsError, Message]] =
    CoreSubscriber.make(connection).flatMap(apply(_, subject, config))

  /**
   * Mint a batched message consumer on an existing shared [[CoreSubscriber]]. The subscription is established
   * lazily on the first `consume`.
   *
   * @param subscriber the shared subscriber to subscribe through
   * @param subject    the subject to subscribe to
   * @param config     batch tuning
   * @return the message consumer
   */
  def apply(
    subscriber: CoreSubscriber,
    subject: String,
    config: Config,
  ): ZIO[Scope, NatsError, ConsumerContract.Batched[NatsError, Message]] =
    CorePoll.make(subscriber, subject).map(poll => new BatchConsumer(config.batchSize, poll))

  /**
   * A batched consumer of decoded values: the message consumer with `A`'s decoder layered over it.
   *
   * Core has nothing to settle, so one undecodable payload aborts `consume` for the '''whole batch''' with
   * [[NatsError.Decode]] — its batch-mates are lost with it, the blast radius of batching. A caller that
   * would rather carry on runs `consume(...).either` in its loop.
   *
   * @param connection the live connection
   * @param subject    the subject to subscribe to
   * @param config     batch tuning
   * @tparam A the value consumed, with a [[Decoder]] in scope
   * @return the consumer; aborts with [[NatsError.Connect]] if the dispatcher can't be created
   */
  def make[A: Decoder](
    connection: Connection,
    subject: String,
    config: Config = Config(),
  ): ZIO[Scope, NatsError, ConsumerContract.Batched[NatsError, A]] =
    apply(connection, subject, config).map(decoded[A])

  /**
   * A batched consumer of decoded values on an existing shared [[CoreSubscriber]].
   *
   * @param subscriber the shared subscriber to subscribe through
   * @param subject    the subject to subscribe to
   * @param config     batch tuning
   * @tparam A the value consumed, with a [[Decoder]] in scope
   * @return the consumer
   */
  def make[A: Decoder](
    subscriber: CoreSubscriber,
    subject: String,
    config: Config,
  ): ZIO[Scope, NatsError, ConsumerContract.Batched[NatsError, A]] =
    apply(subscriber, subject, config).map(decoded[A])

  /**
   * Layer a decoder over a batched message consumer, keeping the batched shape that `mapZIO` erases.
   *
   * @param consumer the batched message consumer to decode for
   * @tparam A the value consumed, with a [[Decoder]] in scope
   * @return a batched consumer of decoded values
   */
  private def decoded[A: Decoder](
    consumer: ConsumerContract.Batched[NatsError, Message]
  ): ConsumerContract.Batched[NatsError, A] =
    val values = consumer.mapZIO(decode[A])
    new ConsumerContract.Batched[NatsError, A]:
      override def consume[E2 >: NatsError](logic: List[A] => IO[E2, Unit]): IO[E2, Unit] = values.consume(logic)

  /**
   * Decode a batch, lifting the first malformed payload into the error channel.
   *
   * @param messages the received messages
   * @tparam A the value decoded, with a [[Decoder]] in scope
   * @return the decoded values; aborts with [[NatsError.Decode]] on the first malformed payload
   */
  private def decode[A: Decoder](messages: List[Message]): IO[NatsError, List[A]] =
    ZIO.foreach(messages)(message => ZIO.fromEither(Decoder[A].decode(message)).mapError(NatsError.Decode(_)))
