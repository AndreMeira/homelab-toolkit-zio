package homelab.nats.core


import homelab.common.messaging.Consumer as ConsumerContract
import homelab.nats.Codec.Decoder
import homelab.nats.NatsError
import io.nats.client.{ Connection, Message }
import zio.*


/**
 * A Core NATS [[ConsumerContract]] over messages — ephemeral, fire-and-forget. A plain drain over a
 * [[CorePoll]]: take the next message, run `logic`. There is '''no ack''' (Core delivers once and forgets),
 * so a handler failure surfaces to the caller — the message is already gone, nothing to retry.
 *
 * It consumes `Message`, not a decoded value: decoding is layered on top by the `make[A]` factory, so this
 * class has no codec and no decode-failure policy. See [[Consumer.make]] for what an undecodable payload does.
 *
 * @param poll the message source (subscribes lazily on first `consume`)
 */
final class Consumer(poll: CorePoll) extends ConsumerContract[NatsError, Message]:

  /**
   * Take the next message and run `logic` on it. One call processes one message; a run loop calls it
   * repeatedly.
   *
   * @param logic processes one consumed message
   * @tparam E2 the widened error, admitting `logic`'s failures
   * @return noop once the message is processed; aborts with `E2` if `logic` fails
   */
  override def consume[E2 >: NatsError](logic: Message => IO[E2, Unit]): IO[E2, Unit] =
    poll.one.flatMap(logic)


object Consumer:

  /**
   * Convenience: a single ephemeral consumer on its own dispatcher. For many consumers sharing one dispatcher
   * (O(1) delivery threads), build a [[CoreSubscriber]] once and use the subscriber overload.
   *
   * @param connection the live connection
   * @param subject    the subject to subscribe to (may be a wildcard, e.g. `orders.*`)
   * @return the message consumer; aborts with [[NatsError.Connect]] if the dispatcher can't be created
   */
  def apply(
    connection: Connection,
    subject: String,
  ): ZIO[Scope, NatsError, ConsumerContract[NatsError, Message]] =
    CoreSubscriber.make(connection).flatMap(apply(_, subject))

  /**
   * Mint a message consumer on an existing shared [[CoreSubscriber]] — the fan-out form (N consumers, one
   * dispatcher). The subscription is established lazily on the first `consume`.
   *
   * @param subscriber the shared subscriber to subscribe through
   * @param subject    the subject to subscribe to
   * @return the message consumer
   */
  def apply(
    subscriber: CoreSubscriber,
    subject: String,
  ): ZIO[Scope, NatsError, ConsumerContract[NatsError, Message]] =
    CorePoll.make(subscriber, subject).map(poll => new Consumer(poll))

  /**
   * A consumer of decoded values: the message consumer with `A`'s decoder layered over it.
   *
   * Core has nothing to settle, so an undecodable payload simply aborts `consume` with
   * [[NatsError.Decode]] — the message is already gone either way. A caller that would rather skip such a
   * message and carry on runs `consume(...).either` in its loop; that choice belongs to the caller here,
   * not to a policy.
   *
   * @param connection the live connection
   * @param subject    the subject to subscribe to
   * @tparam A the value consumed, with a [[Decoder]] in scope
   * @return the consumer; aborts with [[NatsError.Connect]] if the dispatcher can't be created
   */
  def make[A: Decoder](
    connection: Connection,
    subject: String,
  ): ZIO[Scope, NatsError, ConsumerContract[NatsError, A]] =
    apply(connection, subject).map(_.mapZIO(decode[A]))

  /**
   * A consumer of decoded values on an existing shared [[CoreSubscriber]].
   *
   * @param subscriber the shared subscriber to subscribe through
   * @param subject    the subject to subscribe to
   * @tparam A the value consumed, with a [[Decoder]] in scope
   * @return the consumer
   */
  def make[A: Decoder](
    subscriber: CoreSubscriber,
    subject: String,
  ): ZIO[Scope, NatsError, ConsumerContract[NatsError, A]] =
    apply(subscriber, subject).map(_.mapZIO(decode[A]))

  /**
   * Decode a message, lifting a malformed payload into the error channel.
   *
   * @param message the received message
   * @tparam A the value decoded, with a [[Decoder]] in scope
   * @return the decoded value; aborts with [[NatsError.Decode]] if the payload is malformed
   */
  private def decode[A: Decoder](message: Message): IO[NatsError, A] =
    ZIO.fromEither(Decoder[A].decode(message)).mapError(NatsError.Decode(_))
