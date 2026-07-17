package homelab.incubator.messaging.nats.v4


import homelab.common.messaging.Consumer
import io.nats.client.{ Connection, Dispatcher, Message, MessageHandler }
import zio.*
import zio.stream.ZStream


/**
 * Owns one shared Core NATS `Dispatcher` and mints [[CoreConsumer]]s onto it, so N ephemeral consumers
 * share a single delivery thread (O(1), not O(consumers)). This is '''only the factory''' — each
 * subscription feeds its own queue and the standalone [[CoreConsumer]] just drains it; sharing lives here,
 * not in the consumer class. (A `Dispatcher` is a Core NATS concept; the JetStream consumers have no
 * equivalent — they manage their own `MessageConsumer`.)
 *
 * @param dispatcher the shared dispatcher subscriptions are registered on
 */
final class NatsSubscriber(dispatcher: Dispatcher):

  /**
   * Subscribe to `subject` on the shared dispatcher and expose it as a [[CoreConsumer]]. The subscription
   * is removed when the scope closes; the returned effect completes only once the subscription is live
   * (Core NATS drops messages published while no subscription exists).
   *
   * @param subject         the subject to subscribe to (may be a wildcard, e.g. `orders.*`)
   * @param onDecodeFailure what to do when a payload can't be decoded (default [[DecodeFailurePolicy.Surface]])
   * @tparam A the value consumed
   * @return the consumer; aborts with [[NatsError.Connect]] if subscribing fails
   */
  def consumer[A](subject: String, onDecodeFailure: DecodeFailurePolicy = DecodeFailurePolicy.Surface)(using Serde[A])
    : ZIO[Scope, NatsError, Consumer[NatsError, A]] =
    deliveryQueue(subject).map(queue => new CoreConsumer(queue, onDecodeFailure))

  /**
   * As [[consumer]], but batched: drains up to `batchSize` messages per `consume` (see
   * [[CoreBatchedConsumer]]).
   *
   * @param subject         the subject to subscribe to (may be a wildcard, e.g. `orders.*`)
   * @param batchSize       the maximum messages drained per `consume`
   * @param onDecodeFailure what to do when a message can't be decoded (default [[DecodeFailurePolicy.Surface]])
   * @tparam A the value consumed
   * @return the batched consumer; aborts with [[NatsError.Connect]] if subscribing fails
   */
  def batchedConsumer[A](
    subject: String,
    batchSize: Int,
    onDecodeFailure: DecodeFailurePolicy = DecodeFailurePolicy.Surface,
  )(using Serde[A]
  ): ZIO[Scope, NatsError, Consumer.Batched[NatsError, A]] =
    deliveryQueue(subject).map(queue => new CoreBatchedConsumer(queue, batchSize, onDecodeFailure))

  /**
   * Subscribe to `subject` on the shared dispatcher and return the queue its messages are bridged into —
   * the shared setup behind both [[consumer]] and [[batchedConsumer]]. The subscription is removed on
   * scope close, and this completes only once it is live.
   *
   * @param subject the subject to subscribe to
   * @return the fed queue; aborts with [[NatsError.Connect]] if subscribing fails
   */
  private def deliveryQueue(subject: String): ZIO[Scope, NatsError, Queue[Message]] =
    for
      queue      <- Queue.unbounded[Message]
      subscribed <- Promise.make[NatsError, Unit]
      _          <- deliveries(subject, subscribed).runForeach(queue.offer).forkScoped
      _          <- subscribed.await
    yield queue

  /**
   * The subject's messages as a stream: a `ZStream.asyncScoped` whose registration subscribes on the
   * shared dispatcher (unsubscribing when the stream finalizes) and completes `subscribed` once live.
   *
   * @param subject    the subject to subscribe to
   * @param subscribed completed when the subscription is live, or failed if subscribing fails
   * @return the received messages (adapter-internal; never surfaced)
   */
  private def deliveries(subject: String, subscribed: Promise[NatsError, Unit]): ZStream[Any, NatsError, Message] =
    ZStream.asyncScoped[Any, NatsError, Message] { emit =>
      ZIO
        .acquireRelease(
          ZIO
            .attemptBlocking(dispatcher.subscribe(subject, (message: Message) => emit(ZIO.succeed(Chunk.single(message)))))
            .mapError(NatsError.Connect(_))
        )(subscription => ZIO.attemptBlocking(dispatcher.unsubscribe(subscription)).ignore)
        .tapError(subscribed.fail(_))
        .zipRight(subscribed.succeed(()))
    }


object NatsSubscriber:

  /**
   * Create a subscriber over `make`, backed by a fresh shared dispatcher closed when the scope
   * closes.
   *
   * @param connection the live make
   * @return the subscriber; aborts with [[NatsError.Connect]] if the dispatcher can't be created
   */
  def make(connection: Connection): ZIO[Scope, NatsError, NatsSubscriber] =
    ZIO
      .acquireRelease(
        ZIO.attemptBlocking(connection.createDispatcher(noop)).mapError(NatsError.Connect(_))
      )(dispatcher => ZIO.attemptBlocking(connection.closeDispatcher(dispatcher)).ignore)
      .map(dispatcher => new NatsSubscriber(dispatcher))

  /** Unused default handler — every subscription supplies its own, but `createDispatcher` requires one. */
  private val noop: MessageHandler = _ => ()
