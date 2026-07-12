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
   * @param subject the subject to subscribe to (may be a wildcard, e.g. `orders.*`)
   * @tparam A the value consumed
   * @return the consumer; aborts with [[NatsError.Connect]] if subscribing fails
   */
  def consumer[A](subject: String)(using Serde[A]): ZIO[Scope, NatsError, Consumer[NatsError, A]] =
    for
      queue      <- Queue.unbounded[Message]
      subscribed <- Promise.make[NatsError, Unit]
      _          <- deliveries(subject, subscribed).runForeach(queue.offer).forkScoped
      _          <- subscribed.await
    yield new CoreConsumer(queue)

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
   * Create a subscriber over `connection`, backed by a fresh shared dispatcher closed when the scope
   * closes.
   *
   * @param connection the live connection
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
