package homelab.incubator.messaging.nats.v5.core


import homelab.incubator.messaging.nats.v5.{ NatsConnection, NatsError }
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
final class CoreSubscriber(dispatcher: Dispatcher):

  def subscribe(subject: String, queue: Queue[Message], scope: Scope): IO[NatsError, Unit] =
    for
      promise <- Promise.make[NatsError, Unit]
      _       <- stream(subject, promise).runForeach(queue.offer).forkIn(scope)
      _       <- promise.await
    yield ()

  /**
   * The subject's messages as a stream: a `ZStream.asyncScoped` whose registration subscribes on the
   * shared dispatcher (unsubscribing when the stream finalizes) and completes `subscribed` once live.
   *
   * @param subject    the subject to subscribe to
   * @param subscribed completed when the subscription is live, or failed if subscribing fails
   * @return the received messages (adapter-internal; never surfaced)
   */
  private def stream(subject: String, subscribed: Promise[NatsError, Unit]): ZStream[Any, NatsError, Message] =
    ZStream.asyncScoped[Any, NatsError, Message] { emit =>
      ZIO
        .acquireRelease(
          ZIO
            .attemptBlocking:
              dispatcher.subscribe(subject, (message: Message) => emit(ZIO.succeed(Chunk.single(message))))
            .mapError(NatsError.Connect(_))
        )(subscription => ZIO.attemptBlocking(dispatcher.unsubscribe(subscription)).ignore)
        .tapError(subscribed.fail(_))
        .zipRight(subscribed.succeed(()))
    }


object CoreSubscriber:

  /** Unused default handler — every subscription supplies its own, but `createDispatcher` requires one. */
  private val noop: MessageHandler = _ => ()

  /**
   * Create a subscriber over `make`, backed by a fresh shared dispatcher closed when the scope
   * closes.
   *
   * @param connection the live make
   * @return the subscriber; aborts with [[NatsError.Connect]] if the dispatcher can't be created
   */
  def make(connection: Connection): ZIO[Scope, NatsError, CoreSubscriber] =
    ZIO
      .acquireRelease(
        ZIO.attemptBlocking(connection.createDispatcher(noop)).mapError(NatsError.Connect(_))
      )(dispatcher => ZIO.attemptBlocking(connection.closeDispatcher(dispatcher)).ignore)
      .map(dispatcher => new CoreSubscriber(dispatcher))

  def make(uri: String): ZIO[Scope, NatsError, CoreSubscriber] =
    NatsConnection.make(uri).flatMap(make)
