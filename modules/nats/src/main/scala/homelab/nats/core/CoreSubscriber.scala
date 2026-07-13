package homelab.nats.core


import homelab.nats.{ NatsConnection, NatsError }
import io.nats.client.{ Connection, Dispatcher, Message, MessageHandler }
import zio.*
import zio.stream.ZStream


/**
 * Owns one shared Core NATS `Dispatcher` and feeds per-consumer bridge queues from it, so N ephemeral
 * consumers share a single delivery thread (O(1), not O(consumers)). This is only the wiring — each
 * subscription pushes into its own queue and the standalone [[CorePoll]] drains it; sharing lives here, not
 * in the consumer. (A `Dispatcher` is a Core NATS concept; the JetStream side has no equivalent — it manages
 * its own `MessageConsumer`.)
 *
 * @param dispatcher the shared dispatcher subscriptions are registered on
 */
final class CoreSubscriber(dispatcher: Dispatcher):

  /**
   * Subscribe `queue` to `subject` on the shared dispatcher, forking the delivery into `scope` (torn down
   * when that scope closes) and returning only once the subscription is live — so a publish that follows
   * can't race ahead of an un-established SUB.
   *
   * @param subject the subject to subscribe to (may be a wildcard, e.g. `orders.*`)
   * @param queue   the bridge queue delivered messages are offered into
   * @param scope   the scope the delivery fiber is forked into
   * @return unit once the subscription is established; aborts with [[NatsError.Connect]] if subscribing fails
   */
  def subscribe(subject: String, queue: Queue[Message], scope: Scope): IO[NatsError, Unit] =
    for
      promise <- Promise.make[NatsError, Unit]
      _       <- stream(subject, promise).runForeach(queue.offer).forkIn(scope)
      _       <- promise.await
    yield ()

  /**
   * The subject's messages as a stream: a `ZStream.asyncScoped` whose registration subscribes on the shared
   * dispatcher (unsubscribing when the stream finalizes) and completes `subscribed` once the subscription is
   * live — the ordering guarantee that lets a caller safely publish afterwards.
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
   * Create a subscriber over `connection`, backed by a fresh shared dispatcher closed when the scope closes.
   *
   * @param connection the live connection
   * @return the subscriber; aborts with [[NatsError.Connect]] if the dispatcher can't be created
   */
  def make(connection: Connection): ZIO[Scope, NatsError, CoreSubscriber] =
    ZIO
      .acquireRelease(
        ZIO.attemptBlocking(connection.createDispatcher(noop)).mapError(NatsError.Connect(_))
      )(dispatcher => ZIO.attemptBlocking(connection.closeDispatcher(dispatcher)).ignore)
      .map(dispatcher => new CoreSubscriber(dispatcher))

  /**
   * Create a subscriber over a fresh scoped connection to `uri` — the convenience form for a standalone
   * subscriber that owns its connection.
   *
   * @param uri the NATS server URI to connect to
   * @return the subscriber; aborts with [[NatsError.Connect]] if connecting or dispatcher creation fails
   */
  def make(uri: String): ZIO[Scope, NatsError, CoreSubscriber] =
    NatsConnection.make(uri).flatMap(make)
