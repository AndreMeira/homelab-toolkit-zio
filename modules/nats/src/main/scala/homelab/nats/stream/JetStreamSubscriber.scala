package homelab.nats.stream


import homelab.nats.{ NatsConnection, NatsError }
import io.nats.client.{ Connection, ConsumerContext, Message }
import zio.*
import zio.stream.ZStream


/**
 * Attaches durable JetStream consumers over one [[Connection]] and bridges their async delivery into
 * per-consumer queues. Unlike Core's shared `Dispatcher`, each durable consumer has its own
 * `MessageConsumer`; this type holds the connection and, per subscription, creates/attaches the durable
 * consumer and forks its `consume` callback into a bridge queue. `ZStream` is used only internally to adapt
 * the multi-shot callback — it is never surfaced.
 *
 * @param connection the live connection durable consumers are attached over
 */
final class JetStreamSubscriber(connection: Connection):

  /**
   * Attach the durable consumer described by `config`, bridge its async delivery into `queue` (forked into
   * `scope`, torn down on scope close), and return once delivery is live.
   *
   * @param config the stream / durable / subject identity and ack tuning
   * @param queue  the bridge queue delivered messages are offered into
   * @param scope  the scope the delivery fiber is forked into
   * @return unit once delivery is established; aborts with [[NatsError.Connect]] if the consumer can't be
   *         created or attached
   */
  def subscribe(config: ContextConfig, queue: Queue[Message], scope: Scope): IO[NatsError, Unit] =
    for
      context <- consumerContext(config)
      started <- Promise.make[NatsError, Unit]
      _       <- stream(context, started).runForeach(queue.offer).forkIn(scope)
      _       <- started.await
    yield ()

  /**
   * Create (or attach to) the explicit-ack durable pull consumer described by `config` on its stream.
   *
   * @param config the stream / durable / subject identity and ack tuning
   * @return the consumer context; aborts with [[NatsError.Connect]] if the stream is missing or setup fails
   */
  private def consumerContext(config: ContextConfig): IO[NatsError, ConsumerContext] =
    ZIO
      .attemptBlocking:
        connection
          .getStreamContext(config.stream)
          .createOrUpdateConsumer(config.toConsumerConfiguration)
      .mapError(NatsError.Connect(_))

  /**
   * The consumer's messages as a stream: `context.consume` delivers to a `ZStream.asyncScoped`; the
   * `MessageConsumer` is stopped when the stream finalizes, and `started` completes once delivery is live.
   *
   * @param context the consumer context to attach delivery to
   * @param started completed once the async delivery is wired up, or failed if attaching fails
   * @return the delivered messages (adapter-internal; never surfaced)
   */
  private def stream(context: ConsumerContext, started: Promise[NatsError, Unit]): ZStream[Any, NatsError, Message] =
    ZStream.asyncScoped { emit =>
      ZIO
        .acquireRelease(
          ZIO
            .attemptBlocking(context.consume(message => emit(ZIO.succeed(Chunk.single(message)))))
            .mapError(NatsError.Connect(_))
        )(consumer => ZIO.attemptBlocking(consumer.stop()).ignore)
        .tapError(started.fail(_))
        .zipRight(started.succeed(()))
    }


object JetStreamSubscriber:

  /**
   * Create a subscriber over `connection`. The subscriber owns no resource of its own — each durable consumer
   * it attaches is torn down with the scope passed to `subscribe`.
   *
   * @param connection the live connection
   * @return the subscriber
   */
  def make(connection: Connection): JetStreamSubscriber =
    new JetStreamSubscriber(connection)

  /**
   * Create a subscriber over a fresh scoped connection to `uri` — the convenience form for a standalone
   * subscriber that owns its connection.
   *
   * @param uri the NATS server URI to connect to
   * @return the subscriber; aborts with [[NatsError.Connect]] if connecting fails
   */
  def make(uri: String): ZIO[Scope, NatsError, JetStreamSubscriber] =
    NatsConnection.make(uri).map(make)
