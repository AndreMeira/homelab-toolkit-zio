package homelab.incubator.messaging.nats.v5.stream


import homelab.incubator.messaging.nats.v5.{ NatsConnection, NatsError }
import homelab.incubator.messaging.nats.v5.config.ContextConfig
import io.nats.client.{ Connection, ConsumerContext, Message }
import zio.stream.ZStream
import zio.*


class JetStreamSubscriber(connection: Connection) {

  def subscribe(config: ContextConfig, queue: Queue[Message], scope: Scope): IO[NatsError, Unit] =
    for
      promise <- Promise.make[NatsError, Unit]
      context <- consumerContext(connection, config)
      _       <- stream(context, promise).runForeach(queue.offer).forkIn(scope)
      _       <- promise.await
    yield ()

  def consumerContext(connection: Connection, config: ContextConfig): IO[NatsError, ConsumerContext] =
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
   * @param started completed once the async delivery is wired up
   * @return the delivered messages (adapter-internal; never surfaced)
   */
  private def stream(context: ConsumerContext, started: Promise[NatsError, Unit]): ZStream[Any, NatsError, Message] =
    ZStream.asyncScoped { emit =>
      ZIO
        .acquireRelease(
          ZIO
            .attemptBlocking:
              context.consume(message => emit(ZIO.succeed(Chunk.single(message))))
            .mapError(NatsError.Connect(_))
        )(consumer => ZIO.attemptBlocking(consumer.stop()).ignore)
        .tapError(started.fail(_))
        .zipRight(started.succeed(()))
    }
}


object JetStreamSubscriber:

  /**
   * Create a subscriber over `make`, backed by a fresh consumer context closed when the scope closes.
   *
   * @param uri  the NATS server URI to connect to
   * @return the subscriber; aborts with [[NatsError.Connect]] if the consumer context can't be created
   */
  def make(uri: String): ZIO[Scope, NatsError, JetStreamSubscriber] =
    NatsConnection.make(uri).map(JetStreamSubscriber(_))
