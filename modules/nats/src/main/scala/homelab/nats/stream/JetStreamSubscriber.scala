package homelab.nats.stream


import homelab.nats.{ NatsConnection, NatsError }
import io.nats.client.{ Connection, ConsumerContext }
import zio.*


/**
 * Attaches durable JetStream consumers over one [[Connection]].
 *
 * That is all it does: create (or attach to) the durable consumer and hand back its `ConsumerContext`. There
 * is no delivery bridge, because the consumers built on this *pull* — they ask for a message when they are
 * ready to handle one. Nothing is buffered between the server and the handler, so backpressure is intrinsic
 * (an unasked message is not delivered) and a message's `ackWait` clock starts when we take it rather than
 * while it waits in a queue we forgot to heartbeat.
 *
 * @param connection the live connection durable consumers are attached over
 */
final class JetStreamSubscriber(connection: Connection):

  /**
   * Create (or attach to) the explicit-ack durable consumer described by `config` on its stream.
   *
   * @param config the stream / durable / subject identity and ack tuning
   * @return the consumer context to pull from; aborts with [[NatsError.Connect]] if the stream is missing or
   *         setup fails
   */
  def attach(config: ContextConfig): IO[NatsError, ConsumerContext] =
    ZIO
      .attemptBlocking:
        connection
          .getStreamContext(config.stream)
          .createOrUpdateConsumer(config.toConsumerConfiguration)
      .mapError(NatsError.Connect(_))


object JetStreamSubscriber:

  /**
   * Create a subscriber over `connection`. It owns no resource of its own — a durable consumer lives on the
   * server, and the contexts it hands out hold nothing that needs closing.
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
