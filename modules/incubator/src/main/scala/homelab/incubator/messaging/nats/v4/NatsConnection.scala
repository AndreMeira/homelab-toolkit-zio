package homelab.incubator.messaging.nats.v4


import io.nats.client.api.{ StorageType, StreamConfiguration }
import io.nats.client.{ Connection, Nats, StreamContext }
import zio.*


/**
 * NATS adapter sketch — '''v4: Core NATS and JetStream, unified.'''
 *
 * v1..v3 explored the arc; v4 gathers the two *surviving* substrates as parallel capabilities (not a
 * version chain — Core NATS ephemeral pub/sub and JetStream durable delivery are siblings, each for a
 * different need). The three consumers are kept deliberately '''standalone''' — each owns its delivery,
 * decode, and (n)ack logic outright — so any later de-duplication is driven by proven shared *behaviour*,
 * not coincidental syntactic resemblance:
 *
 *   - [[CoreConsumer]] — Core NATS, ephemeral (fire-and-forget, no ack), async dispatcher→queue bridge.
 *   - [[JetStreamPollingConsumer]] — JetStream, durable, blocking pull (`next`) + explicit ack.
 *   - [[JetStreamBridgedConsumer]] — JetStream, durable, async `consume` callback→queue bridge + ack.
 *
 * Producers likewise: [[CoreProducer]] (fire-and-forget) and [[JetStreamProducer]] (durable, `PublishAck`).
 * Both key onto a subject derived from the message (`subjectOf`). Infra ([[NatsError]], [[Serde]], this
 * object) is shared because it's support, not consumer behaviour.
 */
object NatsConnection:

  /**
   * Open a connection to `url`, closing it when the scope closes.
   *
   * @param url the NATS server URL (e.g. `nats://localhost:4222`)
   * @return the live connection; aborts with [[NatsError.Connect]] if connecting fails
   */
  def make(url: String): ZIO[Scope, NatsError, Connection] =
    ZIO.acquireRelease(
      ZIO.attemptBlocking(Nats.connect(url)).mapError(NatsError.Connect(_))
    )(connection => ZIO.attemptBlocking(connection.close()).ignore)

  /**
   * Ensure a durable, memory-backed JetStream stream named `name` capturing `subjects` exists, and
   * return its [[StreamContext]]. (Only the JetStream path needs this; Core NATS publishes to subjects
   * directly.)
   *
   * @param connection the live connection
   * @param name       the stream name
   * @param subjects   the subject pattern the stream captures (e.g. `orders.>`)
   * @return the stream context; aborts with [[NatsError.Connect]] if setup fails
   */
  def stream(connection: Connection, name: String, subjects: String): ZIO[Any, NatsError, StreamContext] =
    ZIO
      .attemptBlocking {
        val configuration =
          StreamConfiguration.builder().name(name).subjects(subjects).storageType(StorageType.Memory).build()
        connection.jetStreamManagement().addStream(configuration)
        connection.getStreamContext(name)
      }
      .mapError(NatsError.Connect(_))
