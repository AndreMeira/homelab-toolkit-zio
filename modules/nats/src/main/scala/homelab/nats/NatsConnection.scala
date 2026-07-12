package homelab.nats


import io.nats.client.api.{ AckPolicy, ConsumerConfiguration, StorageType, StreamConfiguration }
import io.nats.client.{ Connection, ConsumerContext, Nats, StreamContext }
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
   * Ensure a durable, memory-backed JetStream stream named `name` capturing `subjects` exists, and return
   * its [[StreamContext]]. Idempotent: if the stream already exists it is left as-is (not clobbered) and
   * only created when absent — so this is safe to call on redeploy or from a second service. (Only the
   * JetStream path needs this; Core NATS publishes to subjects directly.)
   *
   * @param connection the live connection
   * @param name       the stream name
   * @param subjects   the subject pattern the stream captures (e.g. `orders.>`)
   * @return the stream context; aborts with [[NatsError.Connect]] if setup fails
   */
  def stream(connection: Connection, name: String, subjects: String): ZIO[Any, NatsError, StreamContext] =
    ZIO
      .attemptBlocking {
        val management = connection.jetStreamManagement()
        if !management.getStreamNames().contains(name) then
          val _ = management.addStream(
            StreamConfiguration.builder().name(name).subjects(subjects).storageType(StorageType.Memory).build()
          )
        connection.getStreamContext(name)
      }
      .mapError(NatsError.Connect(_))

  /**
   * Create (or attach to) an explicit-ack durable pull consumer on an existing `stream`. Shared by both
   * JetStream consumer models, which differ only in how they receive from the returned context.
   *
   * @param connection    the live connection
   * @param stream        the (existing) stream name
   * @param durable       the durable consumer name (shared progress across restarts)
   * @param subject       the subject filter
   * @param ackWait       how long the server waits for an ack before redelivering
   * @param maxAckPending the backpressure bound on un-acked in-flight messages
   * @return its context; aborts with [[NatsError.Connect]] if the stream is missing or setup fails
   */
  def durableConsumer(
    connection: Connection,
    stream: String,
    durable: String,
    subject: String,
    ackWait: Duration,
    maxAckPending: Int,
  ): ZIO[Any, NatsError, ConsumerContext] =
    ZIO
      .attemptBlocking {
        val configuration = ConsumerConfiguration
          .builder()
          .durable(durable)
          .filterSubject(subject)
          .ackPolicy(AckPolicy.Explicit)
          .ackWait(ackWait)
          .maxAckPending(maxAckPending.toLong)
          .build()
        connection.getStreamContext(stream).createOrUpdateConsumer(configuration)
      }
      .mapError(NatsError.Connect(_))
