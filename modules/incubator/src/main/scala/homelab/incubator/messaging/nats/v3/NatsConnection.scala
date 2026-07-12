package homelab.incubator.messaging.nats.v3


import io.nats.client.api.{ StorageType, StreamConfiguration }
import io.nats.client.{ Connection, Nats, StreamContext }
import zio.*


/**
 * NATS adapter sketch — '''v3: JetStream (durable + explicit ack).'''
 *
 * This is where the `Consumer` port's real commitment lands. v1/v2 were Core NATS (fire-and-forget); v3
 * uses JetStream durable streams and '''explicit acknowledgement''': a message is `ack`ed only after its
 * handler succeeds, `nak`ed (redelivered) on failure, `term`ed (dead-lettered) if undecodable. Redelivery
 * is therefore real, so '''handlers must be idempotent''' — exactly the port's stated contract.
 *
 * It folds in every lesson from the earlier sketches:
 *   - '''Keying→subject''' ([[JetStreamProducer]]) — unchanged from v1/v2.
 *   - '''Two consumer models''' behind smart constructors ([[NatsConsumer.polling]] vs
 *     [[NatsConsumer.bridged]]) — the pull-vs-push axis, now durable. `polling` blocks a thread per
 *     consumer but is simple and demand-driven; `bridged` reuses v2's push→`ZStream`→`Queue` bridge for
 *     O(1)-thread fan-out.
 *   - '''Backpressure''' — answered server-side by the consumer's `maxAckPending` (the server stops
 *     delivering past that many un-acked messages), which is strictly better than v2's local-queue knobs:
 *     no thread stall, no head-of-line blocking across a shared dispatcher. So the local bridge queue can
 *     stay unbounded and still be bounded overall by `maxAckPending`.
 *
 * A scoped NATS [[Connection]] — connects on acquire, closes on release.
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
   * Ensure a durable, memory-backed stream named `name` capturing `subjects` exists, and return its
   * [[StreamContext]] (the handle producers publish through and consumers attach to).
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
