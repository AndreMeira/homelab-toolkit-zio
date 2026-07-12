package homelab.incubator.messaging.nats.v1


import io.nats.client.{ Connection, Nats }
import zio.*


/**
 * NATS adapter sketch — '''v1: Core NATS (ephemeral pub/sub).'''
 *
 * This is the simplest mapping of the `homelab.common.messaging` topology onto NATS: a [[NatsProducer]]
 * publishes to a subject derived from the message, a [[NatsConsumer]] reads from a synchronous
 * subscription. It proves the port shape, the keying→subject decision, the [[Serde]] seam, and scoped
 * connection lifecycle — but it is '''fire-and-forget''': no acknowledgement, no durability, no
 * redelivery. If a consumer isn't attached, messages are lost.
 *
 * Version arc (exploration):
 *   - '''v1''' (here) — Core NATS, ephemeral, *synchronous* subscription. Does the topology shape fit?
 *     It does — but `consume` parks a blocking thread per consumer (see [[NatsConsumer]]), so consumer
 *     count per instance is bounded by the thread budget, not by NATS.
 *   - '''v2''' — Core NATS, *async* `Dispatcher` → ZIO `Queue` bridge: `consume` becomes fiber-based, so
 *     many consumers share the connection's threads (O(1), not O(consumers)).
 *   - '''v3''' — JetStream: durable streams + ack-based `consume` (the real commit boundary the
 *     `Consumer` port promises), subject-partitioning for keys, redelivery ⇒ idempotent handlers.
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
