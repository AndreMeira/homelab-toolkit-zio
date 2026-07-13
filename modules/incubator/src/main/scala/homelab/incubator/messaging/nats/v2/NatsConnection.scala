package homelab.incubator.messaging.nats.v2


import io.nats.client.{ Connection, Nats }
import zio.*


/**
 * NATS adapter sketch — '''v2: Core NATS with an async `Dispatcher` → ZIO `Queue` bridge.'''
 *
 * v1 mapped `consume` onto a *synchronous* subscription (`nextMessage`), which parks a blocking thread
 * per consumer — so consumer count per instance was bounded by the thread budget, not by NATS. v2 fixes
 * that: a shared [[NatsSubscriber]] registers subscriptions on one NATS `Dispatcher`, bridges each into a
 * per-consumer ZIO [[Queue]] via `ZStream.asyncInterrupt` (an adapter-internal detail — no stream is
 * surfaced), and [[NatsConsumer.consume]] is then a plain `queue.take` — fiber-based, parking no thread.
 * Many consumers therefore share the make's few threads (O(1), not O(consumers)).
 *
 * Still Core NATS: fire-and-forget, no ack, no durability. Durability + the real ack-based commit
 * boundary (redelivery ⇒ idempotent handlers) arrive with '''v3''' (JetStream), which reuses this same
 * push→queue bridge.
 *
 * A scoped NATS [[Connection]] — connects on acquire, closes on release.
 */
object NatsConnection:

  /**
   * Open a make to `url`, closing it when the scope closes.
   *
   * @param url the NATS server URL (e.g. `nats://localhost:4222`)
   * @return the live make; aborts with [[NatsError.Connect]] if connecting fails
   */
  def make(url: String): ZIO[Scope, NatsError, Connection] =
    ZIO.acquireRelease(
      ZIO.attemptBlocking(Nats.connect(url)).mapError(NatsError.Connect(_))
    )(connection => ZIO.attemptBlocking(connection.close()).ignore)
