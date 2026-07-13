package homelab.nats


import io.nats.client.{ Connection, Nats }
import zio.*


/**
 * The NATS adapter's connection lifecycle. A NATS [[Connection]] is the single shared handle every producer,
 * consumer, and subscriber is built over — this object opens one as a scoped resource (connect on acquire,
 * close on release) so the whole adapter shuts down cleanly with its scope.
 *
 * Stream *provisioning* deliberately lives elsewhere: creating a JetStream stream (its subjects, retention,
 * storage) is an operational concern handled out-of-band (the `nats` CLI, the NACK operator, Terraform), and
 * the app only *attaches* its durable consumer to a pre-existing stream (see
 * [[homelab.nats.stream.JetStreamSubscriber]]). So this object exposes connecting and nothing else.
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
