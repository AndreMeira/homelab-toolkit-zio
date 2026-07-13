package homelab.incubator.messaging.nats.v5


import io.nats.client.{ Connection, Nats }
import zio.*


object NatsConnection:

  def make(url: String): ZIO[Scope, NatsError, Connection] = ZIO.acquireRelease(
    ZIO.attemptBlocking(Nats.connect(url)).mapError(NatsError.Connect(_))
  )(connection => ZIO.attemptBlocking(connection.close()).ignore)
