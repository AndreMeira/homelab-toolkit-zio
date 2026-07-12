package homelab.incubator.messaging.nats.v1


import io.nats.client.Connection
import zio.*
import zio.test.*


/**
 * Integration tests for the NATS v1 (Core NATS) sketch against a real broker (Testcontainers). Proves
 * the topology ports round-trip through NATS: a [[NatsProducer]] keying onto subjects and a
 * [[NatsConsumer]] reading them back. Because Core NATS is fire-and-forget, every test subscribes
 * *before* publishing (a message with no live subscription is dropped). Requires a running Docker daemon.
 */
object NatsV1Spec extends ZIOSpecDefault:

  def spec = suite("NATS v1 (integration)")(
    test("round-trips a single message: subscribe, publish, consume") {
      ZIO.scoped:
        for
          connection <- ZIO.service[Connection]
          consumer   <- NatsConsumer.make(connection, "orders.*", Serde.utf8, pollTimeout = 500.millis)
          producer    = NatsProducer.make(connection, Serde.utf8)(order => s"orders.$order")
          _          <- producer.emit("alpha")
          received   <- Ref.make(Option.empty[String])
          _          <- consumer.consume(message => received.set(Some(message)))
          out        <- received.get
        yield assertTrue(out == Some("alpha"))
    },
    test("a wildcard subscription receives messages published across keyed subjects") {
      ZIO.scoped:
        for
          connection <- ZIO.service[Connection]
          consumer   <- NatsConsumer.make(connection, "events.*", Serde.utf8, pollTimeout = 500.millis)
          producer    = NatsProducer.make(connection, Serde.utf8)(event => s"events.$event")
          _          <- ZIO.foreachDiscard(1 to 5)(i => producer.emit(i.toString))
          received   <- Ref.make(List.empty[String])
          _          <- ZIO.foreachDiscard(1 to 5)(_ => consumer.consume(message => received.update(_ :+ message)))
          out        <- received.get
        yield assertTrue(out.toSet == (1 to 5).map(_.toString).toSet)
    },
  ).provideShared(NatsSpecLayers.connection) @@ TestAspect.withLiveClock @@ TestAspect.timeout(60.seconds) @@ TestAspect.sequential
