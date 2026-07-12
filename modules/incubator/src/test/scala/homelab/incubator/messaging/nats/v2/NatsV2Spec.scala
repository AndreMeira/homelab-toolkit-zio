package homelab.incubator.messaging.nats.v2


import io.nats.client.Connection
import zio.*
import zio.test.*


/**
 * Integration tests for the NATS v2 (dispatcher→queue bridge) sketch against a real broker
 * (Testcontainers). Proves the bridge round-trips, and that many consumers share one dispatcher while
 * each `consume` runs as a fiber (no thread-per-consumer). Every test subscribes before publishing
 * (Core NATS drops messages with no live subscription) and `flush`es so the server registers the
 * subscriptions first. Requires a running Docker daemon.
 */
object NatsV2Spec extends ZIOSpecDefault:

  /** Round-trip to the server so all prior subscriptions are registered before publishing. */
  private def flush(connection: Connection): IO[NatsError, Unit] =
    ZIO.attemptBlocking(connection.flush(5.seconds)).mapError(NatsError.Connect(_))

  def spec = suite("NATS v2 — dispatcher→queue bridge (integration)")(
    test("round-trips a message through the bridge") {
      ZIO.scoped:
        for
          connection <- ZIO.service[Connection]
          subscriber <- NatsSubscriber.make(connection)
          consumer   <- subscriber.consumer("orders.*", Serde.utf8)
          producer    = NatsProducer.make(connection, Serde.utf8)(order => s"orders.$order")
          _          <- flush(connection)
          _          <- producer.emit("alpha")
          received   <- Ref.make(Option.empty[String])
          _          <- consumer.consume(message => received.set(Some(message)))
          out        <- received.get
        yield assertTrue(out == Some("alpha"))
    },
    test("many consumers share one dispatcher, each receiving its own subject concurrently") {
      val count = 50
      ZIO.scoped:
        for
          connection <- ZIO.service[Connection]
          subscriber <- NatsSubscriber.make(connection)
          consumers  <- ZIO.foreach((1 to count).toList)(i => subscriber.consumer(s"shard.$i", Serde.utf8).map(i -> _))
          producer    = NatsProducer.make(connection, Serde.utf8)(payload => s"shard.$payload")
          _          <- flush(connection)
          _          <- ZIO.foreachDiscard(1 to count)(i => producer.emit(i.toString))
          received   <- ZIO.foreachPar(consumers): (_, consumer) =>
                          for
                            ref     <- Ref.make(Option.empty[String])
                            _       <- consumer.consume(message => ref.set(Some(message)))
                            message <- ref.get
                          yield message
        yield assertTrue(received.flatten.toSet == (1 to count).map(_.toString).toSet)
    },
  ).provideShared(NatsSpecLayers.connection) @@ TestAspect.withLiveClock @@ TestAspect.timeout(60.seconds) @@ TestAspect.sequential
