package homelab.incubator.messaging.nats.v3


import io.nats.client.Connection
import zio.*
import zio.test.*

import java.nio.charset.StandardCharsets


/**
 * Integration tests for the NATS v3 (JetStream) sketch against a real JetStream broker (Testcontainers).
 * Proves durable publish round-trips through both consumer models, and — the point of JetStream — that a
 * failing handler naks and the message is redelivered until acked (at-least-once + explicit ack). Each
 * test uses its own stream/durable so they don't interfere. Requires a running Docker daemon.
 */
object NatsV3Spec extends ZIOSpecDefault:

  def spec = suite("NATS v3 — JetStream (integration)")(
    test("polling: a durable message round-trips and is acked") {
      ZIO.scoped:
        for
          connection <- ZIO.service[Connection]
          _          <- NatsConnection.stream(connection, "ORDERS", "orders.>") // create the stream
          producer   <- JetStreamProducer.make[String](connection)(order => s"orders.$order")(using Serde.utf8)
          consumer   <- NatsConsumer.polling[String](connection, "ORDERS", "poller", "orders.>")(using Serde.utf8)
          _          <- producer.emit("alpha") // durable — publish before consume is fine
          received   <- Ref.make(Option.empty[String])
          _          <- consumer.consume(message => received.set(Some(message)))
          out        <- received.get
        yield assertTrue(out == Some("alpha"))
    },
    test("bridged: a durable message round-trips through the queue bridge and is acked") {
      ZIO.scoped:
        for
          connection <- ZIO.service[Connection]
          stream     <- NatsConnection.stream(connection, "EVENTS", "events.>") // hold the context to share it
          producer   <- JetStreamProducer.make[String](connection)(event => s"events.$event")(using Serde.utf8)
          consumer   <- NatsConsumer.bridged[String](stream, "bridge", "events.>", NatsConsumer.BridgedConfig())(using Serde.utf8)
          _          <- producer.emit("beta")
          received   <- Ref.make(Option.empty[String])
          _          <- consumer.consume(message => received.set(Some(message)))
          out        <- received.get
        yield assertTrue(out == Some("beta"))
    },
    test("a failing handler naks; the message is redelivered and eventually acked (at-least-once)") {
      ZIO.scoped:
        for
          connection <- ZIO.service[Connection]
          _          <- NatsConnection.stream(connection, "REDELIVER", "redeliver.>") // create the stream
          producer   <- JetStreamProducer.make[String](connection)(value => s"redeliver.$value")(using Serde.utf8)
          consumer   <- NatsConsumer.polling[String](connection, "REDELIVER", "worker", "redeliver.>", NatsConsumer.PollingConfig(ackWait = 2.seconds))(using Serde.utf8)
          attempts   <- Ref.make(0)
          done       <- Promise.make[Nothing, Unit]
          // The error value is irrelevant — `settle` naks any handler failure; only the retry matters.
          logic       = (_: String) =>
                          attempts.updateAndGet(_ + 1).flatMap: attempt =>
                            if attempt == 1 then ZIO.fail(NatsError.Decode("forced first-attempt failure"))
                            else done.succeed(()).unit
          _          <- producer.emit("x")
          fiber      <- consumer.consume(logic).forever.fork
          _          <- done.await // completes only after the redelivered second attempt succeeds
          count      <- attempts.get
          _          <- fiber.interrupt
        yield assertTrue(count >= 2)
    },
    test("an undecodable payload is termed once (not redelivered) and doesn't wedge the consumer") {
      // A fallible codec so a bad payload actually fails to decode; `decodeCount` proves the poison is
      // decoded once (termed) rather than naked and redelivered over and over.
      val decodeCount = new java.util.concurrent.atomic.AtomicInteger(0)
      val intSerde: Serde[Int] = new Serde[Int]:
        def encode(value: Int): Array[Byte] = value.toString.getBytes(StandardCharsets.UTF_8)
        def decode(bytes: Array[Byte]): Either[String, Int] =
          decodeCount.incrementAndGet()
          new String(bytes, StandardCharsets.UTF_8).toIntOption.toRight("not an int")

      ZIO.scoped:
        for
          connection <- ZIO.service[Connection]
          _          <- NatsConnection.stream(connection, "POISON", "poison.>")
          producer   <- JetStreamProducer.make[String](connection)(_ => "poison.in")(using Serde.utf8)
          consumer   <- NatsConsumer.polling[Int](connection, "POISON", "worker", "poison.>")(using intSerde)
          good       <- Promise.make[Nothing, Int]
          _          <- producer.emit("oops") // undecodable → term (dropped, not redelivered)
          _          <- producer.emit("42")   // decodable → delivered next
          fiber      <- consumer.consume(value => good.succeed(value).unit).forever.fork
          value      <- good.await
          _          <- fiber.interrupt
        yield assertTrue(value == 42, decodeCount.get == 2) // oops once (termed), then 42
    },
    test("consumes many messages across keyed subjects under a wildcard filter") {
      ZIO.scoped:
        for
          connection <- ZIO.service[Connection]
          _          <- NatsConnection.stream(connection, "SHARDED", "sharded.>")
          producer   <- JetStreamProducer.make[String](connection)(value => s"sharded.$value")(using Serde.utf8)
          consumer   <- NatsConsumer.polling[String](connection, "SHARDED", "worker", "sharded.>")(using Serde.utf8)
          _          <- ZIO.foreachDiscard(1 to 20)(i => producer.emit(i.toString))
          received   <- Ref.make(Set.empty[String])
          done       <- Promise.make[Nothing, Unit]
          logic       = (message: String) =>
                          received.updateAndGet(_ + message).flatMap(seen => ZIO.when(seen.size == 20)(done.succeed(())).unit)
          fiber      <- consumer.consume(logic).forever.fork
          _          <- done.await
          out        <- received.get
          _          <- fiber.interrupt
        yield assertTrue(out == (1 to 20).map(_.toString).toSet)
    },
  ).provideShared(NatsSpecLayers.connection) @@ TestAspect.withLiveClock @@ TestAspect.timeout(90.seconds) @@ TestAspect.sequential
