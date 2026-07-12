package homelab.incubator.messaging.nats.v4


import io.nats.client.Connection
import zio.*
import zio.test.*

import java.nio.charset.StandardCharsets


/**
 * Integration tests for the unified NATS v4 sketch against a real broker (Testcontainers). Covers both
 * substrates: Core NATS (ephemeral) round-trip + wildcard routing, and JetStream (durable) round-trips
 * through both consumer models plus redelivery, poison/term, and multi-message routing. Core tests
 * subscribe before publishing (fire-and-forget); JetStream tests rely on durability. Each test uses its
 * own subject/stream so they don't interfere. Requires a running Docker daemon.
 */
object NatsV4Spec extends ZIOSpecDefault:

  def spec = suite("NATS v4 — Core + JetStream (integration)")(
    suite("core (ephemeral)")(
      test("round-trips a message: subscribe, publish, consume") {
        ZIO.scoped:
          for
            connection <- ZIO.service[Connection]
            consumer   <- CoreConsumer.make[String](connection, "core.orders.*")(using Serde.utf8)
            producer    = CoreProducer.make[String](connection)(order => s"core.orders.$order")(using Serde.utf8)
            _          <- producer.emit("alpha")
            received   <- Ref.make(Option.empty[String])
            _          <- consumer.consume(message => received.set(Some(message)))
            out        <- received.get
          yield assertTrue(out == Some("alpha"))
      },
      test("a wildcard subscription receives messages across keyed subjects") {
        ZIO.scoped:
          for
            connection <- ZIO.service[Connection]
            consumer   <- CoreConsumer.make[String](connection, "core.events.*")(using Serde.utf8)
            producer    = CoreProducer.make[String](connection)(event => s"core.events.$event")(using Serde.utf8)
            _          <- ZIO.foreachDiscard(1 to 5)(i => producer.emit(i.toString))
            received   <- Ref.make(List.empty[String])
            _          <- ZIO.foreachDiscard(1 to 5)(_ => consumer.consume(message => received.update(_ :+ message)))
            out        <- received.get
          yield assertTrue(out.toSet == (1 to 5).map(_.toString).toSet)
      },
      test("many consumers share one subscriber's dispatcher, each receiving its own subject") {
        val count = 20
        ZIO.scoped:
          for
            connection <- ZIO.service[Connection]
            subscriber <- NatsSubscriber.make(connection)
            consumers  <- ZIO.foreach((1 to count).toList)(i => subscriber.consumer[String](s"core.shard.$i")(using Serde.utf8).map(i -> _))
            producer    = CoreProducer.make[String](connection)(payload => s"core.shard.$payload")(using Serde.utf8)
            _          <- ZIO.foreachDiscard(1 to count)(i => producer.emit(i.toString))
            received   <- ZIO.foreachPar(consumers): (_, consumer) =>
                            for
                              ref     <- Ref.make(Option.empty[String])
                              _       <- consumer.consume(message => ref.set(Some(message)))
                              message <- ref.get
                            yield message
          yield assertTrue(received.flatten.toSet == (1 to count).map(_.toString).toSet)
      },
    ),
    suite("jetstream (durable)")(
      test("polling: a durable message round-trips and is acked") {
        ZIO.scoped:
          for
            connection <- ZIO.service[Connection]
            _          <- NatsConnection.stream(connection, "ORDERS", "orders.>")
            producer   <- JetStreamProducer.make[String](connection)(order => s"orders.$order")(using Serde.utf8)
            consumer   <- JetStreamPollingConsumer.make[String](connection, "ORDERS", "poller", "orders.>")(using Serde.utf8)
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
            _          <- NatsConnection.stream(connection, "EVENTS", "events.>")
            producer   <- JetStreamProducer.make[String](connection)(event => s"events.$event")(using Serde.utf8)
            consumer   <- JetStreamBridgedConsumer.make[String](connection, "EVENTS", "bridge", "events.>")(using Serde.utf8)
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
            _          <- NatsConnection.stream(connection, "REDELIVER", "redeliver.>")
            producer   <- JetStreamProducer.make[String](connection)(value => s"redeliver.$value")(using Serde.utf8)
            consumer   <- JetStreamPollingConsumer.make[String](
                            connection,
                            "REDELIVER",
                            "worker",
                            "redeliver.>",
                            JetStreamPollingConsumer.Config(ackWait = 2.seconds),
                          )(using Serde.utf8)
            attempts   <- Ref.make(0)
            done       <- Promise.make[Nothing, Unit]
            // The error value is irrelevant — settle naks any handler failure; only the retry matters.
            logic       = (_: String) =>
                            attempts.updateAndGet(_ + 1).flatMap: attempt =>
                              if attempt == 1 then ZIO.fail(NatsError.Decode("forced first-attempt failure"))
                              else done.succeed(()).unit
            _          <- producer.emit("x")
            fiber      <- consumer.consume(logic).forever.fork
            _          <- done.await
            count      <- attempts.get
            _          <- fiber.interrupt
          yield assertTrue(count >= 2)
      },
      test("an undecodable payload is termed once (not redelivered) and doesn't wedge the consumer") {
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
            consumer   <- JetStreamPollingConsumer.make[Int](connection, "POISON", "worker", "poison.>")(using intSerde)
            good       <- Promise.make[Nothing, Int]
            _          <- producer.emit("oops") // undecodable → term
            _          <- producer.emit("42")   // decodable → delivered next
            fiber      <- consumer.consume(value => good.succeed(value).unit).forever.fork
            value      <- good.await
            _          <- fiber.interrupt
          yield assertTrue(value == 42, decodeCount.get == 2)
      },
      test("consumes many messages across keyed subjects under a wildcard filter") {
        ZIO.scoped:
          for
            connection <- ZIO.service[Connection]
            _          <- NatsConnection.stream(connection, "SHARDED", "sharded.>")
            producer   <- JetStreamProducer.make[String](connection)(value => s"sharded.$value")(using Serde.utf8)
            consumer   <- JetStreamPollingConsumer.make[String](connection, "SHARDED", "worker", "sharded.>")(using Serde.utf8)
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
    ),
  ).provideShared(NatsSpecLayers.connection) @@ TestAspect.withLiveClock @@ TestAspect.timeout(90.seconds) @@ TestAspect.sequential
