package homelab.nats


import homelab.nats.core.*
import homelab.nats.jetstream.*
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
object NatsSpec extends ZIOSpecDefault:

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
      test("Discard: an undecodable message is skipped and the consumer keeps delivering") {
        val intSerde: Serde[Int] = new Serde[Int]:
          def encode(value: Int): Array[Byte] = value.toString.getBytes(StandardCharsets.UTF_8)
          def decode(bytes: Array[Byte]): Either[String, Int] =
            new String(bytes, StandardCharsets.UTF_8).toIntOption.toRight("not an int")

        ZIO.scoped:
          for
            connection <- ZIO.service[Connection]
            consumer   <- CoreConsumer.make[Int](connection, "core.skip.*", DecodeFailurePolicy.Discard)(using intSerde)
            producer    = CoreProducer.make[String](connection)(_ => "core.skip.in")(using Serde.utf8)
            _          <- producer.emit("oops") // undecodable → skipped
            _          <- producer.emit("42")   // decodable → delivered
            received   <- Ref.make(Option.empty[Int])
            _          <- consumer.consume(value => received.set(Some(value)))
            out        <- received.get
          yield assertTrue(out == Some(42))
      },
      test("batched Discard: drops undecodable messages and delivers the rest") {
        val intSerde: Serde[Int] = new Serde[Int]:
          def encode(value: Int): Array[Byte] = value.toString.getBytes(StandardCharsets.UTF_8)
          def decode(bytes: Array[Byte]): Either[String, Int] =
            new String(bytes, StandardCharsets.UTF_8).toIntOption.toRight("not an int")

        ZIO.scoped:
          for
            connection <- ZIO.service[Connection]
            consumer   <- CoreBatchedConsumer.make[Int](connection, "core.skipbatch.*", 10, DecodeFailurePolicy.Discard)(using intSerde)
            producer    = CoreProducer.make[String](connection)(payload => s"core.skipbatch.$payload")(using Serde.utf8)
            _          <- producer.emit("1")
            _          <- producer.emit("oops") // dropped
            _          <- producer.emit("2")
            received   <- Ref.make(Set.empty[Int])
            done       <- Promise.make[Nothing, Unit]
            logic       = (batch: List[Int]) =>
                            received.updateAndGet(_ ++ batch).flatMap(all => ZIO.when(all == Set(1, 2))(done.succeed(())).unit)
            fiber      <- consumer.consume(logic).forever.fork
            _          <- done.await
            out        <- received.get
            _          <- fiber.interrupt
          yield assertTrue(out == Set(1, 2))
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
      test("an undecodable payload fails the consumer by default (Surface — non-destructive)") {
        val intSerde: Serde[Int] = new Serde[Int]:
          def encode(value: Int): Array[Byte] = value.toString.getBytes(StandardCharsets.UTF_8)
          def decode(bytes: Array[Byte]): Either[String, Int] =
            new String(bytes, StandardCharsets.UTF_8).toIntOption.toRight("not an int")

        ZIO.scoped:
          for
            connection <- ZIO.service[Connection]
            _          <- NatsConnection.stream(connection, "POISON_SURFACE", "surface.>")
            producer   <- JetStreamProducer.make[String](connection)(_ => "surface.in")(using Serde.utf8)
            consumer   <- JetStreamPollingConsumer.make[Int](connection, "POISON_SURFACE", "worker", "surface.>")(using intSerde)
            _          <- producer.emit("oops")
            outcome    <- consumer.consume(_ => ZIO.unit).either
          yield assertTrue(outcome match { case Left(NatsError.Decode(_)) => true; case _ => false })
      },
      test("with DecodeFailurePolicy.Discard a poison message is termed once and the consumer continues") {
        val decodeCount = new java.util.concurrent.atomic.AtomicInteger(0)
        val intSerde: Serde[Int] = new Serde[Int]:
          def encode(value: Int): Array[Byte] = value.toString.getBytes(StandardCharsets.UTF_8)
          def decode(bytes: Array[Byte]): Either[String, Int] =
            decodeCount.incrementAndGet()
            new String(bytes, StandardCharsets.UTF_8).toIntOption.toRight("not an int")

        ZIO.scoped:
          for
            connection <- ZIO.service[Connection]
            _          <- NatsConnection.stream(connection, "POISON_DLQ", "dlq.>")
            producer   <- JetStreamProducer.make[String](connection)(_ => "dlq.in")(using Serde.utf8)
            consumer   <- JetStreamPollingConsumer.make[Int](
                            connection,
                            "POISON_DLQ",
                            "worker",
                            "dlq.>",
                            JetStreamPollingConsumer.Config(onDecodeFailure = DecodeFailurePolicy.Discard),
                          )(using intSerde)
            good       <- Promise.make[Nothing, Int]
            _          <- producer.emit("oops") // undecodable → term (dropped, not redelivered)
            _          <- producer.emit("42")   // decodable → delivered next
            fiber      <- consumer.consume(value => good.succeed(value).unit).forever.fork
            value      <- good.await
            _          <- fiber.interrupt
          yield assertTrue(value == 42, decodeCount.get == 2)
      },
      test("with HandlerFailurePolicy.Surface a handler failure surfaces from consume") {
        ZIO.scoped:
          for
            connection <- ZIO.service[Connection]
            _          <- NatsConnection.stream(connection, "HANDLER_SURFACE", "hsurface.>")
            producer   <- JetStreamProducer.make[String](connection)(_ => "hsurface.in")(using Serde.utf8)
            consumer   <- JetStreamPollingConsumer.make[String](
                            connection,
                            "HANDLER_SURFACE",
                            "worker",
                            "hsurface.>",
                            JetStreamPollingConsumer.Config(onHandlerFailure = HandlerFailurePolicy.Surface),
                          )(using Serde.utf8)
            _          <- producer.emit("x")
            outcome    <- consumer.consume(_ => ZIO.fail(NatsError.Decode("handler boom"))).either
          yield assertTrue(outcome match { case Left(NatsError.Decode("handler boom")) => true; case _ => false })
      },
      test("with HandlerFailurePolicy.Discard a failed message is termed once and the consumer continues") {
        ZIO.scoped:
          for
            connection <- ZIO.service[Connection]
            _          <- NatsConnection.stream(connection, "HANDLER_DLQ", "hdlq.>")
            producer   <- JetStreamProducer.make[String](connection)(payload => s"hdlq.$payload")(using Serde.utf8)
            consumer   <- JetStreamPollingConsumer.make[String](
                            connection,
                            "HANDLER_DLQ",
                            "worker",
                            "hdlq.>",
                            JetStreamPollingConsumer.Config(ackWait = 2.seconds, onHandlerFailure = HandlerFailurePolicy.Discard),
                          )(using Serde.utf8)
            attempts   <- Ref.make(0)
            good       <- Promise.make[Nothing, String]
            logic       = (message: String) =>
                            if message == "bad" then attempts.update(_ + 1) *> ZIO.fail(NatsError.Decode("boom"))
                            else good.succeed(message).unit
            _          <- producer.emit("bad")  // handler fails → term (not redelivered)
            _          <- producer.emit("good") // processed next
            fiber      <- consumer.consume(logic).forever.fork
            value      <- good.await
            _          <- fiber.interrupt
            count      <- attempts.get
          yield assertTrue(value == "good", count == 1) // "bad" attempted once, termed, not redelivered
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
      test("heartbeat keeps a slow handler's message from being redelivered") {
        ZIO.scoped:
          for
            connection <- ZIO.service[Connection]
            _          <- NatsConnection.stream(connection, "HEARTBEAT", "heartbeat.>")
            producer   <- JetStreamProducer.make[String](connection)(_ => "heartbeat.in")(using Serde.utf8)
            consumer   <- JetStreamPollingConsumer.make[String](
                            connection,
                            "HEARTBEAT",
                            "worker",
                            "heartbeat.>",
                            JetStreamPollingConsumer.Config(ackWait = 2.seconds, heartbeat = Some(500.millis)),
                          )(using Serde.utf8)
            attempts   <- Ref.make(0)
            done       <- Promise.make[Nothing, Unit]
            // the handler runs longer than ackWait; the heartbeat should keep it from redelivering
            logic       = (_: String) => attempts.update(_ + 1) *> ZIO.sleep(4.seconds) *> done.succeed(()).unit
            _          <- producer.emit("slow")
            fiber      <- consumer.consume(logic).forever.fork
            _          <- done.await
            _          <- ZIO.sleep(1.second) // let any (unexpected) redelivery bump the count
            count      <- attempts.get
            _          <- fiber.interrupt
          yield assertTrue(count == 1)
      },
      test("a durable publish to a subject no stream captures fails with Publish") {
        ZIO.scoped:
          for
            connection <- ZIO.service[Connection]
            producer   <- JetStreamProducer.make[String](connection)(_ => "nostream.subject")(using Serde.utf8)
            outcome    <- producer.emit("x").either
          yield assertTrue(outcome match { case Left(NatsError.Publish(_)) => true; case _ => false })
      },
      test("a durable consumer resumes from its last ack (shared progress across instances)") {
        ZIO.scoped:
          for
            connection <- ZIO.service[Connection]
            _          <- NatsConnection.stream(connection, "RESUME", "resume.>")
            producer   <- JetStreamProducer.make[String](connection)(value => s"resume.$value")(using Serde.utf8)
            _          <- ZIO.foreachDiscard(1 to 6)(i => producer.emit(i.toString))
            first      <- JetStreamPollingConsumer.make[String](connection, "RESUME", "worker", "resume.>")(using Serde.utf8)
            firstSeen  <- Ref.make(List.empty[String])
            _          <- ZIO.foreachDiscard(1 to 3)(_ => first.consume(message => firstSeen.update(_ :+ message)))
            // a fresh consumer with the SAME durable name picks up where the first left off
            second     <- JetStreamPollingConsumer.make[String](connection, "RESUME", "worker", "resume.>")(using Serde.utf8)
            secondSeen <- Ref.make(List.empty[String])
            _          <- ZIO.foreachDiscard(1 to 3)(_ => second.consume(message => secondSeen.update(_ :+ message)))
            one        <- firstSeen.get
            two        <- secondSeen.get
          yield assertTrue(one == List("1", "2", "3"), two == List("4", "5", "6"))
      },
      test("maxAckPending withholds delivery past the in-flight bound") {
        ZIO.scoped:
          for
            connection <- ZIO.service[Connection]
            _          <- NatsConnection.stream(connection, "BACKPRESSURE", "backpressure.>")
            producer   <- JetStreamProducer.make[String](connection)(value => s"backpressure.$value")(using Serde.utf8)
            _          <- ZIO.foreachDiscard(1 to 10)(i => producer.emit(i.toString))
            consumer   <- JetStreamBridgedConsumer.make[String](
                            connection,
                            "BACKPRESSURE",
                            "worker",
                            "backpressure.>",
                            JetStreamBridgedConsumer.Config(maxAckPending = 2),
                          )(using Serde.utf8)
            gate       <- Promise.make[Nothing, Unit] // never completed → handlers never ack
            delivered  <- Ref.make(0)
            // many concurrent consumes; each takes a message and blocks it unacked
            fiber      <- ZIO.foreachParDiscard(1 to 10)(_ => consumer.consume(_ => delivered.update(_ + 1) *> gate.await)).fork
            _          <- delivered.get.repeatUntil(_ >= 2) // at least maxAckPending get through
            _          <- ZIO.sleep(1.second)               // give the cap a chance to be exceeded
            count      <- delivered.get
            _          <- fiber.interrupt
          yield assertTrue(count == 2) // exactly maxAckPending — the cap held
      },
    ),
    suite("batched")(
      test("core batched: drains ephemeral messages in batches") {
        ZIO.scoped:
          for
            connection <- ZIO.service[Connection]
            consumer   <- CoreBatchedConsumer.make[String](connection, "core.batch.*", 10)(using Serde.utf8)
            producer    = CoreProducer.make[String](connection)(value => s"core.batch.$value")(using Serde.utf8)
            _          <- ZIO.foreachDiscard(1 to 10)(i => producer.emit(i.toString))
            received   <- Ref.make(List.empty[String])
            done       <- Promise.make[Nothing, Unit]
            logic       = (batch: List[String]) =>
                            received.updateAndGet(_ ++ batch).flatMap(all => ZIO.when(all.size == 10)(done.succeed(())).unit)
            fiber      <- consumer.consume(logic).forever.fork
            _          <- done.await
            out        <- received.get
            _          <- fiber.interrupt
          yield assertTrue(out.toSet == (1 to 10).map(_.toString).toSet)
      },
      test("polling batched: fetches a durable batch and acks it") {
        ZIO.scoped:
          for
            connection <- ZIO.service[Connection]
            _          <- NatsConnection.stream(connection, "BATCH_POLL", "batchpoll.>")
            producer   <- JetStreamProducer.make[String](connection)(value => s"batchpoll.$value")(using Serde.utf8)
            consumer   <- JetStreamPollingBatchedConsumer.make[String](
                            connection,
                            "BATCH_POLL",
                            "worker",
                            "batchpoll.>",
                            JetStreamPollingBatchedConsumer.Config(batchSize = 10),
                          )(using Serde.utf8)
            _          <- ZIO.foreachDiscard(1 to 10)(i => producer.emit(i.toString))
            received   <- Ref.make(List.empty[String])
            done       <- Promise.make[Nothing, Unit]
            logic       = (batch: List[String]) =>
                            received.updateAndGet(_ ++ batch).flatMap(all => ZIO.when(all.size == 10)(done.succeed(())).unit)
            fiber      <- consumer.consume(logic).forever.fork
            _          <- done.await
            out        <- received.get
            _          <- fiber.interrupt
          yield assertTrue(out.toSet == (1 to 10).map(_.toString).toSet)
      },
      test("bridged batched: drains the bridge queue in batches and acks") {
        ZIO.scoped:
          for
            connection <- ZIO.service[Connection]
            _          <- NatsConnection.stream(connection, "BATCH_BRIDGE", "batchbridge.>")
            producer   <- JetStreamProducer.make[String](connection)(value => s"batchbridge.$value")(using Serde.utf8)
            consumer   <- JetStreamBridgedBatchedConsumer.make[String](
                            connection,
                            "BATCH_BRIDGE",
                            "worker",
                            "batchbridge.>",
                            JetStreamBridgedBatchedConsumer.Config(batchSize = 10),
                          )(using Serde.utf8)
            _          <- ZIO.foreachDiscard(1 to 10)(i => producer.emit(i.toString))
            received   <- Ref.make(List.empty[String])
            done       <- Promise.make[Nothing, Unit]
            logic       = (batch: List[String]) =>
                            received.updateAndGet(_ ++ batch).flatMap(all => ZIO.when(all.size == 10)(done.succeed(())).unit)
            fiber      <- consumer.consume(logic).forever.fork
            _          <- done.await
            out        <- received.get
            _          <- fiber.interrupt
          yield assertTrue(out.toSet == (1 to 10).map(_.toString).toSet)
      },
      test("batched Discard: an undecodable message is termed, the rest of the batch delivered") {
        val intSerde: Serde[Int] = new Serde[Int]:
          def encode(value: Int): Array[Byte] = value.toString.getBytes(StandardCharsets.UTF_8)
          def decode(bytes: Array[Byte]): Either[String, Int] =
            new String(bytes, StandardCharsets.UTF_8).toIntOption.toRight("not an int")

        ZIO.scoped:
          for
            connection <- ZIO.service[Connection]
            _          <- NatsConnection.stream(connection, "BATCH_DLQ", "batchdlq.>")
            producer   <- JetStreamProducer.make[String](connection)(_ => "batchdlq.in")(using Serde.utf8)
            consumer   <- JetStreamPollingBatchedConsumer.make[Int](
                            connection,
                            "BATCH_DLQ",
                            "worker",
                            "batchdlq.>",
                            JetStreamPollingBatchedConsumer.Config(batchSize = 10, onDecodeFailure = DecodeFailurePolicy.Discard),
                          )(using intSerde)
            _          <- producer.emit("1")
            _          <- producer.emit("oops") // undecodable → termed, excluded from the batch
            _          <- producer.emit("2")
            received   <- Ref.make(Set.empty[Int])
            done       <- Promise.make[Nothing, Unit]
            logic       = (batch: List[Int]) =>
                            received.updateAndGet(_ ++ batch).flatMap(all => ZIO.when(all == Set(1, 2))(done.succeed(())).unit)
            fiber      <- consumer.consume(logic).forever.fork
            _          <- done.await
            out        <- received.get
            _          <- fiber.interrupt
          yield assertTrue(out == Set(1, 2))
      },
      test("batched Surface: a poison message fails the whole batch (blast-radius)") {
        val intSerde: Serde[Int] = new Serde[Int]:
          def encode(value: Int): Array[Byte] = value.toString.getBytes(StandardCharsets.UTF_8)
          def decode(bytes: Array[Byte]): Either[String, Int] =
            new String(bytes, StandardCharsets.UTF_8).toIntOption.toRight("not an int")

        ZIO.scoped:
          for
            connection <- ZIO.service[Connection]
            _          <- NatsConnection.stream(connection, "BATCH_SURFACE", "batchsurface.>")
            producer   <- JetStreamProducer.make[String](connection)(_ => "batchsurface.in")(using Serde.utf8)
            // batchSize == the number published, so one fetch fills with all three (poison included)
            consumer   <- JetStreamPollingBatchedConsumer.make[Int](
                            connection,
                            "BATCH_SURFACE",
                            "worker",
                            "batchsurface.>",
                            JetStreamPollingBatchedConsumer.Config(batchSize = 3),
                          )(using intSerde)
            _          <- producer.emit("1")
            _          <- producer.emit("oops")
            _          <- producer.emit("3")
            outcome    <- consumer.consume(_ => ZIO.unit).either
          yield assertTrue(outcome match { case Left(NatsError.Decode(_)) => true; case _ => false })
      },
    ),
  ).provideShared(NatsSpecLayers.connection) @@ TestAspect.withLiveClock @@ TestAspect.timeout(90.seconds) @@ TestAspect.sequential
