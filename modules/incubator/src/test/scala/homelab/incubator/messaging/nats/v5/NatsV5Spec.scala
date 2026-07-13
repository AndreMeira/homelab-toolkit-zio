package homelab.incubator.messaging.nats.v5


import homelab.incubator.messaging.nats.v5.FailurePolicy.{ DecodeFailurePolicy, HandlerFailurePolicy }
import homelab.incubator.messaging.nats.v5.core.{ BatchConsumer as CoreBatchConsumer, Consumer as CoreConsumer, CoreSubscriber, Producer as CoreProducer }
import homelab.incubator.messaging.nats.v5.stream.{ BatchConsumer as StreamBatchConsumer, Consumer as StreamConsumer, Producer as StreamProducer }
import io.nats.client.Connection
import zio.*
import zio.test.*

import java.nio.charset.StandardCharsets


/**
 * Integration tests for the NATS v5 sketch against a real broker (Testcontainers). Covers both substrates:
 * Core NATS (ephemeral) and JetStream (durable), through the per-item and batched consumers plus
 * redelivery, poison/term, backpressure, resumption, and heartbeat.
 *
 * Two v5-specific shapes to note:
 *   - '''Core subscribes lazily''' (on the first `consume`), so Core tests fork the consumer first and keep
 *     republishing until it's seen — never relying on a fire-and-forget publish landing before the SUB is
 *     live. JetStream is durable, so publish-before-consume is fine.
 *   - v5 has '''one JetStream consumer model''' (the async `consume`→queue bridge), so the v4 polling/bridged
 *     pairs collapse to a single consumer, and "resume across instances" is tested across restarts
 *     (sequential scopes) rather than two concurrent consumers.
 *
 * Each test uses its own subject/stream so they don't interfere. Requires a running Docker daemon.
 */
object NatsV5Spec extends ZIOSpecDefault:

  /** A `Serde[Int]` over decimal text — decoding fails on non-numeric payloads (the poison-message probe). */
  private val intSerde: Serde[Int] = new Serde[Int]:
    def encode(value: Int): Array[Byte] = value.toString.getBytes(StandardCharsets.UTF_8)
    def decode(bytes: Array[Byte]): Either[String, Int] =
      new String(bytes, StandardCharsets.UTF_8).toIntOption.toRight("not an int")

  def spec = suite("NATS v5 — Core + JetStream (integration)")(
    suite("core (ephemeral)")(
      test("round-trips a message: fork the consumer, publish until seen") {
        ZIO.scoped:
          for
            connection <- ZIO.service[Connection]
            consumer   <- CoreConsumer.make[String](connection, "core.orders.*")(using Serde.utf8)
            producer    = CoreProducer.make[String](connection)(order => s"core.orders.$order")(using Serde.utf8)
            received   <- Promise.make[Nothing, String]
            _          <- consumer.consume(message => received.succeed(message).unit).forever.forkScoped
            _          <- producer.emit("alpha").repeat(Schedule.spaced(100.millis)).forkScoped
            out        <- received.await
          yield assertTrue(out == "alpha")
      },
      test("a wildcard subscription receives messages across keyed subjects") {
        val expected = (1 to 5).map(_.toString).toSet
        ZIO.scoped:
          for
            connection <- ZIO.service[Connection]
            consumer   <- CoreConsumer.make[String](connection, "core.events.*")(using Serde.utf8)
            producer    = CoreProducer.make[String](connection)(event => s"core.events.$event")(using Serde.utf8)
            received   <- Ref.make(Set.empty[String])
            done       <- Promise.make[Nothing, Unit]
            _          <- consumer
                            .consume(message => received.updateAndGet(_ + message).flatMap(seen => ZIO.when(seen == expected)(done.succeed(())).unit))
                            .forever
                            .forkScoped
            _          <- ZIO.foreachDiscard(1 to 5)(i => producer.emit(i.toString)).repeat(Schedule.spaced(100.millis)).forkScoped
            _          <- done.await
            out        <- received.get
          yield assertTrue(out == expected)
      },
      test("Discard: an undecodable message is skipped and the consumer keeps delivering") {
        ZIO.scoped:
          for
            connection <- ZIO.service[Connection]
            consumer   <- CoreConsumer.make[Int](connection, "core.skip.*", CoreConsumer.Config(onDecodeFailure = DecodeFailurePolicy.Discard))(using intSerde)
            producer    = CoreProducer.make[String](connection)(_ => "core.skip.in")(using Serde.utf8)
            got        <- Promise.make[Nothing, Int]
            _          <- consumer.consume(value => got.succeed(value).unit).forever.forkScoped
            _          <- (producer.emit("oops") *> producer.emit("42")).repeat(Schedule.spaced(100.millis)).forkScoped
            out        <- got.await
          yield assertTrue(out == 42)
      },
      test("core batched: drains ephemeral messages in batches") {
        val expected = (1 to 10).map(_.toString).toSet
        ZIO.scoped:
          for
            connection <- ZIO.service[Connection]
            consumer   <- CoreBatchConsumer.make[String](connection, "core.batch.*", CoreBatchConsumer.Config(batchSize = 10))(using Serde.utf8)
            producer    = CoreProducer.make[String](connection)(value => s"core.batch.$value")(using Serde.utf8)
            received   <- Ref.make(Set.empty[String])
            done       <- Promise.make[Nothing, Unit]
            _          <- consumer
                            .consume(batch => received.updateAndGet(_ ++ batch).flatMap(seen => ZIO.when(seen == expected)(done.succeed(())).unit))
                            .forever
                            .forkScoped
            _          <- ZIO.foreachDiscard(1 to 10)(i => producer.emit(i.toString)).repeat(Schedule.spaced(100.millis)).forkScoped
            _          <- done.await
            out        <- received.get
          yield assertTrue(out == expected)
      },
      test("core batched Discard: drops undecodable messages and delivers the rest") {
        ZIO.scoped:
          for
            connection <- ZIO.service[Connection]
            consumer   <- CoreBatchConsumer.make[Int](connection, "core.skipbatch.*", CoreBatchConsumer.Config(batchSize = 10, onDecodeFailure = DecodeFailurePolicy.Discard))(using intSerde)
            producer    = CoreProducer.make[String](connection)(payload => s"core.skipbatch.$payload")(using Serde.utf8)
            received   <- Ref.make(Set.empty[Int])
            done       <- Promise.make[Nothing, Unit]
            _          <- consumer
                            .consume(batch => received.updateAndGet(_ ++ batch).flatMap(seen => ZIO.when(seen == Set(1, 2))(done.succeed(())).unit))
                            .forever
                            .forkScoped
            _          <- (producer.emit("1") *> producer.emit("oops") *> producer.emit("2")).repeat(Schedule.spaced(100.millis)).forkScoped
            _          <- done.await
            out        <- received.get
          yield assertTrue(out == Set(1, 2))
      },
      test("many consumers share one subscriber's dispatcher, each receiving its own subject") {
        val count = 20
        ZIO.scoped:
          for
            connection <- ZIO.service[Connection]
            subscriber <- CoreSubscriber.make(connection)
            promises   <- ZIO.foreach((1 to count).toList): i =>
                            for
                              consumer <- CoreConsumer.make[String](subscriber, s"core.shard.$i", CoreConsumer.Config())(using Serde.utf8)
                              promise  <- Promise.make[Nothing, String]
                              _        <- consumer.consume(message => promise.succeed(message).unit).forever.forkScoped
                            yield promise
            producer    = CoreProducer.make[String](connection)(payload => s"core.shard.$payload")(using Serde.utf8)
            _          <- ZIO.foreachDiscard(1 to count)(i => producer.emit(i.toString)).repeat(Schedule.spaced(100.millis)).forkScoped
            received   <- ZIO.foreach(promises)(_.await)
          yield assertTrue(received.toSet == (1 to count).map(_.toString).toSet)
      },
    ),
    suite("jetstream (durable)")(
      test("a durable message round-trips and is acked") {
        ZIO.scoped:
          for
            connection <- ZIO.service[Connection]
            _          <- NatsSpecLayers.stream(connection, "ORDERS", "orders.>")
            producer   <- StreamProducer.make[String](connection)(order => s"orders.$order")(using Serde.utf8)
            consumer   <- StreamConsumer.make[String](connection, "ORDERS", "worker", "orders.>")(using Serde.utf8)
            _          <- producer.emit("alpha") // durable — publish before consume is fine
            received   <- Ref.make(Option.empty[String])
            _          <- consumer.consume(message => received.set(Some(message)))
            out        <- received.get
          yield assertTrue(out == Some("alpha"))
      },
      test("a failing handler naks; the message is redelivered and eventually acked (at-least-once)") {
        ZIO.scoped:
          for
            connection <- ZIO.service[Connection]
            _          <- NatsSpecLayers.stream(connection, "REDELIVER", "redeliver.>")
            producer   <- StreamProducer.make[String](connection)(value => s"redeliver.$value")(using Serde.utf8)
            consumer   <- StreamConsumer.make[String](connection, "REDELIVER", "worker", "redeliver.>", StreamConsumer.Config(ackWait = 2.seconds))(using Serde.utf8)
            attempts   <- Ref.make(0)
            done       <- Promise.make[Nothing, Unit]
            logic       = (_: String) =>
                            attempts.updateAndGet(_ + 1).flatMap: attempt =>
                              if attempt == 1 then ZIO.fail(NatsError.Decode("forced first-attempt failure"))
                              else done.succeed(()).unit
            _          <- producer.emit("x")
            _          <- consumer.consume(logic).forever.forkScoped
            _          <- done.await
            count      <- attempts.get
          yield assertTrue(count >= 2)
      },
      test("an undecodable payload fails the consumer by default (Surface — non-destructive)") {
        ZIO.scoped:
          for
            connection <- ZIO.service[Connection]
            _          <- NatsSpecLayers.stream(connection, "POISON_SURFACE", "surface.>")
            producer   <- StreamProducer.make[String](connection)(_ => "surface.in")(using Serde.utf8)
            consumer   <- StreamConsumer.make[Int](connection, "POISON_SURFACE", "worker", "surface.>")(using intSerde)
            _          <- producer.emit("oops")
            outcome    <- consumer.consume(_ => ZIO.unit).either
          yield assertTrue(outcome match { case Left(NatsError.Decode(_)) => true; case _ => false })
      },
      test("with DecodeFailurePolicy.Discard a poison message is termed once and the consumer continues") {
        val decodeCount = new java.util.concurrent.atomic.AtomicInteger(0)
        val countingSerde: Serde[Int] = new Serde[Int]:
          def encode(value: Int): Array[Byte] = value.toString.getBytes(StandardCharsets.UTF_8)
          def decode(bytes: Array[Byte]): Either[String, Int] =
            decodeCount.incrementAndGet()
            new String(bytes, StandardCharsets.UTF_8).toIntOption.toRight("not an int")

        ZIO.scoped:
          for
            connection <- ZIO.service[Connection]
            _          <- NatsSpecLayers.stream(connection, "POISON_DLQ", "dlq.>")
            producer   <- StreamProducer.make[String](connection)(_ => "dlq.in")(using Serde.utf8)
            consumer   <- StreamConsumer.make[Int](connection, "POISON_DLQ", "worker", "dlq.>", StreamConsumer.Config(onDecodeFailure = DecodeFailurePolicy.Discard))(using countingSerde)
            good       <- Promise.make[Nothing, Int]
            _          <- producer.emit("oops") // undecodable → term (dropped, not redelivered)
            _          <- producer.emit("42")   // decodable → delivered next
            _          <- consumer.consume(value => good.succeed(value).unit).forever.forkScoped
            value      <- good.await
          yield assertTrue(value == 42, decodeCount.get == 2) // oops decoded once (termed, no redelivery), 42 once
      },
      test("with HandlerFailurePolicy.Surface a handler failure surfaces from consume") {
        ZIO.scoped:
          for
            connection <- ZIO.service[Connection]
            _          <- NatsSpecLayers.stream(connection, "HANDLER_SURFACE", "hsurface.>")
            producer   <- StreamProducer.make[String](connection)(_ => "hsurface.in")(using Serde.utf8)
            consumer   <- StreamConsumer.make[String](connection, "HANDLER_SURFACE", "worker", "hsurface.>", StreamConsumer.Config(onHandlerFailure = HandlerFailurePolicy.Surface))(using Serde.utf8)
            _          <- producer.emit("x")
            outcome    <- consumer.consume(_ => ZIO.fail(NatsError.Decode("handler boom"))).either
          yield assertTrue(outcome match { case Left(NatsError.Decode("handler boom")) => true; case _ => false })
      },
      test("with HandlerFailurePolicy.Discard a failed message is termed once and the consumer continues") {
        ZIO.scoped:
          for
            connection <- ZIO.service[Connection]
            _          <- NatsSpecLayers.stream(connection, "HANDLER_DLQ", "hdlq.>")
            producer   <- StreamProducer.make[String](connection)(payload => s"hdlq.$payload")(using Serde.utf8)
            consumer   <- StreamConsumer.make[String](connection, "HANDLER_DLQ", "worker", "hdlq.>", StreamConsumer.Config(ackWait = 2.seconds, onHandlerFailure = HandlerFailurePolicy.Discard))(using Serde.utf8)
            attempts   <- Ref.make(0)
            good       <- Promise.make[Nothing, String]
            logic       = (message: String) =>
                            if message == "bad" then attempts.update(_ + 1) *> ZIO.fail(NatsError.Decode("boom"))
                            else good.succeed(message).unit
            _          <- producer.emit("bad")  // handler fails → term (not redelivered)
            _          <- producer.emit("good") // processed next
            _          <- consumer.consume(logic).forever.forkScoped
            value      <- good.await
            count      <- attempts.get
          yield assertTrue(value == "good", count == 1) // "bad" attempted once, termed, not redelivered
      },
      test("consumes many messages across keyed subjects under a wildcard filter") {
        val expected = (1 to 20).map(_.toString).toSet
        ZIO.scoped:
          for
            connection <- ZIO.service[Connection]
            _          <- NatsSpecLayers.stream(connection, "SHARDED", "sharded.>")
            producer   <- StreamProducer.make[String](connection)(value => s"sharded.$value")(using Serde.utf8)
            consumer   <- StreamConsumer.make[String](connection, "SHARDED", "worker", "sharded.>")(using Serde.utf8)
            _          <- ZIO.foreachDiscard(1 to 20)(i => producer.emit(i.toString))
            received   <- Ref.make(Set.empty[String])
            done       <- Promise.make[Nothing, Unit]
            _          <- consumer
                            .consume(message => received.updateAndGet(_ + message).flatMap(seen => ZIO.when(seen == expected)(done.succeed(())).unit))
                            .forever
                            .forkScoped
            _          <- done.await
            out        <- received.get
          yield assertTrue(out == expected)
      },
      test("heartbeat keeps a slow handler's message from being redelivered") {
        ZIO.scoped:
          for
            connection <- ZIO.service[Connection]
            _          <- NatsSpecLayers.stream(connection, "HEARTBEAT", "heartbeat.>")
            producer   <- StreamProducer.make[String](connection)(_ => "heartbeat.in")(using Serde.utf8)
            consumer   <- StreamConsumer.make[String](connection, "HEARTBEAT", "worker", "heartbeat.>", StreamConsumer.Config(ackWait = 2.seconds, heartbeat = Some(500.millis)))(using Serde.utf8)
            attempts   <- Ref.make(0)
            done       <- Promise.make[Nothing, Unit]
            // the handler runs longer than ackWait; the heartbeat should keep it from redelivering
            logic       = (_: String) => attempts.update(_ + 1) *> ZIO.sleep(4.seconds) *> done.succeed(()).unit
            _          <- producer.emit("slow")
            _          <- consumer.consume(logic).forever.forkScoped
            _          <- done.await
            _          <- ZIO.sleep(1.second) // let any (unexpected) redelivery bump the count
            count      <- attempts.get
          yield assertTrue(count == 1)
      },
      test("a durable publish to a subject no stream captures fails with Publish") {
        ZIO.scoped:
          for
            connection <- ZIO.service[Connection]
            producer   <- StreamProducer.make[String](connection)(_ => "nostream.subject")(using Serde.utf8)
            outcome    <- producer.emit("x").either
          yield assertTrue(outcome match { case Left(NatsError.Publish(_)) => true; case _ => false })
      },
      test("a durable consumer resumes from its last ack across restarts (shared progress)") {
        ZIO.scoped:
          for
            connection <- ZIO.service[Connection]
            _          <- NatsSpecLayers.stream(connection, "RESUME", "resume.>")
            producer   <- StreamProducer.make[String](connection)(value => s"resume.$value")(using Serde.utf8)
            _          <- ZIO.foreachDiscard(1 to 6)(i => producer.emit(i.toString))
            firstSeen  <- Ref.make(List.empty[String])
            // first instance consumes and acks 3, then is torn down — releasing the rest
            _          <- ZIO.scoped:
                            for
                              first <- StreamConsumer.make[String](connection, "RESUME", "worker", "resume.>")(using Serde.utf8)
                              _     <- ZIO.foreachDiscard(1 to 3)(_ => first.consume(message => firstSeen.update(_ :+ message)))
                            yield ()
            secondSeen <- Ref.make(List.empty[String])
            // a fresh instance with the SAME durable name picks up where the first left off
            _          <- ZIO.scoped:
                            for
                              second <- StreamConsumer.make[String](connection, "RESUME", "worker", "resume.>")(using Serde.utf8)
                              _      <- ZIO.foreachDiscard(1 to 3)(_ => second.consume(message => secondSeen.update(_ :+ message)))
                            yield ()
            one        <- firstSeen.get
            two        <- secondSeen.get
          yield assertTrue(one == List("1", "2", "3"), two == List("4", "5", "6"))
      },
      test("maxAckPending withholds delivery past the in-flight bound") {
        ZIO.scoped:
          for
            connection <- ZIO.service[Connection]
            _          <- NatsSpecLayers.stream(connection, "BACKPRESSURE", "backpressure.>")
            producer   <- StreamProducer.make[String](connection)(value => s"backpressure.$value")(using Serde.utf8)
            _          <- ZIO.foreachDiscard(1 to 10)(i => producer.emit(i.toString))
            consumer   <- StreamConsumer.make[String](connection, "BACKPRESSURE", "worker", "backpressure.>", StreamConsumer.Config(maxAckPending = 2))(using Serde.utf8)
            gate       <- Promise.make[Nothing, Unit] // never completed → handlers never ack
            delivered  <- Ref.make(0)
            // many concurrent consumes; each takes a message and blocks it unacked
            _          <- ZIO.foreachParDiscard(1 to 10)(_ => consumer.consume(_ => delivered.update(_ + 1) *> gate.await)).forkScoped
            _          <- delivered.get.repeatUntil(_ >= 2) // at least maxAckPending get through
            _          <- ZIO.sleep(1.second)               // give the cap a chance to be exceeded
            count      <- delivered.get
          yield assertTrue(count == 2) // exactly maxAckPending — the cap held
      },
    ),
    suite("jetstream batched")(
      test("drains a durable batch and acks it") {
        val expected = (1 to 10).map(_.toString).toSet
        ZIO.scoped:
          for
            connection <- ZIO.service[Connection]
            _          <- NatsSpecLayers.stream(connection, "BATCH", "batch.>")
            producer   <- StreamProducer.make[String](connection)(value => s"batch.$value")(using Serde.utf8)
            consumer   <- StreamBatchConsumer.make[String](connection, "BATCH", "worker", "batch.>", StreamBatchConsumer.Config(batchSize = 10))(using Serde.utf8)
            _          <- ZIO.foreachDiscard(1 to 10)(i => producer.emit(i.toString))
            received   <- Ref.make(Set.empty[String])
            done       <- Promise.make[Nothing, Unit]
            _          <- consumer
                            .consume(batch => received.updateAndGet(_ ++ batch).flatMap(seen => ZIO.when(seen == expected)(done.succeed(())).unit))
                            .forever
                            .forkScoped
            _          <- done.await
            out        <- received.get
          yield assertTrue(out == expected)
      },
      test("batched Discard: an undecodable message is termed, the rest delivered") {
        ZIO.scoped:
          for
            connection <- ZIO.service[Connection]
            _          <- NatsSpecLayers.stream(connection, "BATCH_DLQ", "batchdlq.>")
            producer   <- StreamProducer.make[String](connection)(_ => "batchdlq.in")(using Serde.utf8)
            consumer   <- StreamBatchConsumer.make[Int](connection, "BATCH_DLQ", "worker", "batchdlq.>", StreamBatchConsumer.Config(batchSize = 10, onDecodeFailure = DecodeFailurePolicy.Discard))(using intSerde)
            received   <- Ref.make(Set.empty[Int])
            done       <- Promise.make[Nothing, Unit]
            _          <- consumer
                            .consume(batch => received.updateAndGet(_ ++ batch).flatMap(seen => ZIO.when(seen == Set(1, 2))(done.succeed(())).unit))
                            .forever
                            .forkScoped
            _          <- producer.emit("1")
            _          <- producer.emit("oops") // undecodable → termed, excluded from the batch
            _          <- producer.emit("2")
            _          <- done.await
            out        <- received.get
          yield assertTrue(out == Set(1, 2))
      },
      test("batched Surface: a poison message eventually surfaces Decode (blast-radius)") {
        ZIO.scoped:
          for
            connection <- ZIO.service[Connection]
            _          <- NatsSpecLayers.stream(connection, "BATCH_SURFACE", "batchsurface.>")
            producer   <- StreamProducer.make[String](connection)(_ => "batchsurface.in")(using Serde.utf8)
            consumer   <- StreamBatchConsumer.make[Int](connection, "BATCH_SURFACE", "worker", "batchsurface.>", StreamBatchConsumer.Config(batchSize = 3))(using intSerde)
            _          <- producer.emit("1")
            _          <- producer.emit("oops")
            _          <- producer.emit("3")
            // v5's bridged batched drains opportunistically, so we can't force a mixed batch; instead drain
            // until a batch containing the poison surfaces (good messages before it are acked; any sharing
            // its batch are sacrificed — the blast radius).
            outcome    <- consumer.consume(_ => ZIO.unit).either.repeatUntil(_.isLeft)
          yield assertTrue(outcome match { case Left(NatsError.Decode(_)) => true; case _ => false })
      },
    ),
  ).provideShared(NatsSpecLayers.connection) @@ TestAspect.withLiveClock @@ TestAspect.timeout(90.seconds) @@ TestAspect.sequential
