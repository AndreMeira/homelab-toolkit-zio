package homelab.nats


import homelab.nats.core.{ BatchConsumer as CoreBatchConsumer, Consumer as CoreConsumer, CoreSubscriber, Producer as CoreProducer }
import homelab.nats.stream.{ BatchConsumer as StreamBatchConsumer, Consumer as StreamConsumer, Producer as StreamProducer }
import io.nats.client.Connection
import zio.*
import zio.test.*

import java.nio.charset.StandardCharsets


/**
 * Integration tests for the NATS adapter against a real broker (Testcontainers). Covers both substrates —
 * Core NATS (ephemeral) and JetStream (durable) — through the per-item and batched consumers plus
 * redelivery, poison/term, backpressure, resumption, and heartbeat.
 *
 * Two adapter-shape notes:
 *   - '''Core subscribes lazily''' (on the first `consume`), so Core tests fork the consumer first and keep
 *     republishing until it's seen — never relying on a fire-and-forget publish landing before the SUB is
 *     live. JetStream is durable, so publish-before-consume is fine.
 *   - There is '''one JetStream consumer model''' (the async `consume`→queue bridge), so "resume across
 *     instances" is tested across restarts (serial scopes) rather than two concurrent consumers, and the
 *     batched consumer drains opportunistically (whatever is buffered, not wait-to-fill).
 *
 * Each test uses its own subject/stream so they don't interfere. Requires a running Docker daemon.
 */
object NatsSpec extends ZIOSpecDefault:

  /** A `Decoder[Int]` over decimal text — decoding fails on non-numeric payloads (the poison-message probe). */
  private val intDecoder: Codec.Decoder[Int] =
    message => new String(message.getData, StandardCharsets.UTF_8).toIntOption.toRight("not an int")

  def spec = suite("NATS — Core + JetStream (integration)")(
    suite("core (ephemeral)")(
      test("round-trips a message: fork the consumer, publish until seen") {
        ZIO.scoped:
          for
            connection <- ZIO.service[Connection]
            consumer   <- CoreConsumer.make[String](connection, "core.orders.*")(using Codec.Decoder.utf8)
            producer    = CoreProducer.make[String](connection)(using Codec.Encoder.utf8(order => s"core.orders.$order"))
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
            consumer   <- CoreConsumer.make[String](connection, "core.events.*")(using Codec.Decoder.utf8)
            producer    = CoreProducer.make[String](connection)(using Codec.Encoder.utf8(event => s"core.events.$event"))
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
      test("an undecodable message aborts consume; a caller that ignores it keeps delivering") {
        ZIO.scoped:
          for
            connection <- ZIO.service[Connection]
            consumer   <- CoreConsumer.make[Int](connection, "core.skip.*")(using intDecoder)
            producer    = CoreProducer.make[String](connection)(using Codec.Encoder.utf8(_ => "core.skip.in"))
            got        <- Promise.make[Nothing, Int]
            // `.either` is where skipping lives now: Core has nothing to settle, so the loop owns the choice
            _          <- consumer.consume(value => got.succeed(value).unit).either.forever.forkScoped
            _          <- (producer.emit("oops") *> producer.emit("42")).repeat(Schedule.spaced(100.millis)).forkScoped
            out        <- got.await
          yield assertTrue(out == 42)
      },
      test("core batched: drains ephemeral messages in batches") {
        val expected = (1 to 10).map(_.toString).toSet
        ZIO.scoped:
          for
            connection <- ZIO.service[Connection]
            consumer   <- CoreBatchConsumer.make[String](connection, "core.batch.*", CoreBatchConsumer.Config(batchSize = 10))(using Codec.Decoder.utf8)
            producer    = CoreProducer.make[String](connection)(using Codec.Encoder.utf8(value => s"core.batch.$value"))
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
      test("core batched: a poison batch aborts consume; a caller that ignores it gets the rest") {
        ZIO.scoped:
          for
            connection <- ZIO.service[Connection]
            consumer   <- CoreBatchConsumer.make[Int](connection, "core.skipbatch.*", CoreBatchConsumer.Config(batchSize = 10))(using intDecoder)
            producer    = CoreProducer.make[String](connection)(using Codec.Encoder.utf8(payload => s"core.skipbatch.$payload"))
            received   <- Ref.make(Set.empty[Int])
            done       <- Promise.make[Nothing, Unit]
            // a batch holding the poison fails whole; republishing means 1 and 2 eventually land in clean ones
            _          <- consumer
                            .consume(batch => received.updateAndGet(_ ++ batch).flatMap(seen => ZIO.when(seen == Set(1, 2))(done.succeed(())).unit))
                            .either
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
                              consumer <- CoreConsumer.make[String](subscriber, s"core.shard.$i")(using Codec.Decoder.utf8)
                              promise  <- Promise.make[Nothing, String]
                              _        <- consumer.consume(message => promise.succeed(message).unit).forever.forkScoped
                            yield promise
            producer    = CoreProducer.make[String](connection)(using Codec.Encoder.utf8(payload => s"core.shard.$payload"))
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
            producer   <- StreamProducer.make[String](connection)(using Codec.Encoder.utf8(order => s"orders.$order"))
            consumer   <- StreamConsumer.make[String](connection, "ORDERS", "worker", "orders.>")(using Codec.Decoder.utf8)
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
            producer   <- StreamProducer.make[String](connection)(using Codec.Encoder.utf8(value => s"redeliver.$value"))
            consumer   <- StreamConsumer.make[String](connection, "REDELIVER", "worker", "redeliver.>", StreamConsumer.Config(ackWait = 2.seconds))(
                            using Codec.Decoder.utf8
                          )
            attempts   <- Ref.make(0)
            done       <- Promise.make[Nothing, Unit]
            logic       = (_: String) =>
                            attempts
                              .updateAndGet(_ + 1)
                              .flatMap: attempt =>
                                if attempt == 1 then ZIO.fail(NatsError.Decode("forced first-attempt failure"))
                                else done.succeed(()).unit
            _          <- producer.emit("x")
            _          <- consumer.consume(logic).forever.forkScoped
            _          <- done.await
            count      <- attempts.get
          yield assertTrue(count >= 2)
      },
      test("under Surface an undecodable payload fails the consumer, un-acked (non-destructive)") {
        ZIO.scoped:
          for
            connection <- ZIO.service[Connection]
            _          <- NatsSpecLayers.stream(connection, "POISON_SURFACE", "surface.>")
            producer   <- StreamProducer.make[String](connection)(using Codec.Encoder.utf8(_ => "surface.in"))
            consumer   <- StreamConsumer.make[Int](
                            connection,
                            "POISON_SURFACE",
                            "worker",
                            "surface.>",
                            StreamConsumer.Config(onFailure = HandlerFailurePolicy.Surface),
                          )(using intDecoder)
            _          <- producer.emit("oops")
            outcome    <- consumer.consume(_ => ZIO.unit).either
          yield assertTrue(outcome match { case Left(NatsError.Decode(_)) => true; case _ => false })
      },
      test("with Discard a poison message is termed once and the consumer continues") {
        val decodeCount                         = new java.util.concurrent.atomic.AtomicInteger(0)
        val countingDecoder: Codec.Decoder[Int] = message => {
          val _ = decodeCount.incrementAndGet()
          new String(message.getData, StandardCharsets.UTF_8).toIntOption.toRight("not an int")
        }

        ZIO.scoped:
          for
            connection <- ZIO.service[Connection]
            _          <- NatsSpecLayers.stream(connection, "POISON_DLQ", "dlq.>")
            producer   <- StreamProducer.make[String](connection)(using Codec.Encoder.utf8(_ => "dlq.in"))
            consumer   <- StreamConsumer.make[Int](
                            connection,
                            "POISON_DLQ",
                            "worker",
                            "dlq.>",
                            StreamConsumer.Config(onFailure = HandlerFailurePolicy.Discard),
                          )(using countingDecoder)
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
            producer   <- StreamProducer.make[String](connection)(using Codec.Encoder.utf8(_ => "hsurface.in"))
            consumer   <- StreamConsumer.make[String](
                            connection,
                            "HANDLER_SURFACE",
                            "worker",
                            "hsurface.>",
                            StreamConsumer.Config(onFailure = HandlerFailurePolicy.Surface),
                          )(using Codec.Decoder.utf8)
            _          <- producer.emit("x")
            outcome    <- consumer.consume(_ => ZIO.fail(NatsError.Decode("handler boom"))).either
          yield assertTrue(outcome match { case Left(NatsError.Decode("handler boom")) => true; case _ => false })
      },
      test("with HandlerFailurePolicy.Discard a failed message is termed once and the consumer continues") {
        ZIO.scoped:
          for
            connection <- ZIO.service[Connection]
            _          <- NatsSpecLayers.stream(connection, "HANDLER_DLQ", "hdlq.>")
            producer   <- StreamProducer.make[String](connection)(using Codec.Encoder.utf8(payload => s"hdlq.$payload"))
            consumer   <- StreamConsumer.make[String](
                            connection,
                            "HANDLER_DLQ",
                            "worker",
                            "hdlq.>",
                            StreamConsumer.Config(ackWait = 2.seconds, onFailure = HandlerFailurePolicy.Discard),
                          )(using Codec.Decoder.utf8)
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
            producer   <- StreamProducer.make[String](connection)(using Codec.Encoder.utf8(value => s"sharded.$value"))
            consumer   <- StreamConsumer.make[String](connection, "SHARDED", "worker", "sharded.>")(using Codec.Decoder.utf8)
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
            producer   <- StreamProducer.make[String](connection)(using Codec.Encoder.utf8(_ => "heartbeat.in"))
            consumer   <- StreamConsumer.make[String](
                            connection,
                            "HEARTBEAT",
                            "worker",
                            "heartbeat.>",
                            StreamConsumer.Config(ackWait = 2.seconds, heartbeat = Some(500.millis)),
                          )(using Codec.Decoder.utf8)
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
            producer   <- StreamProducer.make[String](connection)(using Codec.Encoder.utf8(_ => "nostream.subject"))
            outcome    <- producer.emit("x").either
          yield assertTrue(outcome match { case Left(NatsError.Publish(_)) => true; case _ => false })
      },
      test("a durable consumer resumes from its last ack across restarts (shared progress)") {
        ZIO.scoped:
          for
            connection <- ZIO.service[Connection]
            _          <- NatsSpecLayers.stream(connection, "RESUME", "resume.>")
            producer   <- StreamProducer.make[String](connection)(using Codec.Encoder.utf8(value => s"resume.$value"))
            _          <- ZIO.foreachDiscard(1 to 6)(i => producer.emit(i.toString))
            firstSeen  <- Ref.make(List.empty[String])
            // first instance consumes and acks 3, then is torn down — releasing the rest
            _          <- ZIO.scoped:
                            for
                              first <- StreamConsumer.make[String](connection, "RESUME", "worker", "resume.>")(using Codec.Decoder.utf8)
                              _     <- ZIO.foreachDiscard(1 to 3)(_ => first.consume(message => firstSeen.update(_ :+ message)))
                            yield ()
            secondSeen <- Ref.make(List.empty[String])
            // a fresh instance with the SAME durable name picks up where the first left off
            _          <- ZIO.scoped:
                            for
                              second <- StreamConsumer.make[String](connection, "RESUME", "worker", "resume.>")(using Codec.Decoder.utf8)
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
            producer   <- StreamProducer.make[String](connection)(using Codec.Encoder.utf8(value => s"backpressure.$value"))
            _          <- ZIO.foreachDiscard(1 to 10)(i => producer.emit(i.toString))
            consumer   <- StreamConsumer.make[String](
                            connection,
                            "BACKPRESSURE",
                            "worker",
                            "backpressure.>",
                            StreamConsumer.Config(maxAckPending = 2),
                          )(using Codec.Decoder.utf8)
            gate       <- Promise.make[Nothing, Unit]       // never completed → handlers never ack
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
            producer   <- StreamProducer.make[String](connection)(using Codec.Encoder.utf8(value => s"batch.$value"))
            consumer   <-
              StreamBatchConsumer.make[String](connection, "BATCH", "worker", "batch.>", StreamBatchConsumer.Config(batchSize = 10))(
                using Codec.Decoder.utf8
              )
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
      test("batched Discard: a poison message terms its whole batch, and the consumer continues") {
        ZIO.scoped:
          for
            connection <- ZIO.service[Connection]
            _          <- NatsSpecLayers.stream(connection, "BATCH_DLQ", "batchdlq.>")
            producer   <- StreamProducer.make[String](connection)(using Codec.Encoder.utf8(_ => "batchdlq.in"))
            consumer   <- StreamBatchConsumer.make[Int](
                            connection,
                            "BATCH_DLQ",
                            "worker",
                            "batchdlq.>",
                            StreamBatchConsumer.Config(batchSize = 10, onFailure = HandlerFailurePolicy.Discard),
                          )(using intDecoder)
            received   <- Ref.make(Set.empty[Int])
            done       <- Promise.make[Nothing, Unit]
            // Publish the poison alone and drain it first, so it is a batch of its own: under Discard the
            // whole batch is termed, which is now the unit of blast radius. Good messages published after it
            // are then delivered, proving the consumer carried on rather than wedging.
            _          <- producer.emit("oops")
            _          <- consumer.consume(_ => ZIO.unit) // drains and terms the poison batch, without failing
            _          <- producer.emit("1")
            _          <- producer.emit("2")
            _          <- consumer
                            .consume(batch => received.updateAndGet(_ ++ batch).flatMap(seen => ZIO.when(seen == Set(1, 2))(done.succeed(())).unit))
                            .forever
                            .forkScoped
            _          <- done.await
            out        <- received.get
          yield assertTrue(out == Set(1, 2))
      },
      test("batched Surface: a poison message eventually surfaces Decode (blast-radius)") {
        ZIO.scoped:
          for
            connection <- ZIO.service[Connection]
            _          <- NatsSpecLayers.stream(connection, "BATCH_SURFACE", "batchsurface.>")
            producer   <- StreamProducer.make[String](connection)(using Codec.Encoder.utf8(_ => "batchsurface.in"))
            consumer   <-
              StreamBatchConsumer.make[Int](
                connection,
                "BATCH_SURFACE",
                "worker",
                "batchsurface.>",
                StreamBatchConsumer.Config(batchSize = 3, onFailure = HandlerFailurePolicy.Surface),
              )(
                using intDecoder
              )
            _          <- producer.emit("1")
            _          <- producer.emit("oops")
            _          <- producer.emit("3")
            // the bridged batched drains opportunistically, so we can't force a mixed batch; instead drain
            // until a batch containing the poison surfaces (good messages before it are acked; any sharing its
            // batch are sacrificed — the blast radius).
            outcome    <- consumer.consume(_ => ZIO.unit).either.repeatUntil(_.isLeft)
          yield assertTrue(outcome match { case Left(NatsError.Decode(_)) => true; case _ => false })
      },
    ),
  ).provideShared(NatsSpecLayers.connection) @@ TestAspect.withLiveClock @@ TestAspect.timeout(90.seconds) @@ TestAspect.sequential
