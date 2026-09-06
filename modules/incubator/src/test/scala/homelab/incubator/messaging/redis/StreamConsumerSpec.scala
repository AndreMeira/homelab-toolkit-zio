package homelab.incubator.messaging.redis


import io.lettuce.core.cluster.api.sync.RedisClusterCommands
import io.lettuce.core.{ Consumer as Group, Limit, Range, RedisClient }
import zio.*
import zio.test.*

import scala.jdk.CollectionConverters.*


/**
 * What a consumer group has to get right: an acknowledged entry leaves the pending list, a failed one does
 * not, and something else can take over what a dead consumer was holding.
 *
 * Every assertion reads the group's pending list rather than the consumer's own state — that list is the
 * commit boundary this adapter borrows, so it is the thing worth checking.
 */
object StreamConsumerSpec extends ZIOSpecDefault:

  private val block = 2.seconds

  /** How many entries the group has delivered and not had acknowledged. */
  private def pending(redis: RedisClusterCommands[String, Array[Byte]], stream: String, group: String): Task[Long] =
    ZIO.attemptBlocking(redis.xpending(stream, group).getCount)

  /** How many of those are held by one consumer. */
  private def pendingFor(
    redis: RedisClusterCommands[String, Array[Byte]],
    stream: String,
    group: String,
    name: String,
  ): Task[Int] =
    ZIO.attemptBlocking(redis.xpending(stream, Group.from(group, name), Range.unbounded[String](), Limit.from(10)).size)

  def spec: Spec[TestEnvironment & Scope, Any] = suite("StreamConsumer")(
    test("an entry whose logic succeeds is acknowledged") {
      for
        client   <- ZIO.service[RedisClient]
        stream    = s"group-ack-${scala.util.Random.nextInt(1 << 16)}"
        writer   <- RedisSpecLayers.connection(client)
        reader   <- RedisSpecLayers.connection(client)
        consumer <- StreamConsumer.make(reader, stream, "workers", "one", block, keepalive = None)
        _        <- RedisSpecLayers.publish(writer, stream, "done")
        seen     <- Ref.make(Chunk.empty[String])
        _        <- consumer.consume(entry => seen.update(_ :+ RedisSpecLayers.valueOf(entry)))
        got      <- seen.get
        owed     <- pending(writer, stream, "workers")
      yield assertTrue(got == Chunk("done"), owed == 0L)
    },
    test("an entry whose logic fails stays pending, and consume does not fail") {
      // Redeliver is doing nothing on purpose: the entry is already recorded as delivered-and-unacked, so
      // leaving it is what makes a peer able to take it.
      for
        client   <- ZIO.service[RedisClient]
        stream    = s"group-redeliver-${scala.util.Random.nextInt(1 << 16)}"
        writer   <- RedisSpecLayers.connection(client)
        reader   <- RedisSpecLayers.connection(client)
        consumer <- StreamConsumer.make(reader, stream, "workers", "one", block, keepalive = None)
        _        <- RedisSpecLayers.publish(writer, stream, "boom")
        outcome  <- consumer.consume(_ => ZIO.fail(StreamError.Unavailable("handler"))).exit
        owed     <- pending(writer, stream, "workers")
      yield assertTrue(outcome.isSuccess, owed == 1L)
    },
    test("a pending entry can be reclaimed by a peer") {
      // The recovery story: the consumer that was holding it is gone, and `after = 0` says everything is
      // fair game — a real caller passes the longest a handler may take.
      for
        client   <- ZIO.service[RedisClient]
        stream    = s"group-reclaim-${scala.util.Random.nextInt(1 << 16)}"
        writer   <- RedisSpecLayers.connection(client)
        reader   <- RedisSpecLayers.connection(client)
        consumer <- StreamConsumer.make(reader, stream, "workers", "one", block, keepalive = None)
        _        <- RedisSpecLayers.publish(writer, stream, "stranded")
        _        <- consumer.consume(_ => ZIO.fail(StreamError.Unavailable("handler")))
        held     <- pendingFor(writer, stream, "workers", "one")
        cursor   <- StreamConsumer.reclaim(writer, stream, "workers", "two", after = Duration.Zero)
        moved    <- pendingFor(writer, stream, "workers", "two")
        left     <- pendingFor(writer, stream, "workers", "one")
      yield assertTrue(held == 1, moved == 1, left == 0, cursor == "0-0")
    },
    test("Discard acknowledges and removes the entry") {
      for
        client   <- ZIO.service[RedisClient]
        stream    = s"group-discard-${scala.util.Random.nextInt(1 << 16)}"
        writer   <- RedisSpecLayers.connection(client)
        reader   <- RedisSpecLayers.connection(client)
        consumer <- StreamConsumer.make(
                      reader,
                      stream,
                      "workers",
                      "one",
                      block,
                      keepalive = None,
                      onFailure = StreamConsumer.OnFailure.Discard,
                    )
        _        <- RedisSpecLayers.publish(writer, stream, "poison")
        _        <- consumer.consume(_ => ZIO.fail(StreamError.Unavailable("handler")))
        owed     <- pending(writer, stream, "workers")
        length   <- ZIO.attemptBlocking(writer.xlen(stream))
      yield assertTrue(owed == 0L, length == 0L)
    },
    test("a handler slower than the keepalive still completes and acknowledges") {
      // Exercises the keepalive path: a ticking self-claim must not interrupt the work or leak a fiber.
      for
        client   <- ZIO.service[RedisClient]
        stream    = s"group-keepalive-${scala.util.Random.nextInt(1 << 16)}"
        writer   <- RedisSpecLayers.connection(client)
        reader   <- RedisSpecLayers.connection(client)
        consumer <- StreamConsumer.make(reader, stream, "workers", "one", block, keepalive = Some(200.millis))
        _        <- RedisSpecLayers.publish(writer, stream, "slow")
        _        <- consumer.consume(_ => ZIO.sleep(1.second))
        owed     <- pending(writer, stream, "workers")
      yield assertTrue(owed == 0L)
    },
  ).provideSomeShared[Scope](RedisSpecLayers.client) @@ TestAspect.withLiveClock @@ TestAspect.sequential
