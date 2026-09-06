package homelab.incubator.messaging.redis


import io.lettuce.core.RedisClient
import zio.*
import zio.test.*


/**
 * What tailing has to get right: everyone hears everything, nobody hears it twice, and a failure does not
 * swallow what it failed on.
 */
object StreamTailConsumerSpec extends ZIOSpecDefault:

  private val block = 2.seconds

  def spec: Spec[TestEnvironment & Scope, Any] = suite("StreamTailConsumer")(
    test("every reader sees every entry") {
      // The property a list could not give: two independent readers, one entry each, not one and none.
      for
        client  <- ZIO.service[RedisClient]
        stream   = s"tail-broadcast-${scala.util.Random.nextInt(1 << 16)}"
        writer  <- RedisSpecLayers.connection(client)
        one     <- RedisSpecLayers.connection(client).flatMap(StreamTailConsumer.make(_, NonEmptyChunk(stream), block))
        two     <- RedisSpecLayers.connection(client).flatMap(StreamTailConsumer.make(_, NonEmptyChunk(stream), block))
        _       <- RedisSpecLayers.publish(writer, stream, "hello")
        seenOne <- Ref.make(Chunk.empty[String])
        seenTwo <- Ref.make(Chunk.empty[String])
        _       <- one.consume(batch => seenOne.update(_ ++ Chunk.fromIterable(batch.map(RedisSpecLayers.valueOf))))
        _       <- two.consume(batch => seenTwo.update(_ ++ Chunk.fromIterable(batch.map(RedisSpecLayers.valueOf))))
        first   <- seenOne.get
        second  <- seenTwo.get
      yield assertTrue(first == Chunk("hello"), second == Chunk("hello"))
    },
    test("an entry already delivered is not delivered again") {
      // The offset moved past it, so a second call finds nothing and returns when the block expires.
      for
        client <- ZIO.service[RedisClient]
        stream  = s"tail-offset-${scala.util.Random.nextInt(1 << 16)}"
        writer <- RedisSpecLayers.connection(client)
        tail   <- RedisSpecLayers.connection(client).flatMap(StreamTailConsumer.make(_, NonEmptyChunk(stream), block))
        _      <- RedisSpecLayers.publish(writer, stream, "once")
        seen   <- Ref.make(Chunk.empty[String])
        record  = (batch: List[io.lettuce.core.StreamMessage[String, Array[Byte]]]) =>
                    seen.update(_ ++ Chunk.fromIterable(batch.map(RedisSpecLayers.valueOf)))
        _      <- tail.consume(record)
        _      <- tail.consume(record)
        got    <- seen.get
      yield assertTrue(got == Chunk("once"))
    },
    test("a failed batch is offered again, because the offset did not move") {
      // Offsets advance after the logic, so at-least-once: the entry a handler died on comes back.
      for
        client   <- ZIO.service[RedisClient]
        stream    = s"tail-retry-${scala.util.Random.nextInt(1 << 16)}"
        writer   <- RedisSpecLayers.connection(client)
        tail     <- RedisSpecLayers.connection(client).flatMap(StreamTailConsumer.make(_, NonEmptyChunk(stream), block))
        _        <- RedisSpecLayers.publish(writer, stream, "sticky")
        attempts <- Ref.make(0)
        failed   <- tail.consume(_ => attempts.update(_ + 1) *> ZIO.fail(StreamError.Unavailable("handler"))).flip
        again    <- Ref.make(Chunk.empty[String])
        _        <- tail.consume(batch => again.update(_ ++ Chunk.fromIterable(batch.map(RedisSpecLayers.valueOf))))
        tried    <- attempts.get
        second   <- again.get
      yield assertTrue(failed.isInstanceOf[StreamError], tried == 1, second == Chunk("sticky"))
    },
    test("a stream is read from where the consumer was made, not from its beginning") {
      // Positions resolve at construction: a notification nobody was listening for has nothing to wake, so
      // what preceded the consumer is not delivered to it.
      for
        client <- ZIO.service[RedisClient]
        stream  = s"tail-start-${scala.util.Random.nextInt(1 << 16)}"
        writer <- RedisSpecLayers.connection(client)
        _      <- RedisSpecLayers.publish(writer, stream, "before")
        tail   <- RedisSpecLayers.connection(client).flatMap(StreamTailConsumer.make(_, NonEmptyChunk(stream), block))
        _      <- RedisSpecLayers.publish(writer, stream, "after")
        seen   <- Ref.make(Chunk.empty[String])
        _      <- tail.consume(batch => seen.update(_ ++ Chunk.fromIterable(batch.map(RedisSpecLayers.valueOf))))
        got    <- seen.get
      yield assertTrue(got == Chunk("after"))
    },
  ).provideSomeShared[Scope](RedisSpecLayers.client) @@ TestAspect.withLiveClock @@ TestAspect.sequential
