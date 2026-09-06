package homelab.incubator.messaging.redis


import io.lettuce.core.RedisClient
import zio.*
import zio.test.*


/**
 * What the cluster variant adds over a single tailing consumer: streams are sorted into slots, each slot
 * gets its own reader, and everything they read arrives through one `consume`.
 *
 * '''Run against a standalone Valkey.''' Slot arithmetic is client-side, so the partitioning and the
 * fan-in are exercised faithfully; what a real cluster would add — `CROSSSLOT` rejection, and slots moving
 * between nodes — is not covered here, and resharding is a known gap in the sketch.
 */
object ClusterStreamTailSpec extends ZIOSpecDefault:

  private val block = 2.seconds

  def spec: Spec[TestEnvironment & Scope, Any] = suite("ClusterStreamTail")(
    test("streams sharing a hash tag share a slot; different tags do not") {
      val streams = NonEmptyChunk("{alpha}:wake", "{alpha}:other", "{beta}:wake")
      val grouped = ClusterStreamTail.bySlot(streams)
      assertTrue(grouped.size == 2, grouped.values.map(_.size).toList.sorted == List(1, 2))
    },
    test("a reader per slot, and everything they read arrives through one consume") {
      for
        client <- ZIO.service[RedisClient]
        suffix  = scala.util.Random.nextInt(1 << 16)
        alpha   = s"{alpha-$suffix}:wake"
        beta    = s"{beta-$suffix}:wake"
        writer <- RedisSpecLayers.connection(client)
        tail   <- ClusterStreamTail.make(
                    _ => RedisSpecLayers.connection(client).mapError(e => StreamError.Unavailable(e.getMessage)),
                    NonEmptyChunk(alpha, beta),
                    block,
                  )
        slots  <- tail.positions
        _      <- RedisSpecLayers.publish(writer, alpha, "from-alpha")
        _      <- RedisSpecLayers.publish(writer, beta, "from-beta")
        seen   <- Ref.make(Set.empty[String])
        record  = (batch: List[io.lettuce.core.StreamMessage[String, Array[Byte]]]) => seen.update(_ ++ batch.map(RedisSpecLayers.valueOf).toSet)
        _      <- tail.consume(record)
        _      <- tail.consume(record)
        got    <- seen.get
      yield assertTrue(slots.size == 2, got == Set("from-alpha", "from-beta"))
    },
    test("every slot is read from the moment the consumer was made") {
      // The readers are opened together, so neither slot's stream is heard from its beginning, and neither
      // waits for the other to be asked for.
      for
        client <- ZIO.service[RedisClient]
        suffix  = scala.util.Random.nextInt(1 << 16)
        one     = s"{one-$suffix}:wake"
        two     = s"{two-$suffix}:wake"
        writer <- RedisSpecLayers.connection(client)
        _      <- RedisSpecLayers.publish(writer, one, "before")
        tail   <- ClusterStreamTail.make(
                    _ => RedisSpecLayers.connection(client).mapError(e => StreamError.Unavailable(e.getMessage)),
                    NonEmptyChunk(one, two),
                    block,
                  )
        slots  <- tail.positions
        _      <- RedisSpecLayers.publish(writer, two, "late")
        seen   <- Ref.make(Chunk.empty[String])
        _      <- tail.consume(batch => seen.update(_ ++ Chunk.fromIterable(batch.map(RedisSpecLayers.valueOf))))
        got    <- seen.get
      yield assertTrue(slots.size == 2, got == Chunk("late"))
    },
  ).provideSomeShared[Scope](RedisSpecLayers.client) @@ TestAspect.withLiveClock @@ TestAspect.sequential
