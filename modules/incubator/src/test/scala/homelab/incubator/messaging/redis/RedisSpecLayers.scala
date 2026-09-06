package homelab.incubator.messaging.redis


import io.lettuce.core.codec.{ ByteArrayCodec, RedisCodec, StringCodec }
import io.lettuce.core.cluster.api.sync.RedisClusterCommands
import io.lettuce.core.{ RedisClient, RedisURI }
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.utility.DockerImageName
import zio.*


/**
 * Test wiring for the Redis stream sketches: a throwaway Valkey, and connections onto it.
 *
 * Mirrors `NatsSpecLayers`. Requires a running Docker daemon.
 */
object RedisSpecLayers:

  /** Valkey rather than Redis, matching what the homelab runs. */
  private val image = "valkey/valkey:8.1-alpine"

  private val port = 6379

  /** Keys as UTF-8 text, values as raw bytes — the shape the sketches are typed for. */
  private val codec: RedisCodec[String, Array[Byte]] = RedisCodec.of(StringCodec.UTF8, ByteArrayCodec.INSTANCE)

  /**
   * A client onto a throwaway Valkey, shut down with the scope.
   *
   * @return the client; fails with the `Throwable` raised if the container or client cannot start
   */
  val client: ZLayer[Any, Throwable, RedisClient] = ZLayer.scoped:
    for
      container <- startContainer
      url        = s"redis://${container.getHost}:${container.getMappedPort(port)}"
      client    <- ZIO.acquireRelease(ZIO.attempt(RedisClient.create(RedisURI.create(url))))(c => ZIO.attempt(c.shutdown()).ignore)
    yield client

  /**
   * A connection of its own, with a command timeout above any block these tests use.
   *
   * Each blocking reader needs one: a parked `XREAD` owns its connection, and a command timeout below the
   * block would abandon a read that is doing what it was told to.
   *
   * @param client the client to connect with
   * @return the synchronous commands, closed with the scope
   */
  def connection(client: RedisClient): ZIO[Scope, Throwable, RedisClusterCommands[String, Array[Byte]]] =
    ZIO
      .acquireRelease(ZIO.attemptBlocking(client.connect(codec)))(c => ZIO.attemptBlocking(c.close()).ignore)
      .map: connection =>
        connection.setTimeout(java.time.Duration.ofSeconds(30))
        connection.sync()

  /**
   * Append one entry, as a producer would.
   *
   * @param redis the connection to write on
   * @param stream the stream key
   * @param body the entry's single `value` field
   * @return the entry id
   */
  def publish(redis: RedisClusterCommands[String, Array[Byte]], stream: String, body: String): Task[String] =
    ZIO.attemptBlocking(redis.xadd(stream, java.util.Map.of("value", body.getBytes("UTF-8"))))

  /** Read an entry's `value` field back as text. */
  def valueOf(entry: io.lettuce.core.StreamMessage[String, Array[Byte]]): String =
    String(entry.getBody.get("value"), "UTF-8")

  private def startContainer: ZIO[Scope, Throwable, GenericContainer[?]] =
    ZIO.fromAutoCloseable:
      ZIO.attemptBlocking:
        val _                              = java.lang.System.setProperty("api.version", "1.40")
        val container: GenericContainer[?] = new GenericContainer(DockerImageName.parse(image))
        container.withExposedPorts(Integer.valueOf(port))
        container.waitingFor(Wait.forLogMessage(".*Ready to accept connections.*", 1))
        container.start()
        container
