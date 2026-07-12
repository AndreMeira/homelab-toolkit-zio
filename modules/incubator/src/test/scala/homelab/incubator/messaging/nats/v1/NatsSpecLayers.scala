package homelab.incubator.messaging.nats.v1


import io.nats.client.Connection
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.utility.DockerImageName
import zio.*


/**
 * Test wiring for the NATS v1 sketch: a single scoped [[Connection]] over a throwaway Testcontainers
 * NATS server. Mirrors `homelab.postgres.PostgresSpecLayers` — the container is a private resource
 * acquired via `ZIO.fromAutoCloseable` (stopped when the suite scope closes), and only the finished,
 * connected [[Connection]] is published, so a spec needs nothing more than `provideShared([[connection]])`.
 *
 * Requires a running Docker daemon.
 */
object NatsSpecLayers:

  /** The NATS image the throwaway container runs. */
  private val image = "nats:2.10-alpine"

  /** The NATS client port exposed by the container. */
  private val clientPort = 4222

  /**
   * A live [[Connection]] to a throwaway Testcontainers NATS server.
   *
   * @return a scoped layer that starts the container and connects; aborts with [[NatsError]] if the
   *         container can't start or the connection can't be established
   */
  val connection: ZLayer[Any, NatsError, Connection] = ZLayer.scoped:
    for
      container  <- startContainer.mapError(NatsError.Connect(_))
      url         = s"nats://${container.getHost}:${container.getMappedPort(clientPort)}"
      connection <- NatsConnection.make(url)
    yield connection

  /**
   * Start a throwaway NATS container, stopped when the enclosing scope closes.
   *
   * @return the started container as a scoped resource; fails with the `Throwable` raised if it can't start
   */
  private def startContainer: ZIO[Scope, Throwable, GenericContainer[?]] =
    ZIO.fromAutoCloseable:
      ZIO.attemptBlocking:
        pinDockerApiVersion()
        val container: GenericContainer[?] = new GenericContainer(DockerImageName.parse(image))
        container.withExposedPorts(clientPort)
        container.waitingFor(Wait.forLogMessage(".*Server is ready.*", 1))
        container.start()
        container

  /**
   * Pin the Docker Remote API version before the first Testcontainers call. Docker Engine ≥ 25 advertises
   * `MinAPIVersion` 1.40 and rejects `/info` with HTTP 400 for anything older; docker-java's bundled default
   * is older, so container startup fails with "Could not find a valid Docker environment". 1.40 is the widest
   * floor — honoured from Engine 19.03 through current — so a single pin works across daemons.
   */
  private def pinDockerApiVersion(): Unit =
    val _ = java.lang.System.setProperty("api.version", "1.40")
