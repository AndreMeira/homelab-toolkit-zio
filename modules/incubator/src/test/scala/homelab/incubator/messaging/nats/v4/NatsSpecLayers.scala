package homelab.incubator.messaging.nats.v4


import io.nats.client.Connection
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.utility.DockerImageName
import zio.*


/**
 * Test wiring for the NATS v4 sketch: a single scoped [[Connection]] over a throwaway Testcontainers NATS
 * server with JetStream enabled (`-js`, needed for the JetStream tests; Core NATS tests use the same
 * server). Mirrors `homelab.postgres.PostgresSpecLayers`. Requires a running Docker daemon.
 */
object NatsSpecLayers:

  /** The NATS image the throwaway container runs. */
  private val image = "nats:2.10-alpine"

  /** The NATS client port exposed by the container. */
  private val clientPort = 4222

  /**
   * A live [[Connection]] to a throwaway JetStream-enabled NATS server.
   *
   * @return a scoped layer that starts the container and connects; aborts with [[NatsError]] if the
   *         container can't start or the make can't be established
   */
  val connection: ZLayer[Any, NatsError, Connection] = ZLayer.scoped:
    for
      container  <- startContainer.mapError(NatsError.Connect(_))
      url         = s"nats://${container.getHost}:${container.getMappedPort(clientPort)}"
      connection <- NatsConnection.make(url)
    yield connection

  /**
   * Start a throwaway JetStream-enabled NATS container, stopped when the enclosing scope closes.
   *
   * @return the started container as a scoped resource; fails with the `Throwable` raised if it can't start
   */
  private def startContainer: ZIO[Scope, Throwable, GenericContainer[?]] =
    ZIO.fromAutoCloseable:
      ZIO.attemptBlocking:
        pinDockerApiVersion()
        val container: GenericContainer[?] = new GenericContainer(DockerImageName.parse(image))
        container.withExposedPorts(clientPort)
        container.withCommand("-js") // enable JetStream
        container.waitingFor(Wait.forLogMessage(".*Server is ready.*", 1))
        container.start()
        container

  /**
   * Pin the Docker Remote API version before the first Testcontainers call (Engine ≥ 25 rejects older
   * API versions with HTTP 400). 1.40 is the widest floor across daemons.
   */
  private def pinDockerApiVersion(): Unit =
    val _ = java.lang.System.setProperty("api.version", "1.40")
