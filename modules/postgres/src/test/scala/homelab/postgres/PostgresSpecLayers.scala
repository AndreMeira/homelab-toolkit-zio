package homelab.postgres


import homelab.common.error.ApplicationError
import homelab.postgres.configuration.{ DatabaseSourceConfig, MigrationConfig }
import org.testcontainers.containers.PostgreSQLContainer
import org.testcontainers.utility.DockerImageName
import zio.*


/**
 * Test wiring for the Postgres adapter: a single scoped [[PostgresDatabase]] over a throwaway
 * Testcontainers Postgres, with the schema already migrated.
 *
 * Everything lives in one layer on purpose. The container is a private resource acquired via
 * `ZIO.fromAutoCloseable` (stopped when the suite's scope closes), migrations run once as part of
 * acquisition, and only the finished [[PostgresDatabase]] is published — so a spec needs nothing more than
 * `provideShared([[database]])`. This deliberately collapses the reference setup's split
 * container/config/datasource/migration layers and its `withMigration` test aspect: with a fresh container
 * per suite there is no leftover state to clean, so migrating on acquisition is all that is needed.
 *
 * Requires a running Docker daemon.
 */
object PostgresSpecLayers:

  /** The Postgres image the throwaway container runs. */
  private val image = "postgres:17-alpine"

  /** Flyway config pointing at the test migrations; clean enabled so local re-runs against a reused daemon
    * start from a known state. */
  private val migrationConfig =
    MigrationConfig(initSql = "", locations = List("classpath:migrations/schema"), allowClean = true)

  /**
   * A migrated [[PostgresDatabase]] over a throwaway Testcontainers Postgres.
   *
   * @return a scoped layer that starts the container, applies migrations, and publishes the database; fails
   *         with the underlying `Throwable` if the container, migrations, or connection pool can't be brought up
   */
  val database: ZLayer[Any, ApplicationError, PostgresDatabase] = ZLayer.scoped:
    for
      container <- startContainer.mapError(_ => ContainerStartError)
      source     = DatabaseSourceConfig(container.getJdbcUrl, container.getUsername, container.getPassword)
      database  <- PostgresDatabase.make(source)
      _         <- database.migrate(migrationConfig)
    yield database

  /**
   * Start a throwaway Postgres container, stopped when the enclosing scope closes.
   *
   * @return the started container as a scoped resource; fails with the `Throwable` raised if it can't start
   */
  private def startContainer: ZIO[Scope, Throwable, PostgreSQLContainer[?]] =
    ZIO.fromAutoCloseable:
      ZIO.attemptBlocking:
        pinDockerApiVersion()
        val container = new PostgreSQLContainer(DockerImageName.parse(image))
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

  /** Can not start the container */
  case object ContainerStartError extends ApplicationError:
    override def message: String = "could not start the Postgres test container"
