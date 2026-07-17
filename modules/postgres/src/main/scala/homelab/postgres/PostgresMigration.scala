package homelab.postgres


import com.zaxxer.hikari.HikariDataSource
import homelab.common.error.ApplicationError.AdapterError
import homelab.common.monitor.Monitor
import homelab.postgres.PostgresDatabase.DatasourceBuildError
import homelab.postgres.PostgresMigration.{ CleanFailed, MigrationFailed }
import homelab.postgres.configuration.{ DatabaseSourceConfig, MigrationConfig }
import org.flywaydb.core.Flyway
import zio.*

import javax.sql.DataSource


/**
 * Runs Flyway migrations against a datasource, observed by `monitor`. Build it with
 * [[PostgresMigration.make]] and invoke it from a `("database", "migrate")` run mode before the
 * application starts; layer wiring is left to the application.
 *
 * @param flyway  the configured Flyway instance (datasource already attached)
 * @param monitor observes each migration (span + metrics)
 */
final class PostgresMigration(flyway: Flyway, monitor: Monitor):

  /**
   * Apply all pending migrations.
   *
   * @return unit; fails with [[MigrationFailed]] if a migration fails
   */
  def applyMigrations: IO[MigrationFailed, Unit] =
    monitor.track("PostgresMigration.applyMigrations", PostgresDatabase.Tag):
      ZIO.attemptBlocking(flyway.migrate()).mapError(MigrationFailed(_)).unit

  /**
   * Drop everything Flyway manages (guarded by the config's `allowClean`).
   *
   * @return unit; fails with [[CleanFailed]] if the clean fails or is disabled
   */
  def cleanMigrations: IO[CleanFailed, Unit] =
    monitor.track("PostgresMigration.cleanMigrations", PostgresDatabase.Tag):
      ZIO.attemptBlocking(flyway.clean()).mapError(CleanFailed(_)).unit


object PostgresMigration:

  /** Applying migrations failed. */
  final case class MigrationFailed(cause: Throwable) extends AdapterError:
    override def message: String = s"database migration failed: ${cause.getMessage}"

  /** Cleaning the schema failed, or was disabled by config. */
  final case class CleanFailed(cause: Throwable) extends AdapterError:
    override def message: String = s"database clean failed: ${cause.getMessage}"

  /**
   * Build a [[PostgresMigration]] over a scoped HikariCP pool from `config` and `datasourceConfig`. The
   * pool is closed when the caller's scope ends, so run it inside `ZIO.scoped` (typically a one-shot
   * migrate run mode).
   *
   * @param config           the migration configuration
   * @param datasourceConfig the datasource to migrate against
   * @param monitor          observes each migration (defaults to [[Monitor.Noop]])
   * @return a scoped migration runner; fails with [[DatasourceBuildError]] if the pool can't be built, or
   *         [[MigrationFailed]] if Flyway can't be loaded
   */
  def make(
    config: MigrationConfig,
    datasourceConfig: DatabaseSourceConfig,
    monitor: Monitor = Monitor.Noop,
  ): ZIO[Scope, MigrationFailed | DatasourceBuildError, PostgresMigration] =
    for
      datasource <- PostgresDatabase.datasource(datasourceConfig)
      flywayConf  = config.toFlywayConfig.dataSource(datasource)
      flyway     <- ZIO.attemptBlocking(flywayConf.load()).mapError(MigrationFailed(_))
    yield PostgresMigration(flyway, monitor)

  /**
   * Build a [[PostgresMigration]] using an already-constructed datasource. Use this when datasource
   * lifecycle is managed elsewhere and only Flyway wiring is needed.
   *
   * @param config     the migration configuration used to build Flyway
   * @param datasource the existing datasource Flyway should run migrations against
   * @param monitor    observes each migration (defaults to [[Monitor.Noop]])
   * @return a migration runner; fails with [[MigrationFailed]] if Flyway can't be loaded
   */
  def build(
    config: MigrationConfig,
    datasource: DataSource,
    monitor: Monitor = Monitor.Noop,
  ): ZIO[Any, MigrationFailed, PostgresMigration] = ZIO
    .attemptBlocking(config.toFlywayConfig.dataSource(datasource).load())
    .mapError(MigrationFailed(_))
    .map(PostgresMigration(_, monitor))
