package homelab.postgres.configuration


import com.zaxxer.hikari.HikariConfig

import scala.concurrent.duration.Duration


/**
 * Connection-pool settings for the Postgres datasource. A plain case class — the consuming service loads
 * it from config (deriving a reader for its config library of choice); the toolkit maps it to a HikariCP
 * config via [[toHikariConfig]].
 *
 * @param jdbcUrl the JDBC URL (e.g. `jdbc:postgresql://host:5432/db`)
 * @param user the database user
 * @param password the database password
 * @param poolSize the minimum idle connections; the maximum pool is twice this
 * @param connectionTestQuery the liveness probe run on borrowed connections
 * @param driverClass the JDBC driver class name
 * @param connectionTimeout how long to wait for a connection before failing
 */
final case class DatabaseSourceConfig(
  jdbcUrl: String,
  user: String,
  password: String,
  poolSize: Int = 10,
  connectionTestQuery: String = "SELECT 1",
  driverClass: String = "org.postgresql.Driver",
  connectionTimeout: Duration = Duration.create(60, "seconds"),
):

  /**
   * Translate to a HikariCP configuration. Auto-commit is switched off — [[homelab.postgres.PostgresDatabase]]
   * drives commit/rollback itself.
   *
   * @return the HikariCP configuration
   */
  def toHikariConfig: HikariConfig =
    val config = HikariConfig()
    config.setJdbcUrl(jdbcUrl)
    config.setUsername(user)
    config.setPassword(password)
    config.setMinimumIdle(poolSize)
    config.setMaximumPoolSize(poolSize * 2)
    config.setConnectionTestQuery(connectionTestQuery)
    config.setDriverClassName(driverClass)
    config.setConnectionTimeout(connectionTimeout.toMillis)
    config.setAutoCommit(false)
    config
