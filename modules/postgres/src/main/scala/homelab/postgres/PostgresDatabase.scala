package homelab.postgres


import com.zaxxer.hikari.HikariDataSource
import homelab.common.database.Database
import homelab.common.error.ApplicationError
import homelab.common.error.ApplicationError.AdapterError
import homelab.common.monitor.Monitor
import homelab.postgres.configuration.DatabaseSourceConfig
import zio.*

import javax.sql.DataSource


/**
 * A Postgres-backed [[Database]] using Magnum: checks a connection out of `dataSource` per transaction,
 * provides it to the body as a [[PostgresTransaction]] in the environment, commits when the body
 * succeeds, and rolls back on failure, defect, or interrupt — closing the connection either way. No
 * cats-effect: Magnum's blocking JDBC is lifted with `ZIO.attemptBlocking`, and the connection lives
 * entirely within the resource scope. Each transaction is observed by `monitor`.
 *
 * @param dataSource the pooled datasource connections are drawn from
 * @param monitor    observes each transaction (span + metrics)
 */
final class PostgresDatabase(dataSource: DataSource, monitor: Monitor) extends Database[PostgresTransaction]:

  /**
   * Run `effect` atomically: acquire a connection-backed [[PostgresTransaction]], provide it, commit on
   * success, roll back on any other outcome, and always close the connection.
   *
   * Commit runs in the body (`<* tx.commit`) so a failed commit surfaces as a typed `AdapterError` — an
   * `acquireReleaseExitWith` release is a `URIO` and can't fail into the error channel, so it only needs
   * to roll back a not-committed transaction (driven by the `Exit`) and close the connection.
   *
   * @tparam R the environment the body needs besides the transaction
   * @tparam E the domain error the body may raise
   * @tparam A the value the body produces
   * @param effect the transactional work; requires the [[PostgresTransaction]] in its environment
   * @return `effect`'s result with the transaction discharged; fails with the body's `E`, or an
   *         `AdapterError` if the connection can't be acquired or the commit fails (rollback failures are swallowed)
   */
  def transaction[R, E <: ApplicationError, A](
    effect: ZIO[R & PostgresTransaction, E, A]
  ): ZIO[R, PostgresTransaction.Error | E, A] =
    monitor.track("PostgresDatabase.transaction", PostgresDatabase.Tag):
      ZIO.acquireReleaseExitWith(acquire)(release): tx =>
        effect
          .provideSomeEnvironment[R](_.add[PostgresTransaction](tx))
          .tap[R, PostgresTransaction.Error | E](_ => tx.commit)

  /**
   * Check a connection out of the datasource, switch off auto-commit, and wrap it as a transaction.
   *
   * @return the transaction; fails with [[PostgresTransaction.ConnectionError]] if a connection can't be acquired
   */
  private def acquire: IO[PostgresTransaction.Error, PostgresTransaction] =
    ZIO
      .attemptBlocking {
        val connection = dataSource.getConnection
        connection.setAutoCommit(false)
        PostgresTransaction(connection)
      }
      .mapError(PostgresTransaction.ConnectionError(_))

  /**
   * Finalise the transaction from its run outcome: on success the body has already committed, so just
   * close; on any failure, defect, or interrupt, roll back then close. A `URIO`, so it can't surface its
   * own errors — the rollback is ignored and the connection is always returned to the pool.
   *
   * @tparam E the domain error the body may have raised
   * @param tx the transaction to finalise
   * @param exit the body's run outcome
   * @return unit — the connection is always closed
   */
  private def release[E](tx: PostgresTransaction, exit: Exit[AdapterError | E, Any]): UIO[Unit] =
    exit match {
      case Exit.Success(_) => close(tx)
      case Exit.Failure(_) => tx.rollback.ignore *> close(tx) // @todo logging? die?
    }

  /**
   * Close the connection, returning it to the pool. Closing one with an open transaction rolls it back, so
   * this doubles as a safety net; errors are ignored.
   *
   * @param tx the transaction whose connection to close
   * @return unit
   */
  private def close(tx: PostgresTransaction): UIO[Unit] =
    ZIO.attemptBlocking(tx.connection.close()).ignore


object PostgresDatabase:

  /** Metric/span tag marking database-resource operations. */
  val Tag: (String, String) = "resource" -> "database"

  /** The HikariCP pool couldn't be built from the datasource config. */
  final case class DatasourceBuildError(cause: Throwable) extends ApplicationError:
    override def message: String = "could not build a pooled datasource"

  /**
   * Build a [[PostgresDatabase]] over a scoped HikariCP pool from `config`. The pool is closed when the
   * caller's scope ends, so run it inside `ZIO.scoped` (or provide it via a scoped layer in app code).
   *
   * @param config  the datasource configuration
   * @param monitor observes each transaction (defaults to [[Monitor.Noop]])
   * @return a scoped database; fails with [[DatasourceBuildError]] if the pool can't be built
   */
  def make(
    config: DatabaseSourceConfig,
    monitor: Monitor = Monitor.Noop,
  ): ZIO[Scope, DatasourceBuildError, PostgresDatabase] =
    datasource(config).map(PostgresDatabase(_, monitor))

  /**
   * Build a scoped HikariCP datasource from `config` — the pool is closed when the caller's scope ends.
   * The building block behind [[make]], exposed so app code can build the pool once and share it (e.g.
   * across the database and its migrations) instead of each `make` opening its own.
   *
   * @param config the datasource configuration
   * @return a scoped datasource; fails with [[DatasourceBuildError]] if the pool can't be built
   */
  def datasource(
    config: DatabaseSourceConfig
  ): ZIO[Scope, DatasourceBuildError, HikariDataSource] = ZIO
    .fromAutoCloseable:
      ZIO.attemptBlocking(HikariDataSource(config.toHikariConfig))
    .mapError(DatasourceBuildError(_))
