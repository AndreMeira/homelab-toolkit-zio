package homelab.incubator.db


import homelab.common.database.Database
import homelab.common.error.ApplicationError
import homelab.common.error.ApplicationError.AdapterError
import zio.*

import javax.sql.DataSource


/**
 * A Postgres-backed [[Database]] using Magnum: checks a connection out of `dataSource` per transaction,
 * provides it to the body as a [[PostgresTransaction]] in the environment, commits when the body
 * succeeds, and rolls back on failure, defect, or interrupt — closing the connection either way. No
 * cats-effect: Magnum's blocking JDBC is lifted with `ZIO.attemptBlocking`, and the connection lives
 * entirely within the resource scope.
 *
 * @param dataSource the pooled datasource connections are drawn from
 */
final class PostgresDatabase(dataSource: DataSource) extends Database[PostgresTransaction]:

  /**
   * Run `effect` atomically: acquire a connection-backed [[PostgresTransaction]], provide it, commit on
   * success, roll back on any other outcome, and always close the connection.
   *
   * Commit runs in the body (`<* tx.commit`) so a failed commit surfaces as a typed `AdapterError` — an
   * `acquireReleaseExitWith` release is a `URIO` and can't fail into the error channel, so it only needs
   * to roll back a not-committed transaction (driven by the `Exit`) and close the connection.
   *
   * @return `effect`'s result with the transaction discharged; fails with the body's `E`, or an
   *         `AdapterError` if the connection / commit / rollback fails
   */
  def transaction[R, E <: ApplicationError, A](
    effect: ZIO[R & PostgresTransaction, E, A]
  ): ZIO[R, AdapterError | E, A] =
    ZIO.acquireReleaseExitWith(acquire)(release): tx =>
      effect.provideSomeEnvironment[R](_.add[PostgresTransaction](tx)) <* tx.commit

  /** Check a connection out, switch off auto-commit, and wrap it as a transaction. */
  private def acquire: IO[AdapterError, PostgresTransaction] =
    ZIO
      .attemptBlocking {
        val connection = dataSource.getConnection
        connection.setAutoCommit(false)
        PostgresTransaction(connection)
      }
      .mapError(PostgresTransaction.ConnectionError(_))

  private def release[E](tx: PostgresTransaction, exit: Exit[AdapterError | E, Any]): UIO[Unit] =
    exit match {
      case Exit.Success(_) => close(tx)
      case Exit.Failure(_) => tx.rollback.ignore *> close(tx)
    }

  /** Return the connection to the pool (a no-op rollback after a commit). */
  private def close(tx: PostgresTransaction): UIO[Unit] =
    ZIO.attemptBlocking(tx.connection.close()).ignore
