package homelab.postgres


import com.augustnagro.magnum.{ DbTx, MagnumInterop }
import homelab.common.database.Database
import homelab.common.error.ApplicationError
import homelab.common.error.ApplicationError.{ AdapterError, NetworkError, PersistenceError }
import zio.*

import java.sql.Connection
import java.time.Instant


/**
 * A Postgres transaction wrapping a JDBC connection. Exposes a Magnum `DbTx` for running queries plus
 * commit/rollback hooks driven by [[PostgresDatabase]]. It is the `Tx` tag repositories require in their
 * environment — [[PostgresDatabase.transaction]] is the only thing that supplies it.
 *
 * @param connection the checked-out JDBC connection this transaction runs on
 */
final class PostgresTransaction(val connection: Connection) extends Database.Transaction:

  /**
   * The Magnum transaction handle bound to this connection.
   *
   * @return a `DbTx` over [[connection]], for running `sql"..."` queries
   */
  def dbTx: DbTx = MagnumInterop.makeDbTx(connection)

  /**
   * Commit the underlying connection.
   *
   * @return unit; fails with [[PostgresTransaction.TransactionError]] if the commit fails
   */
  def commit: IO[PostgresTransaction.Error, Unit] = ZIO
    .attemptBlocking(connection.commit())
    .mapError(PostgresTransaction.TransactionError(_))

  /**
   * Roll the underlying connection back.
   *
   * @return unit; fails with [[PostgresTransaction.TransactionError]] if the rollback fails
   */
  def rollback: IO[PostgresTransaction.Error, Unit] = ZIO
    .attemptBlocking(connection.rollback())
    .mapError(PostgresTransaction.TransactionError(_))


object PostgresTransaction:
  trait Error extends AdapterError

  /** A pooled connection couldn't be checked out of the datasource. */
  final case class ConnectionError(cause: Throwable) extends Error, PersistenceError, NetworkError:
    override def message: String = s"could not establish a database connection: ${cause.getMessage}"

  /** A statement, commit, or rollback failed. */
  final case class TransactionError(cause: Throwable) extends Error, PersistenceError:
    override def message: String = s"database transaction failed: ${cause.getMessage}"

  /**
   * Runs a Magnum action against the [[PostgresTransaction]] in the environment. A repository calls these
   * with a `DbTx ?=> A` block; the given `DbTx` satisfies Magnum's `using DbCon` so `sql"...".query/update
   * .run()` resolve, and the blocking JDBC call is lifted with `ZIO.attemptBlocking`.
   */
  object Transactional:

    /**
     * Run `action` against the transaction's `DbTx`.
     *
     * @tparam A the value the action produces
     * @param action the Magnum query, as a context function over the transaction's `DbTx`
     * @return the query result; requires the [[PostgresTransaction]] and fails with [[TransactionError]] if the statement fails
     */
    def apply[A](action: DbTx ?=> A): ZIO[PostgresTransaction, Error, A] =
      ZIO.serviceWithZIO[PostgresTransaction]: tx =>
        ZIO.attemptBlocking(action(using tx.dbTx)).mapError(TransactionError(_))

    /**
     * Run `action` against the transaction's `DbTx`, supplying the current instant — for rows stamped with
     * a single consistent `now`.
     *
     * @tparam A the value the action produces
     * @param action the Magnum query, as a context function over the transaction's `DbTx` and the current instant
     * @return the query result; requires the [[PostgresTransaction]] and fails with [[TransactionError]] if the statement fails
     */
    def withTime[A](action: DbTx ?=> Instant => A): ZIO[PostgresTransaction, Error, A] =
      ZIO.serviceWithZIO[PostgresTransaction]: tx =>
        Clock.instant.flatMap: now =>
          ZIO.attemptBlocking(action(using tx.dbTx)(now)).mapError(TransactionError(_))
