package homelab.incubator.db


import com.augustnagro.magnum.{ DbTx, MagnumInterop }
import homelab.common.database.Database
import homelab.common.error.ApplicationError.AdapterError
import zio.*

import java.sql.Connection
import java.time.Instant


/**
 * A Postgres transaction wrapping a JDBC connection. Exposes a Magnum `DbTx` for running queries plus
 * commit/rollback hooks driven by [[PostgresDatabase]]. It is the `Tx` tag repositories require in their
 * environment — [[PostgresDatabase.transaction]] is the only thing that supplies it.
 */
final class PostgresTransaction(val connection: Connection) extends Database.Transaction:

  /** Magnum transaction handle bound to this connection (built from the raw JDBC connection). */
  def dbTx: DbTx = MagnumInterop.makeDbTx(connection)

  /** Commit the underlying connection. */
  def commit: IO[AdapterError, Unit] = ZIO
    .attemptBlocking(connection.commit())
    .mapError(PostgresTransaction.TransactionError(_))

  /** Roll the underlying connection back. */
  def rollback: IO[AdapterError, Unit] = ZIO
    .attemptBlocking(connection.rollback())
    .mapError(PostgresTransaction.TransactionError(_))


object PostgresTransaction:

  /** A pooled connection couldn't be checked out of the datasource. */
  final case class ConnectionError(cause: Throwable) extends AdapterError:
    override def message: String = s"could not establish a database connection: ${cause.getMessage}"

  /** A statement, commit, or rollback failed. */
  final case class TransactionError(cause: Throwable) extends AdapterError:
    override def message: String = s"database transaction failed: ${cause.getMessage}"

  /**
   * Run a Magnum action against the transaction from the environment. A repository calls this with a
   * `DbTx ?=> A` block; the given `DbTx` satisfies Magnum's `using DbCon` so `sql"...".query/update.run()`
   * resolve, and the blocking JDBC call is lifted with `ZIO.attemptBlocking`.
   *
   * @param action the Magnum query, as a context function over the transaction's `DbTx`
   * @return the query result; requires the [[PostgresTransaction]] and fails with [[TransactionError]]
   */
  object Transactional:
    def apply[A](action: DbTx ?=> A): ZIO[PostgresTransaction, AdapterError, A] =
      ZIO.serviceWithZIO[PostgresTransaction]: tx =>
        ZIO.attemptBlocking(action(using tx.dbTx)).mapError(TransactionError(_))

    def withTime[A](action: DbTx ?=> Instant => A): ZIO[PostgresTransaction, AdapterError, A] =
      ZIO.serviceWithZIO[PostgresTransaction]: tx =>
        Clock.instant.flatMap: now =>
          ZIO.attemptBlocking(action(using tx.dbTx)(now)).mapError(TransactionError(_))
