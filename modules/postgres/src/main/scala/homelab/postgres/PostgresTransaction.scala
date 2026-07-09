package homelab.postgres


import com.augustnagro.magnum.{ DbTx, MagnumInterop }
import homelab.common.database.Database
import homelab.common.error.ApplicationError
import homelab.common.error.ApplicationError.{ AdapterError, NetworkError, PersistenceError, TransientError }
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
    .mapError(PostgresTransaction.TransactionError.from)

  /**
   * Roll the underlying connection back.
   *
   * @return unit; fails with [[PostgresTransaction.TransactionError]] if the rollback fails
   */
  def rollback: IO[PostgresTransaction.Error, Unit] = ZIO
    .attemptBlocking(connection.rollback())
    .mapError(PostgresTransaction.TransactionError.from)


object PostgresTransaction:
  trait Error extends AdapterError

  /** A pooled connection couldn't be checked out — transient (pool exhaustion, DB briefly unavailable). */
  final case class ConnectionError(cause: Throwable) extends Error, PersistenceError, NetworkError, TransientError:
    override def message: String = s"could not establish a database connection: ${cause.getMessage}"

  /** A statement, commit, or rollback failed for a non-retryable reason (constraint, syntax, …). */
  final case class TransactionError(cause: Throwable) extends Error, PersistenceError:
    override def message: String = s"database transaction failed: ${cause.getMessage}"

  /** A statement rolled back on a transient conflict (serialization failure / deadlock) — safe to retry. */
  final case class TransactionConflict(cause: Throwable) extends Error, PersistenceError, TransientError:
    override def message: String = s"database transaction conflict: ${cause.getMessage}"

  object TransactionError:

    /** SQLSTATEs whose transaction rolled back for a transient reason and can be re-run as a whole. */
    private val retryable = Set("40001", "40P01") // serialization_failure, deadlock_detected

    /**
     * Classify a database failure: a transient [[TransactionConflict]] for a serialization failure or a
     * deadlock (the whole transaction is then safe to retry), otherwise a plain [[TransactionError]].
     *
     * @param cause the failure thrown while running, committing, or rolling back the transaction
     * @return the matching error
     */
    def from(cause: Throwable): Error =
      if sqlState(cause).exists(retryable) then TransactionConflict(cause) else TransactionError(cause)

    /**
     * The SQLSTATE of the first `java.sql.SQLException` in `cause`'s cause-chain, if any.
     *
     * @param cause the throwable, possibly wrapping a `SQLException`, to inspect
     * @return the SQLSTATE code, or `None` when the chain carries no `SQLException` with a state
     */
    @annotation.tailrec
    private def sqlState(cause: Throwable): Option[String] = cause match
      case sql: java.sql.SQLException if sql.getSQLState != null => Some(sql.getSQLState)
      case _                                                     =>
        cause.getCause match
          case null                  => None
          case next if next eq cause => None
          case next                  => sqlState(next)

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
        ZIO.attemptBlocking(action(using tx.dbTx)).mapError(TransactionError.from)

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
          ZIO.attemptBlocking(action(using tx.dbTx)(now)).mapError(TransactionError.from)
