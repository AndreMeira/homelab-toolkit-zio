package homelab.incubator.db

import com.augustnagro.magnum.*
import homelab.common.error.ApplicationError.AdapterError
import homelab.incubator.db.PostgresTransaction.Transactional
import zio.*

/** A sample row — Magnum maps it from/to result sets via the derived `DbCodec`. */
final case class User(id: Long, email: String, username: String) derives DbCodec

/**
 * A sample repository showing the Magnum + [[Database]] shape: raw `sql` inside [[Transactional]],
 * requiring a [[PostgresTransaction]] in the environment (discharged by [[PostgresDatabase.transaction]]).
 * This is the same one-`Transactional`-block-per-method style as registration-service, in ZIO.
 */
final class PostgresUserRepository:

  def find(id: Long): ZIO[PostgresTransaction, AdapterError, Option[User]] =
    Transactional:
      sql"SELECT id, email, username FROM users WHERE id = $id".query[User].run().headOption

  def save(user: User): ZIO[PostgresTransaction, AdapterError, Unit] =
    Transactional:
      val _ = sql"INSERT INTO users (id, email, username) VALUES (${user.id}, ${user.email}, ${user.username})".update.run()

  def existsByEmail(email: String): ZIO[PostgresTransaction, AdapterError, Boolean] =
    Transactional:
      sql"SELECT EXISTS(SELECT 1 FROM users WHERE email = $email)".query[Boolean].run().headOption.contains(true)
