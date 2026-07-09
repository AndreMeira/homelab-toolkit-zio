package homelab.postgres


import com.augustnagro.magnum.*
import homelab.common.error.ApplicationError
import homelab.postgres.PostgresTransaction.Transactional
import zio.*
import zio.test.*


/**
 * Integration tests for [[PostgresDatabase]] against a real Postgres (Testcontainers). Exercises the
 * Unit-of-Work guarantee end to end: migrations build the schema, a committed write is visible to a later
 * transaction, and a failed transaction rolls its write back. Requires a running Docker daemon.
 */
object PostgresDatabaseSpec extends ZIOSpecDefault:

  /** A domain failure used to force a rollback. */
  private case object Boom extends ApplicationError:
    override def message: String = "boom"

  /** Insert a widget within the ambient transaction, returning the affected row count. */
  private def insert(id: String, name: String): ZIO[PostgresTransaction, PostgresTransaction.Error, Int] =
    Transactional(sql"insert into widget (id, name) values ($id, $name)".update.run())

  /** Count widgets with `id` within the ambient transaction. */
  private def countById(id: String): ZIO[PostgresTransaction, PostgresTransaction.Error, Int] =
    Transactional(sql"select count(*) from widget where id = $id".query[Int].run().head)

  def spec = suite("PostgresDatabase (integration)")(
    test("migrations build the schema and a committed insert is visible to a later transaction") {
      for
        database <- ZIO.service[PostgresDatabase]
        inserted <- database.transaction(insert("w1", "alpha"))
        count    <- database.transaction(countById("w1"))
      yield assertTrue(inserted == 1, count == 1)
    },
    test("a transaction that fails after writing rolls back — its write is not visible") {
      for
        database <- ZIO.service[PostgresDatabase]
        outcome  <- database.transaction(insert("w2", "beta") *> ZIO.fail(Boom)).either
        count    <- database.transaction(countById("w2"))
      yield assertTrue(outcome == Left(Boom), count == 0)
    },
  ).provideShared(PostgresSpecLayers.database) @@ TestAspect.sequential
