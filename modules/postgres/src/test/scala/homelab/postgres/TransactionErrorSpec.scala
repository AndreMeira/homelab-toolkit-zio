package homelab.postgres


import homelab.common.error.ApplicationError.TransientError
import homelab.postgres.PostgresTransaction.{ TransactionConflict, TransactionError }
import zio.test.*

import java.sql.SQLException


object TransactionErrorSpec extends ZIOSpecDefault:

  private def sqlError(state: String): SQLException = new SQLException("boom", state)

  def spec = suite("TransactionError.from")(
    test("serialization failure (40001) → a transient TransactionConflict") {
      val error = TransactionError.from(sqlError("40001"))
      assertTrue(error.isInstanceOf[TransactionConflict], error.isInstanceOf[TransientError])
    },
    test("deadlock (40P01) → a transient TransactionConflict") {
      assertTrue(TransactionError.from(sqlError("40P01")).isInstanceOf[TransactionConflict])
    },
    test("a unique violation (23505) → a plain, non-transient TransactionError") {
      val error = TransactionError.from(sqlError("23505"))
      assertTrue(error.isInstanceOf[TransactionError], !error.isInstanceOf[TransientError])
    },
    test("a non-SQL failure → a plain TransactionError") {
      assertTrue(TransactionError.from(new RuntimeException("nope")).isInstanceOf[TransactionError])
    },
    test("a SQLException wrapped deeper in the cause chain is still classified") {
      val wrapped = new RuntimeException("wrapper", sqlError("40001"))
      assertTrue(TransactionError.from(wrapped).isInstanceOf[TransactionConflict])
    },
  )
