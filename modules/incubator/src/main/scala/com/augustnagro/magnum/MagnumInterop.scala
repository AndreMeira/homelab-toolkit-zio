package com.augustnagro.magnum

import java.sql.Connection

/**
 * Interop helper for Magnum, living in Magnum's package to reach `DbTx`'s package-private constructor.
 * Builds a `DbTx` from a raw JDBC connection so the ZIO transaction wrapper ([[homelab.incubator.db.PostgresTransaction]])
 * can drive Magnum queries against a connection it manages itself.
 *
 * (Copied from registration-service — the same shim, framework-agnostic.)
 */
object MagnumInterop:

  /** Build a `DbTx` from a JDBC connection using the default SQL logger. */
  def makeDbTx(connection: Connection): DbTx =
    DbTx(connection, SqlLogger.Default)
