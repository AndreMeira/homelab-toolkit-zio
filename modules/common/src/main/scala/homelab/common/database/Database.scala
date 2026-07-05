package homelab.common.database

import homelab.common.error.ApplicationError
import homelab.common.error.ApplicationError.AdapterError
import zio.*

/**
 * Port for running effects atomically inside a database transaction — the hexagonal Unit of Work.
 *
 * The adapter owns the transaction lifecycle: acquire a connection, provide the transaction to the
 * body, commit when the body succeeds, roll back if it fails, dies, or is interrupted. Parameterised
 * by the transaction type so the same port serves every backend (the Postgres `Tx`, the in-memory
 * one): repositories require `Tx` in their environment, and only this port knows how to supply it.
 *
 * @tparam Tx the transaction handle repositories run against (a type-level tag for DI)
 */
trait Database[Tx <: Database.Transaction] {

  /**
   * Run `effect` atomically (at the database level), discharging its `Tx` requirement.
   *
   * The body asks for the transaction in its environment (`R & Tx`); `transaction` supplies the `Tx`
   * and hands back an effect needing only `R`, so callers never touch the transaction handle. It
   * commits when `effect` succeeds and rolls back otherwise.
   *
   * The error channel widens the body's `E` with `AdapterError` because the transaction *mechanism*
   * can fail (connection lost, commit/rollback failed) independently of any domain error the body
   * raises. `AdapterError` is the opaque infra umbrella — callers carry it in their signature without
   * branching on it, while `E` stays precise for the domain outcomes they do branch on.
   *
   * @tparam R the environment the body needs besides the transaction
   * @tparam E the domain error the body may raise
   * @tparam A the value the body produces
   * @param effect the transactional work; requires `Tx` in its environment
   * @return `effect`'s result with `Tx` discharged; fails with the body's `E`, or with an
   *         `AdapterError` when the transaction mechanism itself fails
   */
  def transaction[R, E <: ApplicationError, A](
    effect: ZIO[R & Tx, E, A]
  ): ZIO[R, AdapterError | E, A]
}

object Database:

  /**
   * Marker for a backend's transaction handle — the type-level tag [[Database]] and its repositories
   * share so the same ports type-check against both the Postgres and in-memory transaction.
   */
  trait Transaction
