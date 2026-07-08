package com.augustnagro.magnum

import java.sql.Connection


/**
 * Interop helpers for Magnum, living in Magnum's package to reach `DbTx`'s package-private constructor
 * and to build `Frag`s directly. Provides:
 *
 *   - [[makeDbTx]] — a `DbTx` from a raw JDBC connection, so the ZIO transaction wrapper
 *     ([[homelab.postgres.PostgresTransaction]]) can drive Magnum against a connection it manages itself.
 *   - [[combine]] and [[syntax]] — a small fragment-concatenation algebra Magnum lacks out of the box:
 *     compose `Frag`s with `and`/`or`/`comma`/`++`, splice conditionally with `when`, and start from
 *     [[syntax.emptyFragment]]. It threads each fragment's `writer` so bound parameters land at the right
 *     `?` positions.
 */
object MagnumInterop:

  /**
   * Build a `DbTx` from a JDBC connection using the default SQL logger.
   *
   * @param connection the connection to run queries against
   * @return a `DbTx` over `connection`
   */
  def makeDbTx(connection: Connection): DbTx =
    DbTx(connection, SqlLogger.Default)

  /**
   * Combine two SQL fragments with a separator, dropping empty fragments and threading their writers so
   * parameters keep their positions.
   *
   * @param left the left fragment
   * @param right the right fragment
   * @param separator the string between them (e.g. `" AND "`, `" OR "`, `", "`)
   * @return the combined fragment
   */
  def combine(left: Frag, right: Frag, separator: String): Frag =
    val nonEmpty = List(left, right).filter(_.sqlString.nonEmpty)
    Frag(
      sqlString = nonEmpty.iterator.map(_.sqlString).mkString(separator),
      params = nonEmpty.flatMap(_.params),
      writer = (statement, pos) => nonEmpty.foldLeft(pos)((currentPos, frag) => frag.writer.write(statement, currentPos)),
    )

  /** Fluent combinators for [[Frag]]: `and`/`or`/`comma`/`++`, a custom `concat`, `when`, and an identity. */
  object syntax:

    /**
     * The identity fragment — empty SQL, no params, no-op writer. Use as a fold base.
     *
     * @return an empty [[Frag]]
     */
    def emptyFragment: Frag =
      Frag(sqlString = "", params = Nil, writer = (_, pos) => pos)

    extension (left: Frag)

      /**
       * Combine with `right` using `" AND "`.
       *
       * @param right the fragment to append
       * @return the combined fragment
       */
      def and(right: Frag): Frag = combine(left, right, " AND ")

      /**
       * Combine with `right` using `" OR "`.
       *
       * @param right the fragment to append
       * @return the combined fragment
       */
      def or(right: Frag): Frag = combine(left, right, " OR ")

      /**
       * Combine with `right` using `", "`.
       *
       * @param right the fragment to append
       * @return the combined fragment
       */
      def comma(right: Frag): Frag = combine(left, right, ", ")

      /**
       * Combine with `right` using a single space.
       *
       * @param right the fragment to append
       * @return the combined fragment
       */
      def ++(right: Frag): Frag = combine(left, right, " ")

      /**
       * Combine with `right` using a custom separator.
       *
       * @param separator the string between the fragments
       * @param right the fragment to append
       * @return the combined fragment
       */
      def concat(separator: String)(right: Frag): Frag = combine(left, right, separator)

      /**
       * Append `other` only when `condition` holds (a space-separated splice) — for optional clauses.
       *
       * @param condition whether to append `other`
       * @param other the fragment to conditionally append
       * @return `left ++ other` when `condition` holds, otherwise `left`
       */
      def when(condition: Boolean)(other: => Frag): Frag = if condition then left ++ other else left
