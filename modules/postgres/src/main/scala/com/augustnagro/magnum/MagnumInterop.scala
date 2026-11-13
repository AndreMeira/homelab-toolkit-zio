package com.augustnagro.magnum


import zio.NonEmptyChunk

import java.sql.Connection


/**
 * Interop helpers for Magnum, living in Magnum's package to reach `DbTx`'s package-private constructor
 * and to build `Frag`s directly. Provides:
 *
 *   - [[makeDbTx]] — a `DbTx` from a raw JDBC connection, so the ZIO transaction wrapper
 *     ([[homelab.postgres.PostgresTransaction]]) can drive Magnum against a connection it manages itself.
 *   - [[combine]], [[combineAll]] and [[syntax]] — a small fragment-concatenation algebra Magnum lacks out
 *     of the box: compose `Frag`s with `and`/`or`/`comma`/`++`, join a whole list (a multi-row `VALUES`),
 *     splice conditionally with `when`, and start from [[syntax.emptyFragment]]. It threads each fragment's
 *     `writer` so bound parameters land at the right `?` positions.
 *   - [[fragment]] — builders over lists, after Doobie's `fragments` (`comma`, `in`, `notIn`, `values`,
 *     `parentheses`), to nest in `sql"…"`: Magnum's `sql` binds a collection as a single parameter.
 */
object MagnumInterop:

  /**
   * Build a `DbTx` from a JDBC connection using the inmemory SQL logger.
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
    combineAll(List(left, right), separator)

  /**
   * Combine any number of SQL fragments with a separator, dropping empty fragments and threading their
   * writers so parameters keep their positions. Joining one fragment per row gives a multi-row `VALUES`
   * list.
   *
   * @param fragments the fragments, in order
   * @param separator the string between them (e.g. `", "`)
   * @return the combined fragment; empty when every fragment is
   */
  def combineAll(fragments: Iterable[Frag], separator: String): Frag =
    val nonEmpty = fragments.view.filter(_.sqlString.nonEmpty).toList
    Frag(
      sqlString = nonEmpty.map(_.sqlString).mkString(separator),
      params = nonEmpty.flatMap(_.params),
      writer = (statement, pos) =>
        nonEmpty.foldLeft(pos) { (currentPos, frag) =>
          frag.writer.write(statement, currentPos)
        },
    )

  /**
   * Fragment builders over lists of values or fragments, after Doobie's `fragments`: `comma`, `in`, `notIn`,
   * `values`, `parentheses`. Each returns one `Frag`, to nest in `sql"…"` (Magnum 2 splices a `Frag`
   * argument), so `sql"SELECT * FROM members WHERE ${fragment.in(sql"id", ids)}"`.
   *
   * A value binds through its `DbCodec`. One spanning several columns is a row, rendered in parentheses: a
   * tuple codec already renders `(?, ?)`, a derived case-class codec renders `?, ?` and gets them added.
   *
   * Doobie keeps empty lists out with non-empty types. Here only `values` needs that, so it takes a
   * `NonEmptyChunk`: `VALUES` with no row has no valid rendering. The others take any `Iterable` and render
   * the exact meaning of an empty list (`FALSE` for `in`, `TRUE` for `notIn`, nothing for `comma`).
   */
  object fragment:

    /**
     * Join fragments with commas.
     *
     * @param fragments the fragments, in order
     * @return `f1, f2, …`; empty when `fragments` is
     */
    def comma(fragments: Iterable[Frag]): Frag =
      combineAll(fragments, ", ")

    /**
     * Wrap a fragment in parentheses.
     *
     * @param fragment the fragment to wrap
     * @return `(fragment)`, with the same parameters
     */
    def parentheses(fragment: Frag): Frag =
      Frag(s"(${fragment.sqlString})", fragment.params, fragment.writer)

    /**
     * Test a column, or a row of columns, against a list of values.
     *
     * @param column the left-hand side, such as `sql"id"` or `sql"(id, attempts)"`
     * @param values the values to match, one bound element each
     * @param codec how one value binds
     * @tparam A the value type
     * @return `column IN (?, ?, …)`; `FALSE` alone, without `column`, when `values` is empty, since Postgres
     *   rejects `IN ()` and an empty list matches nothing
     */
    def in[A](column: Frag, values: Iterable[A])(using codec: DbCodec[A]): Frag =
      if values.isEmpty then sql"FALSE"
      else sql"$column IN ${parentheses(comma(values.map(element)))}"

    /**
     * Test that a column, or a row of columns, matches none of a list of values.
     *
     * @param column the left-hand side, such as `sql"id"` or `sql"(id, attempts)"`
     * @param values the values to exclude, one bound element each
     * @param codec how one value binds
     * @tparam A the value type
     * @return `column NOT IN (?, ?, …)`; `TRUE` alone, without `column`, when `values` is empty, since
     *   Postgres rejects `NOT IN ()` and an empty list excludes nothing
     */
    def notIn[A](column: Frag, values: Iterable[A])(using codec: DbCodec[A]): Frag =
      if values.isEmpty then sql"TRUE"
      else sql"$column NOT IN ${parentheses(comma(values.map(element)))}"

    /**
     * A multi-row `VALUES` list, one parenthesised row per value.
     *
     * @param rows the rows; non-empty, since `VALUES` with no row is a syntax error
     * @param codec how one row binds
     * @tparam A the row type: a single value, a tuple, or a case class with a derived codec
     * @return `VALUES (?, ?), (?, ?), …`
     */
    def values[A](rows: NonEmptyChunk[A])(using codec: DbCodec[A]): Frag =
      sql"VALUES ${comma(rows.toChunk.map(row))}"

    /**
     * One value as an element of an `IN` list: a single column bare, several as a row.
     *
     * @param value the value
     * @param codec how it binds
     * @tparam A the value type
     * @return `?` for a single column, `(?, ?, …)` otherwise
     */
    private def element[A](value: A)(using codec: DbCodec[A]): Frag =
      if codec.cols.length == 1 then sql"$value" else row(value)

    /**
     * One value as a parenthesised row, adding the parentheses only when its codec doesn't render them (a
     * tuple codec does, a derived case-class codec doesn't).
     *
     * @param value the value
     * @param codec how it binds
     * @tparam A the value type
     * @return `(?, ?, …)` binding `value`
     */
    private def row[A](value: A)(using codec: DbCodec[A]): Frag =
      val bound = sql"$value"
      if bound.sqlString.startsWith("(") then bound else parentheses(bound)

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
      def and(right: Frag): Frag =
        combine(left, right, " AND ")

      /**
       * Combine with `right` using `" OR "`.
       *
       * @param right the fragment to append
       * @return the combined fragment
       */
      def or(right: Frag): Frag =
        combine(left, right, " OR ")

      /**
       * Combine with `right` using `", "`.
       *
       * @param right the fragment to append
       * @return the combined fragment
       */
      def comma(right: Frag): Frag =
        combine(left, right, ", ")

      /**
       * Combine with `right` using a single space.
       *
       * @param right the fragment to append
       * @return the combined fragment
       */
      def ++(right: Frag): Frag =
        combine(left, right, " ")

      /**
       * Combine with `right` using a custom separator.
       *
       * @param separator the string between the fragments
       * @param right the fragment to append
       * @return the combined fragment
       */
      def concat(separator: String)(right: Frag): Frag =
        combine(left, right, separator)

      /**
       * Append `other` only when `condition` holds (a space-separated splice) — for optional clauses.
       *
       * @param condition whether to append `other`
       * @param other the fragment to conditionally append
       * @return `left ++ other` when `condition` holds, otherwise `left`
       */
      def when(condition: Boolean)(other: => Frag): Frag =
        if condition then left ++ other else left
