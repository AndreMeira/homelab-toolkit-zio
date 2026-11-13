package com.augustnagro.magnum


import com.augustnagro.magnum.MagnumInterop.{ combineAll, fragment }
import com.augustnagro.magnum.MagnumInterop.syntax.*
import homelab.postgres.PostgresTransaction.Transactional
import homelab.postgres.{ PostgresDatabase, PostgresSpecLayers, PostgresTransaction }
import zio.*
import zio.test.*


/**
 * Tests for [[MagnumInterop]]'s list builders: the rendered SQL and parameter order of `combineAll` and the
 * `fragment` builders, alone and nested in `sql"…"` (Magnum 2 splices a `Frag` argument), then the same
 * fragments run against a real Postgres (Testcontainers), which checks that each writer binds its
 * parameters at the right positions. Requires a running Docker daemon.
 */
object MagnumInteropSpec extends ZIOSpecDefault:
  import Support.*

  def spec = suite("MagnumInterop")(
    test("combineAll joins every fragment with the separator and keeps the parameters in order") {
      val rows = combineAll(List(row(alpha), row(beta)), ", ")
      assertTrue(rows.sqlString == "(?, ?), (?, ?)", rows.params == List("i1", "alpha", "i2", "beta"))
    },
    test("combineAll drops empty fragments") {
      val rows = combineAll(List(emptyFragment, row(alpha), emptyFragment), ", ")
      assertTrue(rows.sqlString == "(?, ?)")
    },
    test("a fragment nested in sql renders in place and keeps its values in order, one per bound value") {
      val predicate = sql"${fragment.in(sql"(id, name)", List(alpha, beta))} AND name <> ${"stale"}"
      assertTrue(
        predicate.sqlString == "(id, name) IN ((?, ?), (?, ?)) AND name <> ?",
        predicate.params == List(alpha, beta, "stale"),
      )
    },
    suite("fragment")(
      test("in binds one bare parameter per single-column value") {
        val predicate = fragment.in(sql"id", List("i1", "i2", "i3"))
        assertTrue(predicate.sqlString == "id IN (?, ?, ?)", predicate.params == List("i1", "i2", "i3"))
      },
      test("in binds a tuple as a row") {
        assertTrue(fragment.in(sql"(id, name)", List(("i1", "alpha"))).sqlString == "(id, name) IN ((?, ?))")
      },
      test("in binds a case class as a row") {
        assertTrue(fragment.in(sql"(id, name)", List(alpha)).sqlString == "(id, name) IN ((?, ?))")
      },
      test("in over no values is FALSE alone") {
        assertTrue(fragment.in(sql"id", List.empty[String]).sqlString == "FALSE")
      },
      test("notIn renders NOT IN, and TRUE alone over no values") {
        assertTrue(
          fragment.notIn(sql"id", List("i1")).sqlString == "id NOT IN (?)",
          fragment.notIn(sql"id", List.empty[String]).sqlString == "TRUE",
        )
      },
      test("values puts every row in parentheses, single-column ones included") {
        assertTrue(
          fragment.values(NonEmptyChunk(alpha, beta)).sqlString == "VALUES (?, ?), (?, ?)",
          fragment.values(NonEmptyChunk("i1", "i2")).sqlString == "VALUES (?), (?)",
        )
      },
    ),
    suite("against Postgres")(
      test("a multi-row values insert writes every row") {
        for
          database <- ZIO.service[PostgresDatabase]
          inserted <- database.transaction(insert(NonEmptyChunk(alpha, beta, gamma)))
          stored   <- database.transaction(names(List("i1", "i2", "i3")))
        yield assertTrue(inserted == 3, stored == List("alpha", "beta", "gamma"))
      },
      test("a row in matches only the pairs given") {
        for
          database <- ZIO.service[PostgresDatabase]
          _        <- database.transaction(insert(NonEmptyChunk(delta, epsilon)))
          matched  <- database.transaction(countMatching(List(delta, epsilon.copy(name = "stale"))))
        yield assertTrue(matched == 1)
      },
      test("an update joined to a VALUES list in a WITH changes only the rows listed") {
        for
          database <- ZIO.service[PostgresDatabase]
          _        <- database.transaction(insert(NonEmptyChunk(zeta, eta, theta)))
          updated  <- database.transaction(rename(NonEmptyChunk(Rename("i6", "zeta2"), Rename("i7", "eta2"))))
          stored   <- database.transaction(names(List("i6", "i7", "i8")))
        yield assertTrue(updated == 2, stored == List("zeta2", "eta2", "theta"))
      },
    ).provideShared(PostgresSpecLayers.database) @@ TestAspect.sequential,
  )

  /** The widgets these tests render and write, and the statements built from them. */
  private object Support {

    /**
     * A row of the `widget` table.
     *
     * @param id the widget's id
     * @param name the widget's name
     */
    final case class Widget(id: String, name: String) derives DbCodec

    val alpha: Widget   = Widget("i1", "alpha")
    val beta: Widget    = Widget("i2", "beta")
    val gamma: Widget   = Widget("i3", "gamma")
    val delta: Widget   = Widget("i4", "delta")
    val epsilon: Widget = Widget("i5", "epsilon")
    val zeta: Widget    = Widget("i6", "zeta")
    val eta: Widget     = Widget("i7", "eta")
    val theta: Widget   = Widget("i8", "theta")

    /**
     * A new name for one widget: the dummy model the `WITH … UPDATE … FROM` test binds as rows.
     *
     * @param id the widget to rename
     * @param name its new name
     */
    final case class Rename(id: String, name: String) derives DbCodec

    /**
     * One widget as a row of bound parameters, built by hand.
     *
     * @param widget the widget
     * @return the fragment `(?, ?)` binding its id and name
     */
    def row(widget: Widget): Frag =
      sql"(${widget.id}, ${widget.name})"

    /**
     * Insert widgets with one multi-row `VALUES` statement.
     *
     * @param widgets the widgets
     * @return the number of rows inserted; aborts with the transaction's error
     */
    def insert(widgets: NonEmptyChunk[Widget]): ZIO[PostgresTransaction, PostgresTransaction.Error, Int] =
      Transactional(sql"insert into widget (id, name) ${fragment.values(widgets)}".update.run())

    /**
     * Rename widgets with one statement: the renames become a `VALUES` table in a `WITH`, and the update
     * joins it on the id.
     *
     * @param renames the new names
     * @return the number of rows updated; aborts with the transaction's error
     */
    def rename(renames: NonEmptyChunk[Rename]): ZIO[PostgresTransaction, PostgresTransaction.Error, Int] =
      Transactional(
        sql"""WITH input(id, name) AS (${fragment.values(renames)})
               UPDATE widget SET name = input.name FROM input WHERE widget.id = input.id""".update.run()
      )

    /**
     * Read the names of the widgets with the given ids.
     *
     * @param ids the ids to look up
     * @return the names, ordered by id; aborts with the transaction's error
     */
    def names(ids: List[String]): ZIO[PostgresTransaction, PostgresTransaction.Error, List[String]] =
      Transactional(sql"select name from widget where ${fragment.in(sql"id", ids)} order by id".query[String].run().toList)

    /**
     * Count the widgets whose id and name both match one of the given widgets.
     *
     * @param widgets the pairs to match
     * @return the number of matching rows; aborts with the transaction's error
     */
    def countMatching(widgets: List[Widget]): ZIO[PostgresTransaction, PostgresTransaction.Error, Int] =
      Transactional(sql"select count(*) from widget where ${fragment.in(sql"(id, name)", widgets)}".query[Int].run().headOption.getOrElse(0))
  }
