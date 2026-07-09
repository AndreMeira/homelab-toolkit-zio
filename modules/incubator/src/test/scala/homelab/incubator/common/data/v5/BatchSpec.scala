package homelab.incubator.common.data.v5


import zio.test.*


object BatchSpec extends ZIOSpecDefault:

  def spec = suite("Batch v5")(
    test("overlay fold: default fallback filled by a partial, complete and in order") {
      val base    = Batch.make(List("a", "b", "c"))
      val found   = base.success.mapEither(v => if v == "b" then Left("missing") else Right(v.toUpperCase))
      val result  = base.defaultError("nf").overlays(found).map(_.toList)
      assertTrue(result == Right(List(Right("A"), Left("missing"), Right("C"))))
    },
    test("collect keeps errors, drops non-matching successes, maps matches") {
      val base  = Batch.make(List(1, 2, 3))
      val mixed = base.success.mapEither(v => if v == 2 then Left("bad") else Right(v)) // 0->1, 1->bad, 2->3
      val out   = base.defaultError("none").overlays(mixed).map { batch =>
        batch.collect { case v if v > 1 => v * 10 }.toList // drop slot0(=1), keep slot1(err), map slot2(3->30)
      }
      assertTrue(out == Right(List(Left("bad"), Right(30))))
    },
    test("Success.collect narrows to a Partial") {
      val base     = Batch.make(List(1, 2, 3, 4))
      val narrowed = base.success.collect { case v if v % 2 == 0 => v * 10 } // slots 1,3 -> 20,40
      val _: Batch.Partial[Nothing, Int] = narrowed                          // it is a Partial (compile check)
      assertTrue(narrowed.toList == List(Right(20), Right(40)))
    },
    test("overlaying a piece from another lineage fails with LineageMismatch") {
      val base    = Batch.make(List(1, 2))
      val foreign = Batch.make(List(3, 4)).success
      assertTrue(base.overlay(foreign) == Left(Batch.LineageMismatch))
    },
    test("several disjoint overlays reunite onto the canvas; uncovered slots keep the default") {
      val base = Batch.make(List(10, 20, 30, 40, 50))
      val p1   = base.success.collect { case 10 => "ten" }    // slot 0
      val p2   = base.success.collect { case 30 => "thirty" } // slot 2
      val p3   = base.success.collect { case 50 => "fifty" }  // slot 4
      val out  = base.defaultError("none").overlays(p1, p2, p3).map(_.toList)
      assertTrue(
        out == Right(
          List(Right("ten"), Left("none"), Right("thirty"), Left("none"), Right("fifty")),
        ),
      )
    },
    test("overlapping overlays: the later one wins") {
      val base   = Batch.make(List(1, 2))
      val first  = base.success.collect { case 1 => "first" }  // slot 0
      val second = base.success.collect { case 1 => "second" } // slot 0 again
      val out    = base.defaultError("x").overlays(first, second).map(_.toList)
      assertTrue(out == Right(List(Right("second"), Left("x"))))
    },
    test("partition splits successes by predicate, and overlaying both halves reconstructs the whole") {
      val base          = Batch.make(List(1, 2, 3, 4))
      val (evens, odds) = base.partition(_ % 2 == 0)
      assertTrue(
        evens.toList == List(Right(2), Right(4)),
        odds.toList == List(Right(1), Right(3)),
        base.defaultError("gap").overlays(evens, odds).map(_.toList) ==
          Right(List(Right(1), Right(2), Right(3), Right(4))),
      )
    },
    test("Success.partitionMap routes each value into one of two typed groups") {
      val base          = Batch.make(List(1, 2, 3, 4, 5))
      val (evens, odds) = base.success.partitionMap(v => if v % 2 == 0 then Left(s"even-$v") else Right(v * 100))
      assertTrue(
        evens.toList == List("even-2", "even-4"), // slots 1, 3 — type String
        odds.toList == List(100, 300, 500),        // slots 0, 2, 4 — type Int
      )
    },
    test("Indexed.replaceWith(orError) left-joins: unmatched originals become errors") {
      val base    = Batch.make(List("a", "b", "c"))
      val indexed = base.success.indexBy(identity) // keyed by the id itself
      val rows    = List("a" -> 1, "c" -> 3)        // "b" absent from the returned rows
      val out     = indexed.replaceWith(rows, "not found")(_._1)
      assertTrue(out.toList == List(Right("a" -> 1), Left("not found"), Right("c" -> 3)))
    },
    test("separate splits a batch into its (error, success) channels") {
      val base  = Batch.make(List(1, 2, 3))
      val mixed = base.success.mapEither(v => if v == 2 then Left("bad") else Right(v * 10)) // 0->10, 1->bad, 2->30
      val out   = base.defaultError("x").overlays(mixed).map { batch =>
        val (errors, successes) = batch.separate
        (errors.toList, successes.toList)
      }
      assertTrue(out == Right((List("bad"), List(10, 30))))
    },
    test("partition puts errors in the remainder side") {
      val base  = Batch.make(List(1, 2, 3))
      val mixed = base.success.mapEither(v => if v == 2 then Left("bad") else Right(v)) // 0->1, 1->bad, 2->3
      val out   = base.defaultError("x").overlays(mixed).map { batch =>
        val (big, rest) = batch.partition(_ > 1) // big: slot2(3); rest: slot0(1) + slot1(error)
        (big.toList, rest.toList)
      }
      assertTrue(out == Right((List(Right(3)), List(Right(1), Left("bad")))))
    },
  )
