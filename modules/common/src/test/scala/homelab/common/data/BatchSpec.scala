package homelab.common.data


import zio.test.*


object BatchSpec extends ZIOSpecDefault:

  sealed trait Request { def id: Int }
  final case class Create(id: Int) extends Request
  final case class Update(id: Int) extends Request

  def spec = suite("Batch")(
    test("partitionMap splits a union batch by type; each half is processed and overlaid back, complete and in order") {
      val base = Batch.make(List[Request](Create(1), Update(2), Create(3), Update(4)))

      // Split the mixed batch into two typed, disjoint halves.
      val (creates, updates) = base.partitionMap {
        case c: Create => Left(c)
        case u: Update => Right(u)
      }

      // Each half goes through its own bulk op, modelled here as a keyed lookup table.
      val created = Map(1 -> "created-1", 3 -> "created-3")
      val updated = Map(2 -> "updated-2") // 4 is absent -> resolves to a not-found error

      val createdPart = creates.replaceWith(created)(_.id, c => s"no create ${c.id}")
      val updatedPart = updates.replaceWith(updated)(_.id, u => s"no update ${u.id}")

      // Recombine onto a same-lineage constant base; every slot is covered by exactly one half.
      val result = base.defaultError("unprocessed").overlays(createdPart, updatedPart)

      assertTrue(
        result.map(_.toList) == Right(
          List(
            Right("created-1"),
            Right("updated-2"),
            Right("created-3"),
            Left("no update 4"),
          )
        )
      )
    },
    test("values, errors and toList preserve index order regardless of Map iteration") {
      val batch = Batch.make((1 to 20).toList).mapEither(n => if n % 2 == 0 then Left(s"even-$n") else Right(n * 10))

      assertTrue(
        batch.values == (1 to 19 by 2).map(_ * 10).toList,
        batch.errors == (2 to 20 by 2).map(n => s"even-$n").toList,
        batch.toList.head == Right(10),
        batch.toList.last == Left("even-20"),
      )
    },
    test("overlay across distinct lineages fails with LineageMismatch") {
      val a = Batch.make(List(1, 2, 3))
      val b = Batch.make(List(4, 5, 6))

      assertTrue(a.overlay(b.partial) == Left(Batch.LineageMismatch))
    },
  )
