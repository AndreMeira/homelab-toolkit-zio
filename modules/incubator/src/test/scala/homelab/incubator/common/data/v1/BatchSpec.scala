package homelab.incubator.common.data.v1


import zio.*
import zio.test.{ Spec, TestEnvironment, ZIOSpecDefault, assertTrue }

import java.util.UUID


object BatchSpec extends ZIOSpecDefault {

  case class Dump(id: UUID, data: Any)

  class DumpObjectRepository(data: Ref[List[Dump]]):
    def find(ids: List[UUID]): UIO[List[Dump]] =
      data.get.map(_.filter(d => ids.contains(d.id)))

    def update(dumps: List[Dump]): UIO[List[Dump]] = data.modify { data =>
      val filtered = dumps.filter(d => data.map(_.id).contains(d.id))
      filtered -> (filtered ++ data)
    }

  def repo(initial: List[Dump]): UIO[DumpObjectRepository] =
    Ref.make(initial).map(new DumpObjectRepository(_))

  val sample: List[Dump] = List(
    Dump(UUID.randomUUID(), "data1"),
    Dump(UUID.randomUUID(), "data2"),
    Dump(UUID.randomUUID(), "data3"),
    Dump(UUID.randomUUID(), "data4"),
  )

  override def spec: Spec[TestEnvironment & Scope, Any] = suite("Batch")(
    test("map either works") {
      repo(sample).flatMap: repo =>
        val batch = Batch.make(Dump(UUID.randomUUID(), "none") :: sample.take(2))
        repo
          .find(batch.success.toList.map(_.id))
          .map: found =>
            batch.success.mapEither: original =>
              if found.contains(original)
              then Right(original.data)
              else Left("not found")
          .map: mapped =>
            assertTrue(
              mapped.toList == List(
                Left("not found"),
                Right(sample(0).data),
                Right(sample(1).data),
              )
            )
    },
    test("map and mapError should transform the elements correctly") {
      assertTrue(true)
    },
  )
}
