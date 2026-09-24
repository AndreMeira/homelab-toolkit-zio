package homelab.incubator.llm.v3.compiletime


import zio.Scope
import zio.schema.{ DeriveSchema, Schema }
import zio.test.*


/** What plain `inline` code can decide about a type, with no value in hand. */
object MirroredSpec extends ZIOSpecDefault:

  final case class Search(text: String, limit: Option[Int])

  final case class Unschemad(whatever: String)

  given Schema[Search] = DeriveSchema.gen[Search]

  def spec: Spec[TestEnvironment & Scope, Any] = suite("Mirrored")(
    test("field names are known at compile time, from the type alone") {
      assertTrue(Mirrored.labels[Search] == List("text", "limit"))
    },
    test("whether a given exists is decided while compiling") {
      assertTrue(
        Mirrored.hasInstance[Search, Schema],
        !Mirrored.hasInstance[Unschemad, Schema],
      )
    },
  )
