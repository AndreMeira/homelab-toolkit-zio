package homelab.incubator.llm.v2


import homelab.incubator.llm.v2.JsonSchema.Shape
import zio.Scope
import zio.json.*
import zio.schema.{ Schema, derived }
import zio.schema.annotation.{ caseName, description, discriminatorName }
import zio.test.*


/** What comes out of `zio.schema.Schema`, including the recursive case that is the reason `$ref` exists. */
object JsonSchemaDerivationSpec extends ZIOSpecDefault:

  @description("Search the knowledge base")
  private final case class Query(
    @description("the natural-language question") text: String,
    limit: Option[Int],
  ) derives Schema

  /** The motivating recursion: a type reachable from itself, whose instances are almost always shallow. */
  private final case class Person(name: String, partner: Option[Person]) derives Schema

  private enum Direction derives Schema:
    case Ascending, Descending

  private final case class Unrenderable(lookup: Map[String, Int]) derives Schema

  // Carries data, and says how a decoder will tell its cases apart.
  @discriminatorName("kind")
  private enum Figure derives Schema:
    case Circle(radius: Double)
    @caseName("rectangle") case Rect(width: Double, height: Double)

  // Carries data, and says nothing — so nothing can read it back.
  private enum Untagged derives Schema:
    case Left(value: Int)
    case Right(value: Int)

  def spec: Spec[TestEnvironment & Scope, Any] = suite("JsonSchemaDerivation")(
    test("inlines a plain record, carrying descriptions and computing required") {
      assertTrue(
        JsonSchema.derive[Query].map(_.json.toJson) ==
          Right(
            """{"description":"Search the knowledge base","type":"object","properties":{""" +
              """"text":{"description":"the natural-language question","type":"string"},""" +
              """"limit":{"anyOf":[{"type":"integer"},{"type":"null"}]}},"required":["text"],"additionalProperties":false}"""
          )
      )
    },
    test("hoists a recursive type into a definition and refers to it") {
      val document = JsonSchema.derive[Person]
      assertTrue(
        document.map(_.root.shape) == Right(Shape.Reference("Person")),
        document.map(_.definitions.keySet) == Right(Set("Person")),
        document.map(_.unresolved) == Right(Set.empty[String]),
        document.map(_.json.toJson).exists(_.contains(""""partner":{"anyOf":[{"$ref":"#/$defs/Person"},{"type":"null"}]}""")),
        document.map(_.json.toJson).exists(_.contains(""""required":["name"]""")),
      )
    },
    test("says optional wherever it appears, not only on a field") {
      // The loophole this closes: an Option inside a list used to render as the bare inner type, while the
      // codec derived from the same schema reads and writes null there.
      final case class Holder(tags: List[Option[String]]) derives Schema
      assertTrue(
        JsonSchema.derive[Holder].map(_.json.toJson) ==
          Right(
            """{"type":"object","properties":{"tags":{"type":"array","items":{"anyOf":[{"type":"string"},""" +
              """{"type":"null"}]}}},"required":["tags"],"additionalProperties":false}"""
          )
      )
    },
    test("renders a payload-free enum as a closed set of names") {
      assertTrue(
        JsonSchema.derive[Direction].map(_.json.toJson) == Right("""{"type":"string","enum":["Ascending","Descending"]}""")
      )
    },
    test("tags each branch of a sum type that says how it is discriminated") {
      assertTrue(
        JsonSchema.derive[Figure].map(_.json.toJson) ==
          Right(
            """{"anyOf":[{"type":"object","properties":{"kind":{"type":"string","enum":["Circle"]},""" +
              """"radius":{"type":"number"}},"required":["kind","radius"],"additionalProperties":false},""" +
              """{"type":"object","properties":{"kind":{"type":"string","enum":["rectangle"]},""" +
              """"width":{"type":"number"},"height":{"type":"number"}},""" +
              """"required":["kind","width","height"],"additionalProperties":false}]}"""
          )
      )
    },
    test("refuses a sum type that carries data without saying how to tell its cases apart") {
      // Bare branches would be ambiguous to the model and unreadable to any decoder.
      assertTrue(JsonSchema.derive[Untagged].left.map(_.reason.contains("@discriminatorName")) == Left(true))
    },
    test("refuses a type the subset cannot express, with a reason") {
      assertTrue(
        JsonSchema.derive[Unrenderable] ==
          Left(JsonSchema.Unsupported("a map has open keys; this subset closes every object"))
      )
    },
    test("the given encoder describes a type with no value in hand") {
      assertTrue(summon[JsonSchema.Encoder[Query]].get.map(_.json.toJson).exists(_.contains(""""type":"object"""")))
    },
    test("the given encoder carries a recursive type's definitions with it") {
      // Why `get` returns a Document: `Person` describes itself as a `$ref`, which is meaningless without the
      // table it points at.
      assertTrue(
        summon[JsonSchema.Encoder[Person]].get.map(_.definitions.keySet) == Right(Set("Person")),
        summon[JsonSchema.Encoder[Person]].get.map(_.unresolved) == Right(Set.empty[String]),
      )
    },
    test("an undescribable type yields a reason instead of throwing") {
      // The summon itself must succeed — the rejection is data, so a registry can collect every bad tool and
      // report them together rather than dying on the first one.
      val encoder = summon[JsonSchema.Encoder[Unrenderable]]
      assertTrue(encoder.get.left.map(_.reason.contains("map")) == Left(true))
    },
  )
