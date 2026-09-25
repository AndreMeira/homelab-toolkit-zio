package homelab.llm.schema


import zio.Scope
import zio.json.*
import zio.schema.annotation.{ caseName, description, discriminatorName }
import zio.schema.{ Schema, derived }
import zio.test.*


/** What comes out of a `zio.schema.Schema`, including the recursive case that is the reason `$defs` exists. */
object GeneratorSpec extends ZIOSpecDefault:

  @description("Search the knowledge base")
  final private case class Query(
    @description("the natural-language question") text: String,
    limit: Option[Int],
  ) derives Schema

  // The motivating recursion: a type reachable from itself, whose instances are almost always shallow.
  // Deliberately not a scaladoc — see the test that a doc comment ships to the model.
  final private case class Person(name: String, partner: Option[Person]) derives Schema

  /** Chosen by whoever is asking. */
  final private case class Documented(value: String) derives Schema

  final private case class Boxed(values: List[Option[String]]) derives Schema

  final private case class Empty() derives Schema

  final private case class Wrapped(inner: Query) derives Schema

  private enum Direction derives Schema:
    case Ascending, Descending

  private enum Renamed derives Schema:
    @caseName("asc") case Ascending
    @caseName("desc") case Descending

  @discriminatorName("kind")
  private enum Ending derives Schema:
    case Done(summary: String)
    case GiveUp(reason: String)

  private enum Undiscriminated derives Schema:
    case Done(summary: String)
    case GiveUp(reason: String)

  @discriminatorName("kind")
  private enum Colliding derives Schema:
    case Done(kind: String)

  final private case class Mapped(lookup: Map[String, Int]) derives Schema

  final private case class Tupled(pair: (String, Int)) derives Schema

  private def rendered[A: Schema]: Either[String, String] =
    Generator.generate[A].left.map(_.message).map(_.json.toJson)

  private def refused[A: Schema]: String =
    Generator.generate[A].fold(_.message, schema => s"unexpectedly described as ${schema.json.toJson}")

  def spec: Spec[TestEnvironment & Scope, Any] = suite("Generator")(
    suite("records")(
      test("inlines a plain record, carrying descriptions and computing required") {
        assertTrue(
          rendered[Query] == Right(
            """{"description":"Search the knowledge base","type":"object","properties":""" +
              """{"text":{"description":"the natural-language question","type":"string"},""" +
              """"limit":{"anyOf":[{"type":"integer"},{"type":"null"}]}},""" +
              """"required":["text"],"additionalProperties":false}"""
          )
        )
      },
      test("a record with no fields is an object that requires nothing") {
        assertTrue(
          rendered[Empty] == Right("""{"type":"object","properties":{},"required":[],"additionalProperties":false}""")
        )
      },
      test("a nested record is inlined too, since only recursion needs a definition") {
        assertTrue(rendered[Wrapped].exists(json => json.contains("\"inner\"") && !json.contains("$defs")))
      },
    ),
    suite("recursion")(
      test("hoists a recursive type into a definition and refers to it") {
        assertTrue(
          rendered[Person] == Right(
            """{"$ref":"#/$defs/Person","$defs":{"Person":{"type":"object","properties":""" +
              """{"name":{"type":"string"},"partner":{"anyOf":[{"$ref":"#/$defs/Person"},{"type":"null"}]}},""" +
              """"required":["name"],"additionalProperties":false}}}"""
          )
        )
      },
      test("a schema that refers to nothing carries no definitions") {
        assertTrue(Generator.generate[Query].map(_.definitions.isEmpty) == Right(true))
      },
      test("what it produces for a recursive type resolves") {
        assertTrue(Generator.generate[Person].map(_.unresolved) == Right(Set.empty[String]))
      },
    ),
    suite("sum types")(
      test("renders a payload-free enum as a closed set of names") {
        assertTrue(rendered[Direction] == Right("""{"type":"string","enum":["Ascending","Descending"]}"""))
      },
      test("a renamed case is advertised by the name the decoder reads") {
        assertTrue(rendered[Renamed] == Right("""{"type":"string","enum":["asc","desc"]}"""))
      },
      test("tags each branch of a sum type that says how it is discriminated") {
        assertTrue(
          rendered[Ending] == Right(
            """{"anyOf":[{"type":"object","properties":{"kind":{"type":"string","enum":["Done"]},""" +
              """"summary":{"type":"string"}},"required":["kind","summary"],"additionalProperties":false},""" +
              """{"type":"object","properties":{"kind":{"type":"string","enum":["GiveUp"]},""" +
              """"reason":{"type":"string"}},"required":["kind","reason"],"additionalProperties":false}]}"""
          )
        )
      },
      test("refuses a sum type that carries data without saying how to tell its cases apart") {
        assertTrue(refused[Undiscriminated].contains("discriminatorName"))
      },
      test("refuses a case that would carry the discriminator twice") {
        assertTrue(refused[Colliding].contains("already has a property named 'kind'"))
      },
    ),
    suite("outside the subset")(
      test("refuses a map, because a map has open keys") {
        assertTrue(refused[Mapped].contains("open keys"))
      },
      test("refuses a tuple, because a tuple needs positional items") {
        assertTrue(refused[Tupled].contains("positional"))
      },
    ),
    suite("descriptions")(
      test("a scaladoc ships to the model as written, markers and all") {
        // The sharpest edge in the derivation: a comment written for a developer becomes prompt text.
        assertTrue(
          rendered[Documented] == Right(
            """{"description":"/** Chosen by whoever is asking. */","type":"object",""" +
              """"properties":{"value":{"type":"string"}},"required":["value"],"additionalProperties":false}"""
          )
        )
      },
    ),
    suite("optionality")(
      test("says optional wherever it appears, not only on a field") {
        assertTrue(
          rendered[Boxed] == Right(
            """{"type":"object","properties":{"values":{"type":"array","items":""" +
              """{"anyOf":[{"type":"string"},{"type":"null"}]}}},""" +
              """"required":["values"],"additionalProperties":false}"""
          )
        )
      },
    ),
  )
