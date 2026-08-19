package homelab.incubator.llm.v2


import homelab.incubator.llm.v2.Schemas.{ Entry, mapAsEntries }
import zio.Scope
import zio.json.*
import zio.schema.{ Schema, derived }
import zio.test.*


/** A `Map` made describable by describing it as something the subset already admits. */
object SchemasSpec extends ZIOSpecDefault:

  /** Opting in, for this pair only. */
  private given Schema[Map[String, Int]] = mapAsEntries

  private final case class Tagged(name: String, scores: Map[String, Int]) derives Schema

  /** A map of a pair that was *not* opted in — still outside the subset. */
  private final case class Untagged(flags: Map[String, Boolean]) derives Schema

  /** The decode direction of the transform, reached without needing a JSON codec. */
  private def decode(entries: List[Entry[String, Int]]): Either[String, Map[String, Int]] =
    mapAsEntries[String, Int] match
      case transform: Schema.Transform[?, ?, ?] =>
        transform.asInstanceOf[Schema.Transform[List[Entry[String, Int]], Map[String, Int], ?]].f(entries)
      case other => Left(s"expected a transform, got $other")

  def spec: Spec[TestEnvironment & Scope, Any] = suite("Schemas")(
    test("renders a map as an array of closed key/value objects") {
      assertTrue(
        JsonSchema.derive[Tagged].map(_.json.toJson) ==
          Right(
            """{"type":"object","properties":{"name":{"type":"string"},"scores":{"type":"array",""" +
              """"items":{"type":"object","properties":{"key":{"type":"string"},"value":{"type":"integer"}},""" +
              """"required":["key","value"],"additionalProperties":false}}},""" +
              """"required":["name","scores"],"additionalProperties":false}"""
          )
      )
    },
    test("leaves a map of any other key/value pair outside the subset") {
      // The opt-in is per pair: `Map[String, Int]` describes, `Map[String, Boolean]` still does not.
      assertTrue(JsonSchema.derive[Untagged].left.map(_.reason.contains("map")) == Left(true))
    },
    test("accepts distinct keys") {
      assertTrue(decode(List(Entry("a", 1), Entry("b", 2))) == Right(Map("a" -> 1, "b" -> 2)))
    },
    test("refuses duplicate keys rather than keeping the last silently") {
      // The model gets a reason it can act on; `toMap` would have produced a one-entry map and said nothing.
      assertTrue(decode(List(Entry("a", 1), Entry("a", 2))) == Left("duplicate keys: a"))
    },
  )
