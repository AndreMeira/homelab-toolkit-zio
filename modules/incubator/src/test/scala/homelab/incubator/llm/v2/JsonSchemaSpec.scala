package homelab.incubator.llm.v2


import homelab.incubator.llm.v2.JsonSchema.Field
import zio.Scope
import zio.json.*
import zio.test.*

import scala.collection.immutable.ListMap


/** What the ADT renders — the only contract that matters, since a provider reads the output, not the types. */
object JsonSchemaSpec extends ZIOSpecDefault:

  def spec: Spec[TestEnvironment & Scope, Any] = suite("JsonSchema")(
    test("renders an object with computed required and closed properties") {
      val schema = JsonSchema
        .obj(
          "text"  -> Field(JsonSchema.text.describedAs("the question")),
          "limit" -> Field(JsonSchema.integer, required = false),
        )
        .describedAs("Search the knowledge base")

      assertTrue(
        schema.json.toJson ==
          """{"description":"Search the knowledge base","type":"object",""" +
          """"properties":{"text":{"description":"the question","type":"string"},""" +
          """"limit":{"type":"integer"}},"required":["text"],"additionalProperties":false}"""
      )
    },
    test("says optional as a nullable union, which is what strict mode wants") {
      assertTrue(JsonSchema.nullable(JsonSchema.text).json.toJson == """{"anyOf":[{"type":"string"},{"type":"null"}]}""")
    },
    test("renders an enumeration as a closed set of strings") {
      assertTrue(
        JsonSchema.enumeration("asc", "desc").json.toJson == """{"type":"string","enum":["asc","desc"]}"""
      )
    },
    test("carries definitions for a recursive type, and reports a dangling reference") {
      val node = JsonSchema.obj(
        "value"    -> Field(JsonSchema.text),
        "children" -> Field(JsonSchema.array(JsonSchema.ref("Node"))),
      )
      val closed   = JsonSchema(JsonSchema.ref("Node"), ListMap("Node" -> node))
      val dangling = JsonSchema(JsonSchema.ref("Missing"))

      assertTrue(
        closed.unresolved.isEmpty,
        closed.json.toJson.contains("\"$defs\":{\"Node\""),
        closed.json.toJson.contains("\"$ref\":\"#/$defs/Node\""),
        dangling.unresolved == Set("Missing"),
      )
    },
  )
