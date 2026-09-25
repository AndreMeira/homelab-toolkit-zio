package homelab.incubator.llm.v4.schema


import zio.Scope
import zio.json.*
import zio.test.*

import scala.collection.immutable.ListMap


/** What the ADT renders to, and the one invariant its types cannot carry. */
object JsonSchemaSpec extends ZIOSpecDefault:

  private def json(schema: JsonSchema): String = schema.json.toJson

  def spec: Spec[TestEnvironment & Scope, Any] = suite("JsonSchema")(
    suite("objects")(
      test("renders computed required and closed properties") {
        val schema = JsonSchema(
          Node.obj(
            Shape.Obj.Field("city", Node.text.describedAs("city name")),
            Shape.Obj.Field("unit", Node.enumeration("c", "f"), required = false),
          )
        )
        assertTrue(
          json(schema) ==
            """{"type":"object","properties":{"city":{"description":"city name","type":"string"},""" +
              """"unit":{"type":"string","enum":["c","f"]}},"required":["city"],"additionalProperties":false}"""
        )
      },
      test("an object with no properties requires nothing and still closes") {
        // `Node.obj()` is ambiguous with no arguments — both overloads match — so the shape is built directly.
        assertTrue(
          json(JsonSchema(Node(Shape.Obj(Nil)))) ==
            """{"type":"object","properties":{},"required":[],"additionalProperties":false}"""
        )
      },
    ),
    suite("primitives")(
      test("each renders as its own type") {
        assertTrue(
          json(JsonSchema(Node.text)) == """{"type":"string"}""",
          json(JsonSchema(Node.number)) == """{"type":"number"}""",
          json(JsonSchema(Node.integer)) == """{"type":"integer"}""",
          json(JsonSchema(Node.boolean)) == """{"type":"boolean"}""",
          json(JsonSchema(Node.nothing)) == """{"type":"null"}""",
        )
      },
      test("a formatted string keeps its format") {
        assertTrue(json(JsonSchema(Node.formatted("date-time"))) == """{"type":"string","format":"date-time"}""")
      },
      test("a description is rendered first, before the mechanics of the shape") {
        assertTrue(json(JsonSchema(Node.text.describedAs("the city"))) == """{"description":"the city","type":"string"}""")
      },
    ),
    suite("composites")(
      test("an array carries its item schema") {
        assertTrue(json(JsonSchema(Node.array(Node.text))) == """{"type":"array","items":{"type":"string"}}""")
      },
      test("a union renders every branch in order") {
        assertTrue(
          json(JsonSchema(Node.anyOf(Node.text, Node.integer))) ==
            """{"anyOf":[{"type":"string"},{"type":"integer"}]}"""
        )
      },
      test("nullable is a union with null, which is what strict mode wants") {
        assertTrue(
          json(JsonSchema(Node.nullable(Node.text))) == """{"anyOf":[{"type":"string"},{"type":"null"}]}"""
        )
      },
      test("an enumeration renders as a closed set of strings") {
        assertTrue(json(JsonSchema(Node.enumeration("red", "green"))) == """{"type":"string","enum":["red","green"]}""")
      },
    ),
    suite("definitions")(
      test("a document carries its table at the end, and only when it has one") {
        val schema = JsonSchema(
          root = Node.obj(Shape.Obj.Field("children", Node.array(Node.ref("Tree")))),
          definitions = ListMap("Tree" -> Node.obj(Shape.Obj.Field("value", Node.text))),
        )
        assertTrue(
          json(schema) ==
            """{"type":"object","properties":{"children":{"type":"array","items":{"$ref":"#/$defs/Tree"}}},""" +
              """"required":["children"],"additionalProperties":false,"$defs":{"Tree":{"type":"object",""" +
              """"properties":{"value":{"type":"string"}},"required":["value"],"additionalProperties":false}}}"""
        )
      },
      test("a closed schema reports nothing unresolved") {
        val schema = JsonSchema(
          root = Node.ref("Tree"),
          definitions = ListMap("Tree" -> Node.obj(Shape.Obj.Field("value", Node.text))),
        )
        assertTrue(schema.unresolved.isEmpty)
      },
      test("a dangling reference is reported rather than pretended away") {
        assertTrue(JsonSchema(Node.ref("Missing")).unresolved == Set("Missing"))
      },
      test("a reference dangling inside a definition is found too") {
        val schema = JsonSchema(
          root = Node.ref("Tree"),
          definitions = ListMap("Tree" -> Node.obj(Shape.Obj.Field("child", Node.ref("Nowhere")))),
        )
        assertTrue(schema.unresolved == Set("Nowhere"))
      },
      test("a reference reachable through an array or a union is found") {
        val schema = JsonSchema(Node.anyOf(Node.array(Node.ref("A")), Node.ref("B")))
        assertTrue(schema.unresolved == Set("A", "B"))
      },
    ),
  )
