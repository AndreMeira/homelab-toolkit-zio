package homelab.llm.openai.request


import homelab.llm.schema.{ JsonSchema, Node, Shape }
import homelab.llm.{ Advertised, Message, Model, Tool }
import zio.json.*
import zio.json.ast.Json
import zio.test.*
import zio.{ Chunk, Scope }


/** What a request looks like on the wire, and what a conversation becomes on the way there. */
object CompletionRequestSpec extends ZIOSpecDefault:

  private val model = Model.Name("anthropic/claude-3.5-sonnet")

  private def text(value: String): Chunk[Message.Content] = Chunk(Message.Content.Text(value))

  private def id(value: String): Tool.Call.Id = Tool.Call.Id(value)

  private val weather = Advertised(
    "weather",
    "Report it.",
    JsonSchema(Node.obj(Shape.Obj.Field("city", Node.text))),
  )

  private def sent(messages: Message*): String =
    body(CompletionRequest(model, messages.map(MessageRequest.from).toList))

  private def body(tools: List[Advertised] = Nil, extra: Json.Obj = Json.Obj()): String =
    body(
      CompletionRequest(
        model,
        Nil,
        tools = Option.when(tools.nonEmpty)(tools.map(ToolRequest.from)),
        extra = extra,
      )
    )

  private def body(request: CompletionRequest): String = CompletionRequest.body(request).toJson

  def spec: Spec[TestEnvironment & Scope, Any] = suite("CompletionRequest")(
    suite("roles")(
      test("each role is sent under the name the wire uses, with content as parts") {
        assertTrue(
          sent(Message.system(text("be brief"))) ==
            s"""{"model":"$model","messages":[{"role":"system","content":[{"type":"text","text":"be brief"}]}]}""",
          sent(Message.user(text("hello"))).contains("""{"role":"user","content":[{"type":"text","text":"hello"}]}"""),
          sent(Message.assistant(text("hi"))).contains("""{"role":"assistant","content":"""),
        )
      },
      test("a tool result is sent as a string, which is the one place the shapes differ") {
        assertTrue(
          sent(Message.toolResult(id("c1"), text("""{"degrees":12.0}"""))).contains(
            """{"role":"tool","tool_call_id":"c1","content":"{\"degrees\":12.0}"}"""
          )
        )
      },
      test("a tool result made of several parts is run together, and a raw part is left out of it") {
        val parts   = Seq(Message.Content.Text("one"), Message.Content.Raw(Json.Str("x")), Message.Content.Text("two"))
        val content = Chunk.fromIterable(parts)
        assertTrue(sent(Message.toolResult(id("c1"), content)).contains(""""content":"onetwo""""))
      },
    ),
    suite("tool calls")(
      test("an assistant turn carries back the calls it made, arguments unparsed") {
        val turn = Message.assistant(Chunk.empty, Chunk(Tool.Call.Raw(id("c1"), "weather", """{"city":"Hamburg"}""")))
        assertTrue(
          sent(turn).contains(
            """"tool_calls":[{"id":"c1","type":"function",""" +
              """"function":{"name":"weather","arguments":"{\"city\":\"Hamburg\"}"}}]"""
          )
        )
      },
      test("a turn that asked for nothing carries no tool_calls at all") {
        assertTrue(!sent(Message.assistant(text("hi"))).contains("tool_calls"))
      },
      test("tools are offered only when there are any") {
        assertTrue(body(tools = List(weather)).contains(""""tools":["""), !body().contains("tools"))
      },
      test("a tool is wrapped in a function object, with its schema under parameters") {
        assertTrue(
          body(tools = List(weather)).contains(
            """"tools":[{"type":"function","function":{"name":"weather","description":"Report it.",""" +
              """"parameters":{"type":"object","properties":{"city":{"type":"string"}},""" +
              """"required":["city"],"additionalProperties":false}}}]"""
          )
        )
      },
    ),
    suite("content")(
      test("a raw part reaches the wire as it was written") {
        val image = Json.Obj("type" -> Json.Str("image_url"), "image_url" -> Json.Obj("url" -> Json.Str("data:…")))
        assertTrue(sent(Message.user(Chunk(Message.Content.Raw(image)))).contains(""""type":"image_url""""))
      }
    ),
    suite("extra")(
      test("a caller's own fields are merged into the body") {
        assertTrue(body(extra = Json.Obj("temperature" -> Json.Num(0.2))).contains(""""temperature":0.2"""))
      },
      test("a field a caller sets wins, so the escape hatch is not fenced off from what matters") {
        val overridden = body(extra = Json.Obj("model" -> Json.Str("other/model")))
        assertTrue(overridden.contains(""""model":"other/model""""), !overridden.contains("claude-3.5-sonnet"))
      },
    ),
  )
