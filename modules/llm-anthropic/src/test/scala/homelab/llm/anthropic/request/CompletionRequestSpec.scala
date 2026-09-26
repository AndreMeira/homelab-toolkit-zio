package homelab.llm.anthropic.request


import homelab.llm.schema.{ JsonSchema, Node, Shape }
import homelab.llm.{ Advertised, Message, Model, Tool }
import zio.json.*
import zio.json.ast.Json
import zio.test.*
import zio.{ Chunk, Scope }


/** A conversation reshaped for an API that has two roles where the toolkit has four. */
object CompletionRequestSpec extends ZIOSpecDefault:

  private val model = Model.Name("claude-3-5-sonnet-latest")

  private def text(value: String): Chunk[Message.Content] = Chunk(Message.Content.Text(value))

  private def id(value: String): Tool.Call.Id = Tool.Call.Id(value)

  private val weather = Advertised("weather", "Report it.", JsonSchema(Node.obj(Shape.Obj.Field("city", Node.text))))

  private val ceiling = 4096

  private def sent(messages: Message*): String =
    val (system, turns) = MessageRequest.conversation(Chunk.fromIterable(messages))
    body(CompletionRequest(model, ceiling, turns, system = system))

  private def body(request: CompletionRequest): String = CompletionRequest.body(request).toJson

  def spec: Spec[TestEnvironment & Scope, Any] = suite("CompletionRequest")(
    suite("the instructions")(
      test("are lifted out of the conversation into a field of their own") {
        val body = sent(Message.system(text("be brief")), Message.user(text("hello")))
        assertTrue(body.contains(""""system":"be brief""""), !body.contains(""""role":"system""""))
      },
      test("run together when a conversation was told more than once") {
        val body = sent(Message.system(text("be brief")), Message.system(text("be kind")))
        assertTrue(body.contains(""""system":"be brief\nbe kind""""))
      },
      test("are absent when a conversation was told nothing") {
        assertTrue(!sent(Message.user(text("hello"))).contains("system"))
      },
    ),
    suite("a tool's answer")(
      test("becomes a block inside a user turn, since this API has no tool role") {
        val body = sent(Message.toolResult(id("c1"), text("""{"degrees":12.0}""")))
        assertTrue(
          body.contains(""""role":"user""""),
          body.contains(""""type":"tool_result","tool_use_id":"c1","content":"{\"degrees\":12.0}""""),
          !body.contains(""""role":"tool""""),
        )
      },
      test("several in a row become one turn, which is what an alternating conversation needs") {
        val body = sent(Message.toolResult(id("c1"), text("one")), Message.toolResult(id("c2"), text("two")))
        assertTrue(body.split(""""role":"user"""").length == 2, body.contains("c1"), body.contains("c2"))
      },
      test("that could not answer is marked on the block, which this API has a field for") {
        val body = sent(Message.toolResult(id("c1"), text("no such city"), failed = true))
        assertTrue(
          body.contains(""""type":"tool_result","tool_use_id":"c1","content":"no such city","is_error":true""")
        )
      },
      test("that answered carries no flag at all, which the API reads as false") {
        assertTrue(!sent(Message.toolResult(id("c1"), text("done"))).contains("is_error"))
      },
      test("one after a model turn opens a user turn of its own") {
        val asked = Message.assistant(Chunk.empty, Chunk(Tool.Call.Raw(id("c1"), "weather", "{}")))
        val body  = sent(Message.user(text("hi")), asked, Message.toolResult(id("c1"), text("done")))
        assertTrue(body.split(""""role":"user"""").length == 3)
      },
    ),
    suite("a call the model made")(
      test("goes back as a block whose input is an object, not the string the toolkit holds") {
        val asked = Message.assistant(Chunk.empty, Chunk(Tool.Call.Raw(id("c1"), "weather", """{"city":"Hamburg"}""")))
        assertTrue(
          sent(asked).contains(
            """{"type":"tool_use","id":"c1","name":"weather","input":{"city":"Hamburg"}}"""
          )
        )
      },
      test("keeps what a model wrote as text when it will not parse as an object") {
        val asked = Message.assistant(Chunk.empty, Chunk(Tool.Call.Raw(id("c1"), "weather", "not json")))
        assertTrue(sent(asked).contains(""""input":"not json""""))
      },
    ),
    suite("tools")(
      test("are advertised flat, with the schema under input_schema") {
        val sent = body(CompletionRequest(model, ceiling, Nil, tools = Some(List(ToolRequest.from(weather)))))
        assertTrue(
          sent.contains(
            """"tools":[{"name":"weather","description":"Report it.","input_schema":{"type":"object",""" +
              """"properties":{"city":{"type":"string"}},"required":["city"],"additionalProperties":false}}]"""
          ),
          !sent.contains(""""type":"function""""),
        )
      },
      test("are absent when there are none") {
        assertTrue(!sent(Message.user(text("hi"))).contains("tools"))
      },
    ),
    suite("max_tokens")(
      test("is always sent, because the API will not do without") {
        assertTrue(sent(Message.user(text("hi"))).contains(s""""max_tokens":$ceiling"""))
      },
      test("is a caller's to replace through what the call merges over the request") {
        val overridden =
          CompletionRequest.body(CompletionRequest(model, ceiling, Nil), Json.Obj("max_tokens" -> Json.Num(64))).toJson
        assertTrue(overridden.contains(""""max_tokens":64"""), !overridden.contains(s""""max_tokens":$ceiling"""))
      },
    ),
  )
