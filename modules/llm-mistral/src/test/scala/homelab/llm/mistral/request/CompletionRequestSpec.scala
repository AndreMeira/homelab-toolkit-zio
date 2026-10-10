package homelab.llm.mistral.request


import homelab.llm.mistral.MistralModel
import homelab.llm.schema.{ JsonSchema, Node, Shape }
import homelab.llm.{ Advertised, Message, Model, Tool }
import zio.json.*
import zio.json.ast.Json
import zio.test.*
import zio.{ Chunk, Scope }


/** A toolkit conversation, as a Mistral request: four roles to four roles, and ids the API takes. */
object CompletionRequestSpec extends ZIOSpecDefault:
  import Support.*

  def spec: Spec[TestEnvironment & Scope, Any] = suite("CompletionRequest")(
    suite("the roles")(
      test("go out one to one, the instructions staying a message of their own") {
        val body = sent(Message.system(text("be brief")), Message.user(text("hello")))
        assertTrue(
          body.contains(
            """"messages":[{"role":"system","content":[{"type":"text","text":"be brief"}]},""" +
              """{"role":"user","content":[{"type":"text","text":"hello"}]}]"""
          )
        )
      },
      test("a tool's answer is a tool message, paired to its call by id") {
        val body = sent(Message.toolResult(id("D681PevKs"), text("12 degrees")))
        assertTrue(
          body.contains(""""role":"tool","tool_call_id":"D681PevKs","content":[{"type":"text","text":"12 degrees"}]""")
        )
      },
      test("a tool that could not answer says so in its words alone, since the API has no field for it") {
        val body = sent(Message.toolResult(id("D681PevKs"), text("no such city"), failed = true))
        assertTrue(body.contains("no such city"), !body.contains("error"), !body.contains("failed"))
      },
    ),
    suite("a call the model made")(
      test("goes back with the arguments it wrote, as the string it wrote them") {
        val body = sent(Message.assistant(Chunk.empty, Chunk(Tool.Call.Raw(id("D681PevKs"), "weather", """{"city":"Hamburg"}"""))))
        assertTrue(
          body.contains(""""tool_calls":[{"id":"D681PevKs","function":{"name":"weather","arguments":"{\"city\":\"Hamburg\"}"}""")
        )
      },
      test("under an id another provider minted, rewritten the same way in the call and in its answer") {
        val messages = conversation(
          Message.assistant(Chunk.empty, Chunk(Tool.Call.Raw(id("call_0fypS1hVXab"), "weather", "{}"))),
          Message.toolResult(id("call_0fypS1hVXab"), text("12 degrees")),
        )
        assertTrue(
          calls(messages) == answers(messages),
          calls(messages).forall(_.matches("[A-Za-z0-9]{9}")),
          !calls(messages).contains("call_0fypS1hVXab"),
        )
      },
      test("under an id Mistral minted, untouched") {
        val messages = conversation(
          Message.assistant(Chunk.empty, Chunk(Tool.Call.Raw(id("D681PevKs"), "weather", "{}"))),
          Message.toolResult(id("D681PevKs"), text("12 degrees")),
        )
        assertTrue(calls(messages) == Chunk("D681PevKs"), answers(messages) == Chunk("D681PevKs"))
      },
    ),
    suite("content")(
      test("held raw goes out exactly as it is held, which is how reasoning reaches the next turn") {
        val thinking = Json.Obj(
          "type"     -> Json.Str("thinking"),
          "thinking" -> Json.Arr(Json.Obj("type" -> Json.Str("text"), "text" -> Json.Str("weighing it up"))),
        )
        val body     = sent(Message.assistant(Chunk(Message.Content.Raw(thinking), Message.Content.Text("12 degrees"))))
        assertTrue(body.contains(s""""content":[${thinking.toJson},{"type":"text","text":"12 degrees"}]"""))
      }
    ),
    suite("tools")(
      test("are offered as functions, the schema under parameters") {
        val body = CompletionRequest
          .body(CompletionRequest.from(model, Model.Request(Chunk.empty, Chunk(weather)), MistralModel.Config()))
          .toJson
        assertTrue(
          body.contains(
            """"tools":[{"type":"function","function":{"name":"weather","parameters":{"type":"object",""" +
              """"properties":{"city":{"type":"string"}},"required":["city"],"additionalProperties":false},""" +
              """"description":"Report it."}}]"""
          )
        )
      },
      test("are absent when there are none") {
        assertTrue(!sent(Message.user(text("hi"))).contains("tools"))
      },
    ),
  )

  /** What these tests send, and readers for what the request makes of it. */
  private object Support {

    val model: Model.Name = Model.Name("mistral-medium-latest")

    val weather: Advertised = Advertised("weather", "Report it.", JsonSchema(Node.obj(Shape.Obj.Field("city", Node.text))))

    def text(value: String): Chunk[Message.Content] = Chunk(Message.Content.Text(value))

    def id(value: String): Tool.Call.Id = Tool.Call.Id(value)

    /** The messages of a conversation, as the request carries them. */
    def conversation(messages: Message*): Chunk[MessageRequest] =
      CompletionRequest.from(model, Model.Request(Chunk.fromIterable(messages)), MistralModel.Config()).messages

    /** The body a conversation is sent as. */
    def sent(messages: Message*): String =
      CompletionRequest
        .body(CompletionRequest.from(model, Model.Request(Chunk.fromIterable(messages)), MistralModel.Config()))
        .toJson

    /** The ids every call in the messages goes out under. */
    def calls(messages: Chunk[MessageRequest]): Chunk[String] =
      messages.flatMap {
        case MessageRequest.Assistant(_, Some(asked), _) => asked.map(_.id)
        case _                                           => Chunk.empty
      }

    /** The ids every tool message answers. */
    def answers(messages: Chunk[MessageRequest]): Chunk[String] =
      messages.collect { case MessageRequest.Tool(callId, _, _) => callId }
  }
