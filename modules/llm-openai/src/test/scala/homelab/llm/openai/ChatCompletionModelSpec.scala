package homelab.llm.openai


import homelab.llm.openai.error.ChatCompletionError
import homelab.llm.openai.request.CompletionRequest
import homelab.llm.openai.response.CompletionResponse
import homelab.llm.{ Message, Model }
import zio.json.*
import zio.json.ast.Json
import zio.test.*
import zio.{ Chunk, IO, Ref, Scope, UIO, ZIO }


/** What the port narrows, and what it leaves to whoever holds the client. */
object ChatCompletionModelSpec extends ZIOSpecDefault:
  import Support.*

  def spec: Spec[TestEnvironment & Scope, Any] = suite("ChatCompletionModel")(
    suite("what it narrows")(
      test("takes the first of several answers, because a completion holds one") {
        val twice = """{"choices":[{"message":{"content":"first"},"finish_reason":"stop"},
                      |{"message":{"content":"second"},"finish_reason":"stop"}]}""".stripMargin
        for
          client     <- ClientStub.answering(twice)
          completion <- ChatCompletionModel(client).complete(gpt4o, conversation)
        yield assertTrue(completion.content == text("first"))
      },
      test("asks for nothing the conversation did not imply") {
        for
          client <- ClientStub.answering(said)
          _      <- ChatCompletionModel(client).complete(gpt4o, conversation)
          asked  <- client.seen
        yield assertTrue(
          asked.exists(request => request.n.isEmpty && request.maxTokens.isEmpty && request.temperature.isEmpty),
          asked.exists(_.model == "gpt-4o"),
        )
      },
      test("what the instance always asks for, without the conversation saying so") {
        val cool = ChatCompletionModel.Config(temperature = Some(0.1), maxTokens = Some(256))
        for
          client <- ClientStub.answering(said)
          _      <- ChatCompletionModel(client, cool).complete(gpt4o, conversation)
          asked  <- client.seen
        yield assertTrue(asked.flatMap(_.temperature) == Some(0.1), asked.flatMap(_.maxTokens) == Some(256))
      },
      test("what the instance asks for beyond the protocol this adapter models") {
        val configured = ChatCompletionModel.Config(extra = Json.Obj("service_tier" -> Json.Str("flex")))
        for
          client  <- ClientStub.answering(said)
          _       <- ChatCompletionModel(client, configured).complete(gpt4o, conversation)
          carried <- client.carried
        yield assertTrue(carried == Some(Json.Obj("service_tier" -> Json.Str("flex"))))
      },
    ),
    suite("what it reads")(
      test("the words, why it stopped, and what the call cost") {
        val counted = """{"choices":[{"message":{"content":"12 degrees"},"finish_reason":"stop"}],
                        |"usage":{"prompt_tokens":11,"completion_tokens":3,"cost":0.00021}}""".stripMargin
        for
          client     <- ClientStub.answering(counted)
          completion <- ChatCompletionModel(client).complete(gpt4o, conversation)
        yield assertTrue(
          completion.content == text("12 degrees"),
          completion.finish == Model.FinishReason.Stop,
          completion.usage == Model.Usage(11, 3, Some(BigDecimal("0.00021"))),
        )
      },
      test("the calls the model asked for, arguments still unparsed") {
        val called = """{"choices":[{"message":{"tool_calls":[{"id":"c1","type":"function",
                       |"function":{"name":"weather","arguments":"{\"city\":\"Hamburg\"}"}}]},
                       |"finish_reason":"tool_calls"}]}""".stripMargin
        for
          client     <- ClientStub.answering(called)
          completion <- ChatCompletionModel(client).complete(gpt4o, conversation)
        yield assertTrue(
          completion.calls.map(_.name) == Chunk("weather"),
          completion.calls.map(_.arguments) == Chunk("""{"city":"Hamburg"}"""),
          completion.finish == Model.FinishReason.ToolCalls,
        )
      },
      test("a reason the toolkit does not name is kept as the provider spelled it") {
        for
          client     <- ClientStub.answering("""{"choices":[{"message":{"content":"x"},"finish_reason":"error"}]}""")
          completion <- ChatCompletionModel(client).complete(gpt4o, conversation)
        yield assertTrue(completion.finish == Model.FinishReason.Other("error"))
      },
    ),
    suite("what it refuses")(
      test("a body with no choices has not answered") {
        for
          client  <- ClientStub.answering("""{"choices":[]}""")
          failure <- ChatCompletionModel(client).complete(gpt4o, conversation).flip
        yield assertTrue(failure == ChatCompletionError.Malformed("the response carried no choices"))
      }
    ),
  )

  /** What these tests ask for, what the provider answers, and the client that stands in for it. */
  private object Support {

    /** The model every test names. */
    val gpt4o = Model.Name("gpt-4o")

    /** A one-turn conversation, enough to build a request from. */
    val conversation = Model.Request(Chunk(Message.user(text("what is the weather?"))))

    /** A plain answer, for a test that cares what was asked rather than what came back. */
    val said = """{"choices":[{"message":{"content":"ok"},"finish_reason":"stop"}]}"""

    /** One text part, the shape a turn's content takes. */
    def text(value: String): Chunk[Message.Content] = Chunk(Message.Content.Text(value))

    /** A client that answers from a fixture, and holds what it was asked for. */
    final class ClientStub(answer: CompletionResponse, recorded: Ref[Option[(CompletionRequest, Json.Obj)]]) extends ChatCompletionClient:
      def seen: UIO[Option[CompletionRequest]] = recorded.get.map(_.map((request, _) => request))
      def carried: UIO[Option[Json.Obj]]       = recorded.get.map(_.map((_, extra) => extra))

      override def complete(
        request: CompletionRequest,
        extra: Json.Obj = Json.Obj(),
      ): IO[ChatCompletionError, CompletionResponse] =
        recorded.set(Some(request -> extra)).as(answer)

    object ClientStub:

      /** Builds a client answering with `body`; aborts with a reason when the fixture cannot be read. */
      def answering(body: String): IO[String, ClientStub] =
        for
          answer   <- ZIO.fromEither(body.fromJson[CompletionResponse]).mapError(reason => s"unreadable fixture: $reason, in $body")
          recorded <- Ref.make(Option.empty[(CompletionRequest, Json.Obj)])
        yield ClientStub(answer, recorded)
  }
