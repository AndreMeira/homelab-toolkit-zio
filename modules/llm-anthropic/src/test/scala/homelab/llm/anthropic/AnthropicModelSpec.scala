package homelab.llm.anthropic


import homelab.llm.Model.Name
import homelab.llm.anthropic.error.AnthropicError
import homelab.llm.anthropic.request.{ CompletionRequest, Thinking }
import homelab.llm.anthropic.response.CompletionResponse
import homelab.llm.{ Message, Model }
import zio.json.*
import zio.json.ast.Json
import zio.test.*
import zio.{ Chunk, IO, Ref, Scope, UIO, ZIO }


/** What the port narrows, and what it leaves to whoever holds the client. */
object AnthropicModelSpec extends ZIOSpecDefault:
  import Support.*

  def spec: Spec[TestEnvironment & Scope, Any] = suite("AnthropicModel")(
    suite("what it settles")(
      test("a ceiling the port has no field for, since the API will not do without one") {
        for
          client <- ClientStub.answering(said)
          _      <- AnthropicModel(client).complete(sonnet, conversation)
          asked  <- client.seen
        yield assertTrue(asked.map(_.maxTokens).contains(AnthropicModel.DefaultMaxTokens))
      },
      test("the one an instance was built with instead") {
        for
          client <- ClientStub.answering(said)
          config  = AnthropicModel.Config(maxTokens = 64)
          _      <- AnthropicModel(client, config).complete(sonnet, conversation)
          asked  <- client.seen
        yield assertTrue(asked.map(_.maxTokens).contains(64))
      },
      test("asks for nothing else the conversation did not imply") {
        for
          client <- ClientStub.answering(said)
          _      <- AnthropicModel(client).complete(sonnet, conversation)
          asked  <- client.seen
        yield assertTrue(
          asked.exists(request => request.temperature.isEmpty && request.toolChoice.isEmpty),
          asked.exists(_.thinking.isEmpty),
        )
      },
      test("what the instance always asks for, without the conversation saying so") {
        for
          client <- ClientStub.answering(said)
          warm    = AnthropicModel.Config(temperature = Some(0.9), thinking = Some(Thinking.Enabled(512)))
          _      <- AnthropicModel(client, warm).complete(sonnet, conversation)
          asked  <- client.seen
        yield assertTrue(asked.flatMap(_.temperature).contains(0.9), asked.exists(_.thinking.isDefined))
      },
      test("what the instance asks for beyond the API this adapter models") {
        for
          client  <- ClientStub.answering(said)
          config   = AnthropicModel.Config(extra = Json.Obj("service_tier" -> Json.Str("standard")))
          _       <- AnthropicModel(client, config).complete(sonnet, conversation)
          carried <- client.carried
        yield assertTrue(carried.contains(Json.Obj("service_tier" -> Json.Str("standard"))))
      },
    ),
    suite("what it reshapes")(
      test("the instructions out of the conversation and into a field of their own") {
        for
          client <- ClientStub.answering(said)
          told    = Model.Request(Chunk(Message.system(text("be brief")), Message.user(text("hi"))))
          _      <- AnthropicModel(client).complete(sonnet, told)
          asked  <- client.seen
        yield assertTrue(
          asked.flatMap(_.system).contains("be brief"),
          asked.exists(_.messages.map(_.role) == Chunk("user")),
        )
      }
    ),
    suite("what it reads")(
      test("the words, why it stopped, and what it consumed — with no cost, which this API does not report") {
        for
          client     <- ClientStub.answering(counted)
          completion <- AnthropicModel(client).complete(sonnet, conversation)
        yield assertTrue(
          completion.content == text("12 degrees"),
          completion.finish == Model.FinishReason.Stop,
          completion.usage == Model.Usage(11, 3, None),
        )
      },
      test("a call, whose arguments come back as the string the toolkit carries") {
        for
          client     <- ClientStub.answering(called)
          completion <- AnthropicModel(client).complete(sonnet, conversation)
        yield assertTrue(
          completion.calls.map(_.arguments) == Chunk("""{"city":"Hamburg"}"""),
          completion.finish == Model.FinishReason.ToolCalls,
          completion.content.isEmpty,
        )
      },
      test("this API's names for stopping are translated to the toolkit's") {
        for
          client     <- ClientStub.answering("""{"content":[],"stop_reason":"max_tokens"}""")
          completion <- AnthropicModel(client).complete(sonnet, conversation)
        yield assertTrue(completion.finish == Model.FinishReason.Length)
      },
    ),
    suite("what it refuses")(
      test("a body with neither content nor a reason has not answered") {
        for
          client  <- ClientStub.answering("""{"content":[]}""")
          failure <- AnthropicModel(client).complete(sonnet, conversation).flip
        yield assertTrue(failure.isInstanceOf[AnthropicError.Malformed])
      }
    ),
  )

  /** What these tests ask for, what the API answers, and the client that stands in for it. */
  private object Support {

    /** The model every test names. */
    val sonnet: Name = Model.Name("claude-3-5-sonnet-latest")

    /** A one-turn conversation, enough to build a request from. */
    val conversation = Model.Request(Chunk(Message.user(text("what is the weather?"))))

    /** A plain answer, for a test that cares what was asked rather than what came back. */
    val said = """{"content":[{"type":"text","text":"12 degrees"}],"stop_reason":"end_turn"}"""

    /** An answer that reports what the call consumed. */
    val counted: String =
      """{"content":[{"type":"text","text":"12 degrees"}],"stop_reason":"end_turn",
        |"usage":{"input_tokens":11,"output_tokens":3}}""".stripMargin

    /** An answer that asks for a tool rather than saying anything. */
    val called: String =
      """{"content":[{"type":"tool_use","id":"c1","name":"weather","input":{"city":"Hamburg"}}],
        |"stop_reason":"tool_use"}""".stripMargin

    /** One text part, the shape a turn's content takes. */
    def text(value: String): Chunk[Message.Content] = Chunk(Message.Content.Text(value))

    /** A client that answers from a fixture, and holds what it was asked for. */
    final class ClientStub(answer: CompletionResponse, recorded: Ref[Option[(CompletionRequest, Json.Obj)]]) extends AnthropicClient:
      def seen: UIO[Option[CompletionRequest]] = recorded.get.map(_.map((request, _) => request))
      def carried: UIO[Option[Json.Obj]]       = recorded.get.map(_.map((_, extra) => extra))

      override def complete(request: CompletionRequest, extra: Json.Obj = Json.Obj()): IO[AnthropicError, CompletionResponse] =
        recorded.set(Some(request -> extra)).as(answer)

    object ClientStub:

      /** Builds a client answering with `body`; aborts with a reason when the fixture cannot be read. */
      def answering(body: String): IO[String, ClientStub] =
        for
          decoded   = body.fromJson[CompletionResponse]
          answer   <- ZIO.fromEither(decoded).mapError(reason => s"unreadable fixture: $reason, in $body")
          recorded <- Ref.make(Option.empty[(CompletionRequest, Json.Obj)])
        yield ClientStub(answer, recorded)
  }
