package homelab.llm.openai


import homelab.llm.{ Message, Model }
import sttp.client4.impl.zio.RIOMonadAsyncError
import sttp.client4.testing.BackendStub
import sttp.client4.UriContext
import sttp.model.StatusCode
import zio.test.*
import zio.{ Chunk, Scope, Task }


/** What the adapter makes of what a provider answers, without a network. */
object ChatCompletionModelSpec extends ZIOSpecDefault:

  private val answered =
    """{"choices":[{"message":{"content":"12 degrees"},"finish_reason":"stop"},{"message":{"content":"ignored"}}],
      |"usage":{"prompt_tokens":11,"completion_tokens":3,"cost":0.00021}}""".stripMargin

  private val asked =
    """{"choices":[{"message":{"tool_calls":[{"id":"c1","type":"function",
      |"function":{"name":"weather","arguments":"{\"city\":\"Hamburg\"}"}}]},
      |"finish_reason":"tool_calls"}]}""".stripMargin

  private def answering(body: String, status: StatusCode = StatusCode.Ok): ChatCompletionModel =
    val backend = BackendStub[Task](RIOMonadAsyncError[Any]).whenAnyRequest.thenRespondAdjust(body, status)
    ChatCompletionModel.openRouter(backend, "test-key")

  /** A stub that answers nothing, and records the one request it was handed. */
  private def recording(seen: zio.Ref[Option[sttp.client4.GenericRequest[?, ?]]]) =
    BackendStub[Task](RIOMonadAsyncError[Any]).whenAnyRequest.thenRespondF { request =>
      seen.set(Some(request)).as(sttp.client4.testing.ResponseStub.adjust("""{"choices":[]}"""))
    }

  private def ask(model: ChatCompletionModel) =
    model.complete(Model.Name("anthropic/claude-3.5-sonnet"), Model.Request(Chunk(Message.user(Chunk.empty))))

  def spec: Spec[TestEnvironment & Scope, Any] = suite("ChatCompletionModel")(
    suite("what it reads")(
      test("takes the first choice, its reason, and what the call cost") {
        for completion <- ask(answering(answered))
        yield assertTrue(
          completion.content == Chunk(Message.Content.Text("12 degrees")),
          completion.finish == Model.FinishReason.Stop,
          completion.usage == Model.Usage(11, 3, Some(BigDecimal("0.00021"))),
        )
      },
      test("reads the calls a model asked for, arguments still unparsed") {
        for completion <- ask(answering(asked))
        yield assertTrue(
          completion.calls.map(_.name) == Chunk("weather"),
          completion.calls.map(_.arguments) == Chunk("""{"city":"Hamburg"}"""),
          completion.finish == Model.FinishReason.ToolCalls,
        )
      },
      test("keeps a reason the toolkit does not name rather than choosing one") {
        val body = """{"choices":[{"message":{"content":"x"},"finish_reason":"error"}]}"""
        for completion <- ask(answering(body))
        yield assertTrue(completion.finish == Model.FinishReason.Other("error"))
      },
      test("a gateway that reports no usage gives no cost, which a budget reads as unknown") {
        val body = """{"choices":[{"message":{"content":"x"},"finish_reason":"stop"}]}"""
        for completion <- ask(answering(body))
        yield assertTrue(completion.usage == Model.Usage(0, 0, None))
      },
    ),
    suite("presets")(
      test("openRouter posts to openrouter with a bearer token, and attributes when asked") {
        for
          seen <- zio.Ref.make(Option.empty[sttp.client4.GenericRequest[?, ?]])
          _    <- ask(ChatCompletionModel.openRouter(recording(seen), "k", Some("https://example.test"))).flip
          sent <- seen.get
        yield assertTrue(
          sent.exists(_.uri == ChatCompletionModel.OpenRouter),
          sent.exists(_.headers.exists(header => header.name == "Authorization" && header.value == "Bearer k")),
          sent.exists(_.headers.exists(_.name == "HTTP-Referer")),
        )
      },
      test("openRouter sends no attribution when none was asked for") {
        for
          seen <- zio.Ref.make(Option.empty[sttp.client4.GenericRequest[?, ?]])
          _    <- ask(ChatCompletionModel.openRouter(recording(seen), "k")).flip
          sent <- seen.get
        yield assertTrue(sent.exists(request => !request.headers.exists(_.name == "HTTP-Referer")))
      },
      test("openAi posts to openai, and names an organisation when given one") {
        for
          seen <- zio.Ref.make(Option.empty[sttp.client4.GenericRequest[?, ?]])
          _    <- ask(ChatCompletionModel.openAi(recording(seen), "k", Some("org-1"))).flip
          sent <- seen.get
        yield assertTrue(
          sent.exists(_.uri == ChatCompletionModel.OpenAi),
          sent.exists(_.headers.exists(header => header.name == "OpenAI-Organization" && header.value == "org-1")),
        )
      },
      test("a compatible server needs no credential at all") {
        val local = uri"http://localhost:11434/v1/chat/completions"
        for
          seen <- zio.Ref.make(Option.empty[sttp.client4.GenericRequest[?, ?]])
          _    <- ask(ChatCompletionModel.compatible(recording(seen), local)).flip
          sent <- seen.get
        yield assertTrue(
          sent.exists(_.uri == local),
          sent.exists(request => !request.headers.exists(_.name == "Authorization")),
        )
      },
    ),
    suite("what it refuses")(
      test("a body with no choices has not answered") {
        for failure <- ask(answering("""{"choices":[]}""")).flip
        yield assertTrue(failure == ChatCompletionError.Malformed("the response carried no choices"))
      },
      test("a body that is not a completion says what could not be read, and out of what") {
        for failure <- ask(answering("""{"unexpected":true}""")).flip
        yield assertTrue(failure.isInstanceOf[ChatCompletionError.Malformed], failure.message.contains("unexpected"))
      },
      test("a refused credential is not worth retrying") {
        val body = """{"error":{"message":"No auth credentials found"}}"""
        for failure <- ask(answering(body, StatusCode.Unauthorized)).flip
        yield assertTrue(
          failure == ChatCompletionError.Refused("No auth credentials found"),
          !failure.isInstanceOf[homelab.common.error.ApplicationError.TransientError],
        )
      },
      test("a rate limit and a server error are") {
        for
          limited <- ask(answering("""{"error":{"message":"rate limited"}}""", StatusCode.TooManyRequests)).flip
          broken  <- ask(answering("""{"error":{"message":"upstream"}}""", StatusCode.BadGateway)).flip
        yield assertTrue(
          limited.isInstanceOf[homelab.common.error.ApplicationError.TransientError],
          broken.isInstanceOf[homelab.common.error.ApplicationError.TransientError],
        )
      },
      test("anything else is the request itself, and says what the gateway called it") {
        val body = """{"error":{"message":"model not found"}}"""
        for failure <- ask(answering(body, StatusCode.NotFound)).flip
        yield assertTrue(failure == ChatCompletionError.Rejected(404, "model not found"))
      },
      test("a refusal that is not an error object keeps what it said anyway") {
        for failure <- ask(answering("upstream timeout", StatusCode.BadRequest)).flip
        yield assertTrue(failure == ChatCompletionError.Rejected(400, "upstream timeout"))
      },
    ),
  )
