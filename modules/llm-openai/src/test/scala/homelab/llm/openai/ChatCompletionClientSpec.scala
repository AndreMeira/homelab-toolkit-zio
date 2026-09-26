package homelab.llm.openai


import homelab.llm.Model
import homelab.llm.openai.error.ChatCompletionError
import homelab.llm.openai.request.{ CompletionRequest, MessageRequest, ResponseFormat, ToolChoice }
import homelab.llm.schema.{ JsonSchema, Node, Shape }
import sttp.client4.UriContext
import sttp.client4.impl.zio.RIOMonadAsyncError
import sttp.client4.testing.BackendStub
import sttp.model.StatusCode
import zio.test.*
import zio.{ Ref, Scope, Task, ZIO }


/** The call itself: what goes out, and everything a provider's answer carries back — without a network. */
object ChatCompletionClientSpec extends ZIOSpecDefault:

  private val answered =
    """{"choices":[{"message":{"content":"12 degrees"},"finish_reason":"stop"},
      |{"message":{"content":"about twelve"},"finish_reason":"stop"}],
      |"usage":{"prompt_tokens":11,"completion_tokens":3,"cost":0.00021}}""".stripMargin

  private def answering(body: String, status: StatusCode = StatusCode.Ok): ChatCompletionClient =
    val backend = BackendStub[Task](RIOMonadAsyncError[Any]).whenAnyRequest.thenRespondAdjust(body, status)
    ChatCompletionClient.openRouter(backend, "test-key", None)

  /** A stub that answers nothing, and records the one request it was handed. */
  private def recording(seen: Ref[Option[sttp.client4.GenericRequest[?, ?]]]) =
    BackendStub[Task](RIOMonadAsyncError[Any]).whenAnyRequest.thenRespondF { request =>
      seen.set(Some(request)).as(sttp.client4.testing.ResponseStub.adjust("""{"choices":[]}"""))
    }

  private val asked = CompletionRequest(Model.Name("anthropic/claude-3.5-sonnet"), List(MessageRequest.User(Nil)))

  private def ask(client: ChatCompletionClient) = client.complete(asked)

  /** The body one request renders to, as the provider would receive it. */
  private def posted(request: CompletionRequest, extra: zio.json.ast.Json.Obj = zio.json.ast.Json.Obj()) =
    for
      seen <- Ref.make(Option.empty[sttp.client4.GenericRequest[?, ?]])
      _    <- ChatCompletionClient.openRouter(recording(seen), "k", None).complete(request, extra).ignore
      body <- seen.get.map(_.map(_.body.show))
    yield body

  private def sent(build: sttp.client4.Backend[Task] => ChatCompletionClient) =
    for
      seen <- Ref.make(Option.empty[sttp.client4.GenericRequest[?, ?]])
      _    <- ask(build(recording(seen))).ignore
      sent <- seen.get
    yield sent

  def spec: Spec[TestEnvironment & Scope, Any] = suite("ChatCompletionClient")(
    suite("what it answers with")(
      test("every choice, not only the one a port would take") {
        for response <- ask(answering(answered))
        yield assertTrue(
          response.choices.size == 2,
          response.choices.map(_.message.content) == List(Some("12 degrees"), Some("about twelve")),
        )
      },
      test("the usage as the provider reported it, cost included") {
        for response <- ask(answering(answered))
        yield assertTrue(response.usage.map(_.cost) == Some(Some(BigDecimal("0.00021"))))
      },
      test("a call's arguments, still the string the model wrote") {
        val body = """{"choices":[{"message":{"tool_calls":[{"id":"c1","type":"function",
                     |"function":{"name":"weather","arguments":"{\"city\":\"Hamburg\"}"}}]},
                     |"finish_reason":"tool_calls"}]}""".stripMargin
        for response <- ask(answering(body))
        yield assertTrue(
          response.choices.head.message.toolCalls.map(_.map(_.function.arguments)) ==
            Some(List("""{"city":"Hamburg"}"""))
        )
      },
    ),
    suite("what it sends")(
      test("a request's own fields reach the body, each under the name the protocol uses") {
        val rich = asked.copy(
          n = Some(3),
          maxTokens = Some(64),
          toolChoice = Some(ToolChoice.Named("weather")),
          parallelToolCalls = Some(false),
        )
        for body <- posted(rich)
        yield assertTrue(
          body.exists(_.contains(""""n":3""")),
          body.exists(_.contains(""""max_tokens":64""")),
          body.exists(_.contains(""""tool_choice":{"type":"function","function":{"name":"weather"}}""")),
          body.exists(_.contains(""""parallel_tool_calls":false""")),
        )
      },
      test("a tool choice that is a bare word is sent as one, not as an object") {
        for body <- posted(asked.copy(toolChoice = Some(ToolChoice.Required)))
        yield assertTrue(body.exists(_.contains(""""tool_choice":"required"""")))
      },
      test("a response format carries the schema the answer must conform to") {
        val schema = JsonSchema(Node.obj(Shape.Obj.Field("city", Node.text)))
        for body <- posted(asked.copy(responseFormat = Some(ResponseFormat.conforming("place", schema))))
        yield assertTrue(
          body.exists(_.contains(""""response_format":{"type":"json_schema","json_schema":{"name":"place"""")),
          body.exists(_.contains(""""strict":true""")),
        )
      },
      test("what a caller adds is merged over the request, for a protocol that has moved") {
        for body <- posted(asked, zio.json.ast.Json.Obj("service_tier" -> zio.json.ast.Json.Str("flex")))
        yield assertTrue(body.exists(_.contains(""""service_tier":"flex"""")))
      },
    ),
    suite("presets")(
      test("openRouter posts to openrouter with a bearer token, and attributes when asked") {
        for request <- sent(ChatCompletionClient.openRouter(_, "k", Some("https://example.test")))
        yield assertTrue(
          request.exists(_.uri == ChatCompletionClient.OpenRouter),
          request.exists(_.headers.exists(header => header.name == "Authorization" && header.value == "Bearer k")),
          request.exists(_.headers.exists(_.name == "HTTP-Referer")),
        )
      },
      test("openRouter sends no attribution when none was asked for") {
        for request <- sent(ChatCompletionClient.openRouter(_, "k", None))
        yield assertTrue(request.exists(sent => !sent.headers.exists(_.name == "HTTP-Referer")))
      },
      test("openAi posts to openai, and names an organisation when given one") {
        for request <- sent(ChatCompletionClient.openAi(_, "k", Some("org-1")))
        yield assertTrue(
          request.exists(_.uri == ChatCompletionClient.OpenAi),
          request.exists(_.headers.exists(header => header.name == "OpenAI-Organization" && header.value == "org-1")),
        )
      },
      test("a compatible server needs no credential at all") {
        val local = uri"http://localhost:11434/v1/chat/completions"
        for request <- sent(ChatCompletionClient.compatible(_, local, None))
        yield assertTrue(
          request.exists(_.uri == local),
          request.exists(sent => !sent.headers.exists(_.name == "Authorization")),
        )
      },
    ),
    suite("a transport of its own")(
      test("a caller with no opinion gets one, and it is closed with the scope") {
        ZIO.scoped(ChatCompletionClient.openAi("k")).map(client => assertTrue(client.isInstanceOf[ChatCompletionClient]))
      }
    ),
    suite("what it refuses")(
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
      test("anything else is the request itself, and says what the provider called it") {
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
