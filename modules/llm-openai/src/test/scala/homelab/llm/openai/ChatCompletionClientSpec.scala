package homelab.llm.openai


import homelab.common.error.ApplicationError
import homelab.common.monitor.Monitor
import homelab.llm.Model
import homelab.llm.openai.error.ChatCompletionError
import homelab.llm.openai.request.{ CompletionRequest, MessageRequest, ResponseFormat, ToolChoice }
import homelab.llm.schema.{ JsonSchema, Node, Shape }
import sttp.client4.impl.zio.RIOMonadAsyncError
import sttp.client4.testing.{ BackendStub, ResponseStub }
import sttp.client4.{ GenericRequest, UriContext }
import sttp.model.StatusCode
import zio.test.*
import zio.json.ast.Json
import zio.{ Chunk, Ref, Scope, Task, UIO, ZIO }


/** The call itself: what goes out, and everything a provider's answer carries back — without a network. */
object ChatCompletionClientSpec extends ZIOSpecDefault:
  import Support.*

  def spec: Spec[TestEnvironment & Scope, Any] = suite("ChatCompletionClient")(
    suite("what it answers with")(
      test("every choice, not only the one a port would take") {
        for
          backend   = HttpClient.stub(response = answered)
          response <- ChatCompletionClient.openRouter(backend, "api-key", None, Monitor.Noop).complete(asked)
        yield assertTrue(
          response.choices.size == 2,
          response.choices.map(_.message.content) == Chunk(Some("12 degrees"), Some("about twelve")),
        )
      },
      test("the usage as the provider reported it, cost included") {
        for
          backend   = HttpClient.stub(response = answered)
          response <- ChatCompletionClient.openRouter(backend, "api-key", None, Monitor.Noop).complete(asked)
        yield assertTrue(response.usage.map(_.cost) == Some(Some(BigDecimal("0.00021"))))
      },
      test("a call's arguments, still the string the model wrote") {
        for
          backend   = HttpClient.stub(response = called)
          response <- ChatCompletionClient.openRouter(backend, "api-key", None, Monitor.Noop).complete(asked)
        yield assertTrue(
          response.choices.headOption.flatMap(_.message.toolCalls).map(_.map(_.function.arguments)) ==
            Some(Chunk("""{"city":"Hamburg"}"""))
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
        for
          recorder <- Recorder.make
          backend   = recorder.httpClient(response = empty)
          _        <- ChatCompletionClient.openRouter(backend, "api-key", None, Monitor.Noop).complete(rich).ignore
          body     <- recorder.requestBody
        yield assertTrue(
          body.exists(_.contains(""""n":3""")),
          body.exists(_.contains(""""max_tokens":64""")),
          body.exists(_.contains(""""tool_choice":{"type":"function","function":{"name":"weather"}}""")),
          body.exists(_.contains(""""parallel_tool_calls":false""")),
        )
      },
      test("a tool choice that is a bare word is sent as one, not as an object") {
        for
          recorder <- Recorder.make
          backend   = recorder.httpClient(response = empty)
          request   = asked.copy(toolChoice = Some(ToolChoice.Required))
          _        <- ChatCompletionClient.openRouter(backend, "api-key", None, Monitor.Noop).complete(request).ignore
          body     <- recorder.requestBody
        yield assertTrue(body.exists(_.contains(""""tool_choice":"required"""")))
      },
      test("a response format carries the schema the answer must conform to") {
        val schema = JsonSchema(Node.obj(Shape.Obj.Field("city", Node.text)))
        for
          recorder <- Recorder.make
          backend   = recorder.httpClient(response = empty)
          request   = asked.copy(responseFormat = Some(ResponseFormat.conforming("place", schema)))
          _        <- ChatCompletionClient.openRouter(backend, "api-key", None, Monitor.Noop).complete(request).ignore
          body     <- recorder.requestBody
        yield assertTrue(
          body.exists(_.contains(""""response_format":{"type":"json_schema","json_schema":{"name":"place"""")),
          body.exists(_.contains(""""strict":true""")),
        )
      },
      test("what a caller adds is merged over the request, for a protocol that has moved") {
        for
          recorder <- Recorder.make
          backend   = recorder.httpClient(response = empty)
          _        <- ChatCompletionClient
                        .openRouter(backend, "api-key", None, Monitor.Noop)
                        .complete(asked, Json.Obj("service_tier" -> Json.Str("flex")))
                        .ignore
          body     <- recorder.requestBody
        yield assertTrue(body.exists(_.contains(""""service_tier":"flex"""")))
      },
    ),
    suite("what it reports")(
      test("one measured call, named for the client and tagged with the model it asked for") {
        for
          backend  = HttpClient.stub(response = answered)
          monitor <- Monitoring.make
          _       <- ChatCompletionClient.openRouter(backend, "api-key", None, monitor).complete(asked)
          names   <- monitor.callNames
          tags    <- monitor.callTags.map(_.headOption)
        yield assertTrue(
          names == Chunk("ChatCompletionClient.complete"),
          tags.contains(Map("resource" -> "llm", "model" -> "anthropic/claude-3.5-sonnet")),
        )
      },
      test("a refused call is measured too, since a failure is what a dashboard is for") {
        for
          backend  = HttpClient.stub(response = """{"error":{"message":"nope"}}""", StatusCode.Unauthorized)
          monitor <- Monitoring.make
          _       <- ChatCompletionClient.openRouter(backend, "api-key", None, monitor).complete(asked).flip
          seen    <- monitor.seen
        yield assertTrue(seen.size == 1)
      },
    ),
    suite("presets")(
      test("openRouter posts to openrouter with a bearer token, and attributes when asked") {
        for
          recorder <- Recorder.make
          backend   = recorder.httpClient(response = empty)
          client    = ChatCompletionClient.openRouter(backend, "api-key", Some("https://example.test"), Monitor.Noop)
          _        <- client.complete(asked).ignore
          request  <- recorder.seen
        yield assertTrue(
          request.exists(_.uri == ChatCompletionClient.OpenRouter),
          request.exists(_.headers.exists(header => header.name == "Authorization" && header.value == "Bearer api-key")),
          request.exists(_.headers.exists(_.name == "HTTP-Referer")),
        )
      },
      test("openRouter sends no attribution when none was asked for") {
        for
          recorder <- Recorder.make
          backend   = recorder.httpClient(response = empty)
          _        <- ChatCompletionClient.openRouter(backend, "api-key", None, Monitor.Noop).complete(asked).ignore
          request  <- recorder.seen
        yield assertTrue(request.exists(sent => !sent.headers.exists(_.name == "HTTP-Referer")))
      },
      test("openAi posts to openai, and names an organisation when given one") {
        for
          recorder <- Recorder.make
          backend   = recorder.httpClient(response = empty)
          _        <- ChatCompletionClient.openAi(backend, "api-key", Some("org-1"), Monitor.Noop).complete(asked).ignore
          request  <- recorder.seen
        yield assertTrue(
          request.exists(_.uri == ChatCompletionClient.OpenAi),
          request.exists(_.headers.exists(header => header.name == "OpenAI-Organization" && header.value == "org-1")),
        )
      },
      test("a compatible server needs no credential at all") {
        val local = uri"http://localhost:11434/v1/chat/completions"
        for
          recorder <- Recorder.make
          backend   = recorder.httpClient(response = empty)
          _        <- ChatCompletionClient.compatible(backend, local, None, Monitor.Noop).complete(asked).ignore
          request  <- recorder.seen
        yield assertTrue(
          request.exists(_.uri == local),
          request.exists(sent => !sent.headers.exists(_.name == "Authorization")),
        )
      },
    ),
    suite("a transport of its own")(
      test("a caller with no opinion gets one, and it is closed with the scope") {
        ZIO.scoped(ChatCompletionClient.openAi("api-key")).map(client => assertTrue(client.isInstanceOf[ChatCompletionClient]))
      }
    ),
    suite("what it refuses")(
      test("a body that is not a completion says what could not be read, and out of what") {
        for
          backend  = HttpClient.stub(response = """{"unexpected":true}""")
          failure <- ChatCompletionClient.openRouter(backend, "api-key", None, Monitor.Noop).complete(asked).flip
        yield assertTrue(failure.isInstanceOf[ChatCompletionError.Malformed], failure.message.contains("unexpected"))
      },
      test("a refused credential is not worth retrying") {
        for
          backend  = HttpClient.stub(response = """{"error":{"message":"No auth credentials found"}}""", StatusCode.Unauthorized)
          failure <- ChatCompletionClient.openRouter(backend, "api-key", None, Monitor.Noop).complete(asked).flip
        yield assertTrue(
          failure == ChatCompletionError.Refused("No auth credentials found"),
          !failure.isInstanceOf[ApplicationError.TransientError],
        )
      },
      test("a rate limit and a server error are") {
        for
          limiting = HttpClient.stub(response = """{"error":{"message":"rate limited"}}""", StatusCode.TooManyRequests)
          failing  = HttpClient.stub(response = """{"error":{"message":"upstream"}}""", StatusCode.BadGateway)
          limited <- ChatCompletionClient.openRouter(limiting, "api-key", None, Monitor.Noop).complete(asked).flip
          broken  <- ChatCompletionClient.openRouter(failing, "api-key", None, Monitor.Noop).complete(asked).flip
        yield assertTrue(
          limited.isInstanceOf[ApplicationError.TransientError],
          broken.isInstanceOf[ApplicationError.TransientError],
        )
      },
      test("anything else is the request itself, and says what the provider called it") {
        for
          backend  = HttpClient.stub(response = """{"error":{"message":"model not found"}}""", StatusCode.NotFound)
          failure <- ChatCompletionClient.openRouter(backend, "api-key", None, Monitor.Noop).complete(asked).flip
        yield assertTrue(failure == ChatCompletionError.Rejected(404, "model not found"))
      },
      test("a refusal that is not an error object keeps what it said anyway") {
        for
          backend  = HttpClient.stub(response = "upstream timeout", StatusCode.BadRequest)
          failure <- ChatCompletionClient.openRouter(backend, "api-key", None, Monitor.Noop).complete(asked).flip
        yield assertTrue(failure == ChatCompletionError.Rejected(400, "upstream timeout"))
      },
    ),
  )

  /** What these tests send, what they get back, and the stubs that carry it. */
  private object Support {

    /** The smallest request this protocol takes, naming the model the tag assertions expect. */
    val asked = CompletionRequest(Model.Name("anthropic/claude-3.5-sonnet"), Chunk(MessageRequest.User(Chunk.empty)))

    /** A complete answer: two choices, a stop reason each, and a usage block that reports cost. */
    val answered: String =
      """{"choices":[{"message":{"content":"12 degrees"},"finish_reason":"stop"},
        |{"message":{"content":"about twelve"},"finish_reason":"stop"}],
        |"usage":{"prompt_tokens":11,"completion_tokens":3,"cost":0.00021}}""".stripMargin

    /** An answer that asks for a tool rather than saying anything. */
    val called: String =
      """{"choices":[{"message":{"tool_calls":[{"id":"c1","type":"function",
        |"function":{"name":"weather","arguments":"{\"city\":\"Hamburg\"}"}}]},
        |"finish_reason":"tool_calls"}]}""".stripMargin

    /** An answer with nothing in it, for a test that only cares what went out. */
    val empty: String = """{"choices":[]}"""

    /** Holds the last request a backend of its making was given, and reads it back. */
    case class Recorder(recorded: Ref[Option[GenericRequest[?, ?]]]):
      def seen: UIO[Option[GenericRequest[?, ?]]] = recorded.get
      def requestBody: UIO[Option[String]]        = seen.map(_.map(_.body.show))

      def httpClient(response: String, status: StatusCode = StatusCode.Ok): BackendStub[Task] =
        HttpClient.stubWith(response, status)(request => recorded.set(Some(request)))

    /** Builds a recorder that has seen nothing yet. */
    object Recorder:
      def make: UIO[Recorder] = Ref.make(Option.empty[GenericRequest[?, ?]]).map(Recorder(_))

    /** Backends that answer every request with the same response, one of them recording what it was given. */
    object HttpClient:
      def stub(response: String, status: StatusCode = StatusCode.Ok): BackendStub[Task] =
        BackendStub[Task](RIOMonadAsyncError[Any]).whenAnyRequest.thenRespondAdjust(response, status)

      def stubWith(
        response: String,
        status: StatusCode = StatusCode.Ok,
      )(
        effect: GenericRequest[?, ?] => Task[Unit]
      ): BackendStub[Task] =
        BackendStub[Task](RIOMonadAsyncError[Any]).whenAnyRequest
          .thenRespondF(request => effect(request).as(ResponseStub.adjust(response, status)))

    /** A monitor that records what it was asked to measure, and runs the work untouched. */
    class Monitoring(recorded: Ref[Chunk[(String, Map[String, String])]]) extends Monitor:
      def seen: UIO[Chunk[(String, Map[String, String])]] = recorded.get
      def callNames: UIO[Chunk[String]]                   = seen.map(_.map((name, _) => name))
      def callTags: UIO[Chunk[Map[String, String]]]       = seen.map(_.map((_, tags) => tags))

      def trace[R, E, A](name: String, tags: (String, String)*)(effect: => ZIO[R, E, A]): ZIO[R, E, A] =
        effect

      def measure[R, E, A](name: String, tags: (String, String)*)(effect: => ZIO[R, E, A]): ZIO[R, E, A] =
        recorded.update(_ :+ (name -> tags.toMap)) *> effect

    /** Builds a monitor that has recorded nothing yet. */
    object Monitoring:
      def make: UIO[Monitoring] = Ref.make(Chunk.empty[(String, Map[String, String])]).map(new Monitoring(_))
  }
