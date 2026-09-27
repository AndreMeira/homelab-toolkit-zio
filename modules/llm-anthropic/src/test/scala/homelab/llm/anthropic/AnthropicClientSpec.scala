package homelab.llm.anthropic


import homelab.common.error.ApplicationError
import homelab.common.monitor.Monitor
import homelab.llm.Model
import homelab.llm.anthropic.error.AnthropicError
import homelab.llm.anthropic.request.{ CompletionRequest, MessageRequest, Thinking, ToolChoice }
import homelab.llm.anthropic.response.CompletionResponse
import sttp.client4.GenericRequest
import sttp.client4.impl.zio.RIOMonadAsyncError
import sttp.client4.testing.{ BackendStub, ResponseStub }
import sttp.model.StatusCode
import zio.test.*
import zio.json.ast.Json
import zio.{ Chunk, Ref, Scope, Task, UIO, ZIO }


/** The call itself: what goes out, and everything Anthropic's answer carries back — without a network. */
object AnthropicClientSpec extends ZIOSpecDefault:
  import Support.*

  def spec: Spec[TestEnvironment & Scope, Any] = suite("AnthropicClient")(
    suite("what it reports")(
      test("one measured call, named for the client and tagged with the model it asked for") {
        for
          backend   = HttpClient.stub(response = temperatureAnswer)
          monitor  <- Monitoring.make
          _        <- AnthropicClient.make(backend, "api-key", monitor).complete(asked)
          recorded <- monitor.callNames.map(_.headOption)
          tags     <- monitor.callTags.map(_.headOption)
        yield assertTrue(
          recorded.contains("AnthropicClient.complete"),
          tags.contains(Map("resource" -> "llm", "model" -> "claude-3-5-sonnet-latest")),
        )
      },
      test("a refused call is measured too, since a failure is what a dashboard is for") {
        for
          backend   = HttpClient.stub(response = """{"error":{"message":"nope"}}""", StatusCode.Unauthorized)
          monitor  <- Monitoring.make
          result   <- AnthropicClient.make(backend, "api-key", monitor).complete(asked).either
          recorded <- monitor.seen
        yield assertTrue(result.isLeft, recorded.size == 1)
      },
    ),
    suite("what it answers with")(
      test("every block, in the order they came") {
        for
          backend   = HttpClient.stub(response = temperatureAnswer)
          response <- AnthropicClient.make(backend, "api-key", Monitor.Noop).complete(asked)
        yield assertTrue(
          response.content == Chunk(CompletionResponse.Block.Decoded(CompletionResponse.Block.Kind.Text("12 degrees"))),
          response.stopReason.contains("end_turn"),
        )
      },
      test("a block it does not model, kept whole so the next turn can carry it back") {
        for
          backend   = HttpClient.stub(response = reasoned)
          response <- AnthropicClient.make(backend, "api-key", Monitor.Noop).complete(asked)
        yield assertTrue(
          response.content.size == 2,
          response.content.headOption.exists {
            case CompletionResponse.Block.Raw(json) => json.toString.contains("weighing it up")
            case _                                  => false
          },
        )
      },
      test("the tokens it reported, which carry no cost on this API") {
        for
          backend   = HttpClient.stub(response = temperatureAnswer)
          response <- AnthropicClient.make(backend, "api-key", Monitor.Noop).complete(asked)
        yield assertTrue(response.usage.map(_.inputTokens).contains(11))
      },
    ),
    suite("what it sends")(
      test("the credential and the version in the headers this API reads them from") {
        for
          recorder <- Recorder.make
          backend   = recorder.httpClient(response = empty)
          _        <- AnthropicClient.make(backend, "api-key", Monitor.Noop).complete(asked).ignore
          request  <- recorder.seen
        yield assertTrue(
          request.exists(_.uri == AnthropicClient.Endpoint),
          request.exists(_.headers.exists(header => header.name == "x-api-key" && header.value == "api-key")),
          request.exists(_.headers.exists(_.name == "anthropic-version")),
          request.exists(sent => !sent.headers.exists(_.name == "Authorization")),
        )
      },
      test("a request's own fields reach the body, each under the name the API uses") {
        val rich = asked.copy(
          temperature = Some(0.2),
          topK = Some(40),
          toolChoice = Some(ToolChoice.Named("weather")),
        )

        for
          recorder <- Recorder.make
          backend   = recorder.httpClient(response = empty)
          _        <- AnthropicClient.make(backend, "api-key", Monitor.Noop).complete(rich).ignore
          body     <- recorder.requestBody
        yield assertTrue(
          body.exists(_.contains(""""max_tokens":64""")),
          body.exists(_.contains(""""temperature":0.2""")),
          body.exists(_.contains(""""top_k":40""")),
          body.exists(_.contains(""""tool_choice":{"type":"tool","name":"weather"}""")),
        )
      },
      test("thinking carries its budget, and says so the way the API does") {
        for
          recorder <- Recorder.make
          backend   = recorder.httpClient(response = empty)
          _        <- AnthropicClient
                        .make(backend, "api-key", Monitor.Noop)
                        .complete(asked.copy(thinking = Some(Thinking.Enabled(1024))))
                        .ignore
          body     <- recorder.requestBody
        yield assertTrue(body.exists(_.contains(""""thinking":{"type":"enabled","budget_tokens":1024}""")))
      },
      test("what a caller adds is merged over the request, for an API that has moved") {
        for
          recorder <- Recorder.make
          backend   = recorder.httpClient(response = empty)
          _        <- AnthropicClient
                        .make(backend, "api-key", Monitor.Noop)
                        .complete(asked, Json.Obj("service_tier" -> Json.Str("auto")))
                        .ignore
          body     <- recorder.requestBody
        yield assertTrue(body.exists(_.contains(""""service_tier":"auto"""")))
      },
    ),
    suite("a transport of its own")(
      test("a caller with no opinion gets one, and it is closed with the scope") {
        ZIO.scoped(AnthropicClient.make("api-key")).map(client => assertTrue(client.isInstanceOf[AnthropicClient]))
      }
    ),
    suite("what it refuses")(
      test("a refused credential is not worth retrying") {
        for
          backend  = HttpClient.stub(response = """{"error":{"message":"invalid x-api-key"}}""", StatusCode.Unauthorized)
          failure <- AnthropicClient.make(backend, "api-key", Monitor.Noop).complete(asked).flip
        yield assertTrue(
          failure == AnthropicError.Refused("invalid x-api-key"),
          !failure.isInstanceOf[ApplicationError.TransientError],
        )
      },
      test("an overloaded API is") {
        for
          backend  = HttpClient.stub(response = """{"error":{"message":"Overloaded"}}""", StatusCode.TooManyRequests)
          failure <- AnthropicClient.make(backend, "api-key", Monitor.Noop).complete(asked).flip
        yield assertTrue(failure.isInstanceOf[ApplicationError.TransientError])
      },
      test("a request it will not take says what it called the problem") {
        for
          backend  = HttpClient.stub(response = """{"error":{"message":"max_tokens: field required"}}""", StatusCode.BadRequest)
          failure <- AnthropicClient.make(backend, "api-key", Monitor.Noop).complete(asked).flip
        yield assertTrue(failure == AnthropicError.Rejected(400, "max_tokens: field required"))
      },
    ),
  )

  /** What these tests send, what they get back, and the stubs that carry it. */
  private object Support {

    /** The smallest request this API takes, naming the model the tag assertions expect. */
    val asked = CompletionRequest(
      Model.Name("claude-3-5-sonnet-latest"),
      64,
      Chunk(MessageRequest("user", Chunk.empty)),
    )

    /** A complete answer: one text block, a stop reason, and the tokens it cost. */
    val temperatureAnswer: String =
      """{"content":[{"type":"text","text":"12 degrees"}],"stop_reason":"end_turn",
        |"usage":{"input_tokens":11,"output_tokens":3}}""".stripMargin

    /** An answer whose first block is a kind the client does not model. */
    val reasoned: String =
      """{"content":[{"type":"thinking","thinking":"weighing it up","signature":"sig"},
        |{"type":"text","text":"12 degrees"}],"stop_reason":"end_turn"}""".stripMargin

    /** An answer with nothing in it, for a test that only cares what went out. */
    val empty: String = """{"content":[]}"""

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
