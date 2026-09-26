package homelab.llm.anthropic


import homelab.llm.Model
import homelab.llm.anthropic.error.AnthropicError
import homelab.llm.anthropic.request.{ CompletionRequest, MessageRequest }
import homelab.llm.anthropic.response.CompletionResponse
import sttp.client4.impl.zio.RIOMonadAsyncError
import sttp.client4.testing.BackendStub
import sttp.model.StatusCode
import zio.test.*
import zio.{ Ref, Scope, Task, ZIO }


/** The call itself: what goes out, and everything Anthropic's answer carries back — without a network. */
object AnthropicClientSpec extends ZIOSpecDefault:

  private val answered =
    """{"content":[{"type":"text","text":"12 degrees"}],"stop_reason":"end_turn",
      |"usage":{"input_tokens":11,"output_tokens":3}}""".stripMargin

  private val reasoned =
    """{"content":[{"type":"thinking","thinking":"weighing it up","signature":"sig"},
      |{"type":"text","text":"12 degrees"}],"stop_reason":"end_turn"}""".stripMargin

  private def answering(body: String, status: StatusCode = StatusCode.Ok): AnthropicClient =
    val backend = BackendStub[Task](RIOMonadAsyncError[Any]).whenAnyRequest.thenRespondAdjust(body, status)
    AnthropicClient.make(backend, "test-key")

  /** A stub that answers nothing, and records the one request it was handed. */
  private def recording(seen: Ref[Option[sttp.client4.GenericRequest[?, ?]]]) =
    BackendStub[Task](RIOMonadAsyncError[Any]).whenAnyRequest.thenRespondF { request =>
      seen.set(Some(request)).as(sttp.client4.testing.ResponseStub.adjust("""{"content":[]}"""))
    }

  private val asked =
    CompletionRequest(Model.Name("claude-3-5-sonnet-latest"), 64, List(MessageRequest("user", Nil)))

  private def ask(client: AnthropicClient) = client.complete(asked)

  def spec: Spec[TestEnvironment & Scope, Any] = suite("AnthropicClient")(
    suite("what it answers with")(
      test("every block, in the order they came") {
        for response <- ask(answering(answered))
        yield assertTrue(
          response.content == List(CompletionResponse.Block.Decoded(CompletionResponse.Block.Kind.Text("12 degrees"))),
          response.stopReason == Some("end_turn"),
        )
      },
      test("a block it does not model, kept whole so the next turn can carry it back") {
        for response <- ask(answering(reasoned))
        yield assertTrue(
          response.content.size == 2,
          response.content.head match
            case CompletionResponse.Block.Raw(json) => json.toString.contains("weighing it up")
            case _                                  => false,
        )
      },
      test("the tokens it reported, which carry no cost on this API") {
        for response <- ask(answering(answered))
        yield assertTrue(response.usage.map(_.inputTokens) == Some(11))
      },
    ),
    suite("what it sends")(
      test("the credential and the version in the headers this API reads them from") {
        for
          seen    <- Ref.make(Option.empty[sttp.client4.GenericRequest[?, ?]])
          _       <- AnthropicClient.make(recording(seen), "k").complete(asked).ignore
          request <- seen.get
        yield assertTrue(
          request.exists(_.uri == AnthropicClient.Endpoint),
          request.exists(_.headers.exists(header => header.name == "x-api-key" && header.value == "k")),
          request.exists(_.headers.exists(_.name == "anthropic-version")),
          request.exists(sent => !sent.headers.exists(_.name == "Authorization")),
        )
      },
      test("a request's own fields reach the body, and extra is merged over them") {
        for
          seen <- Ref.make(Option.empty[sttp.client4.GenericRequest[?, ?]])
          rich  = asked.copy(temperature = Some(0.2), extra = zio.json.ast.Json.Obj("top_k" -> zio.json.ast.Json.Num(5)))
          _    <- AnthropicClient.make(recording(seen), "k").complete(rich).ignore
          body <- seen.get.map(_.map(_.body.show))
        yield assertTrue(
          body.exists(_.contains(""""max_tokens":64""")),
          body.exists(_.contains(""""temperature":0.2""")),
          body.exists(_.contains(""""top_k":5""")),
        )
      },
    ),
    suite("a transport of its own")(
      test("a caller with no opinion gets one, and it is closed with the scope") {
        ZIO.scoped(AnthropicClient.make("k")).map(client => assertTrue(client.isInstanceOf[AnthropicClient]))
      },
    ),
    suite("what it refuses")(
      test("a refused credential is not worth retrying") {
        val body = """{"error":{"message":"invalid x-api-key"}}"""
        for failure <- ask(answering(body, StatusCode.Unauthorized)).flip
        yield assertTrue(
          failure == AnthropicError.Refused("invalid x-api-key"),
          !failure.isInstanceOf[homelab.common.error.ApplicationError.TransientError],
        )
      },
      test("an overloaded API is") {
        val body = """{"error":{"message":"Overloaded"}}"""
        for failure <- ask(answering(body, StatusCode.TooManyRequests)).flip
        yield assertTrue(failure.isInstanceOf[homelab.common.error.ApplicationError.TransientError])
      },
      test("a request it will not take says what it called the problem") {
        val body = """{"error":{"message":"max_tokens: field required"}}"""
        for failure <- ask(answering(body, StatusCode.BadRequest)).flip
        yield assertTrue(failure == AnthropicError.Rejected(400, "max_tokens: field required"))
      },
    ),
  )
