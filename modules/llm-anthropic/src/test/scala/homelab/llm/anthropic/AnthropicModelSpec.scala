package homelab.llm.anthropic


import homelab.llm.{ Message, Model }
import sttp.client4.impl.zio.RIOMonadAsyncError
import sttp.client4.testing.BackendStub
import sttp.model.StatusCode
import zio.test.*
import zio.{ Chunk, Scope, Task }


/** What the adapter makes of what Anthropic answers, without a network. */
object AnthropicModelSpec extends ZIOSpecDefault:

  private val answered =
    """{"content":[{"type":"text","text":"12 degrees"}],"stop_reason":"end_turn",
      |"usage":{"input_tokens":11,"output_tokens":3}}""".stripMargin

  private val asked =
    """{"content":[{"type":"tool_use","id":"c1","name":"weather","input":{"city":"Hamburg"}}],
      |"stop_reason":"tool_use","usage":{"input_tokens":20,"output_tokens":9}}""".stripMargin

  private val reasoned =
    """{"content":[{"type":"thinking","thinking":"weighing it up","signature":"sig"},
      |{"type":"text","text":"12 degrees"}],"stop_reason":"end_turn"}""".stripMargin

  private def answering(body: String, status: StatusCode = StatusCode.Ok): AnthropicModel =
    val backend = BackendStub[Task](RIOMonadAsyncError[Any]).whenAnyRequest.thenRespondAdjust(body, status)
    new AnthropicModel(backend, "test-key")

  private def ask(model: AnthropicModel) =
    model.complete(Model.Name("claude-3-5-sonnet-latest"), Model.Request(Chunk(Message.user(Chunk.empty))))

  def spec: Spec[TestEnvironment & Scope, Any] = suite("AnthropicModel")(
    suite("what it reads")(
      test("the words, why it stopped, and what it consumed — with no cost, which this API does not report") {
        for completion <- ask(answering(answered))
        yield assertTrue(
          completion.content == Chunk(Message.Content.Text("12 degrees")),
          completion.finish == Model.FinishReason.Stop,
          completion.usage == Model.Usage(11, 3, None),
        )
      },
      test("a call, whose arguments come back as the string the toolkit carries") {
        for completion <- ask(answering(asked))
        yield assertTrue(
          completion.calls.map(_.name) == Chunk("weather"),
          completion.calls.map(_.arguments) == Chunk("""{"city":"Hamburg"}"""),
          completion.finish == Model.FinishReason.ToolCalls,
          completion.content.isEmpty,
        )
      },
      test("a block it does not model is kept whole, since the next turn has to carry it back") {
        for completion <- ask(answering(reasoned))
        yield assertTrue(
          completion.content.size == 2,
          completion.content.exists {
            case Message.Content.Raw(json) => json.toString.contains("weighing it up")
            case _                         => false
          },
          completion.content.contains(Message.Content.Text("12 degrees")),
        )
      },
      test("this API's names for stopping are translated to the toolkit's") {
        val ended = """{"content":[],"stop_reason":"max_tokens"}"""
        for completion <- ask(answering(ended))
        yield assertTrue(completion.finish == Model.FinishReason.Length)
      },
      test("a reason it does not name is kept as the API spelled it") {
        for completion <- ask(answering("""{"content":[],"stop_reason":"pause_turn"}"""))
        yield assertTrue(completion.finish == Model.FinishReason.Other("pause_turn"))
      },
    ),
    suite("what it refuses")(
      test("a body with neither content nor a reason has not answered") {
        for failure <- ask(answering("""{"content":[]}""")).flip
        yield assertTrue(failure.isInstanceOf[AnthropicError.Malformed])
      },
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
