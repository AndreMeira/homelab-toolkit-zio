package homelab.llm.anthropic


import homelab.llm.anthropic.error.AnthropicError
import homelab.llm.anthropic.request.{ CompletionRequest, Thinking }
import homelab.llm.anthropic.response.CompletionResponse
import homelab.llm.{ Message, Model }
import zio.json.*
import zio.json.ast.Json
import zio.test.*
import zio.{ Chunk, IO, Ref, Scope }


/** What the port narrows, and what it leaves to whoever holds the client. */
object AnthropicModelSpec extends ZIOSpecDefault:

  private def decoded(body: String): CompletionResponse =
    body.fromJson[CompletionResponse].getOrElse(throw new IllegalArgumentException(s"unreadable fixture: $body"))

  /** A client that answers from a fixture, and records what it was asked for. */
  final private class Scripted(answer: CompletionResponse, seen: Ref[Option[(CompletionRequest, Json.Obj)]]) extends AnthropicClient:

    override def complete(
      request: CompletionRequest,
      extra: Json.Obj = Json.Obj(),
    ): IO[AnthropicError, CompletionResponse] =
      seen.set(Some(request -> extra)).as(answer)

  private def asking(body: String, config: AnthropicModel.Config = AnthropicModel.Config()) =
    for
      seen <- Ref.make(Option.empty[(CompletionRequest, Json.Obj)])
      model = AnthropicModel(Scripted(decoded(body), seen), config)
    yield (model, seen)

  private def text(value: String): Chunk[Message.Content] = Chunk(Message.Content.Text(value))

  private val conversation = Model.Request(Chunk(Message.user(text("what is the weather?"))))

  private def ask(model: AnthropicModel, request: Model.Request = conversation) =
    model.complete(Model.Name("claude-3-5-sonnet-latest"), request)

  private val said = """{"content":[{"type":"text","text":"12 degrees"}],"stop_reason":"end_turn"}"""

  def spec: Spec[TestEnvironment & Scope, Any] = suite("AnthropicModel")(
    suite("what it settles")(
      test("a ceiling the port has no field for, since the API will not do without one") {
        for
          (model, seen) <- asking(said)
          _             <- ask(model)
          asked         <- seen.get.map(_.map(_._1))
        yield assertTrue(asked.map(_.maxTokens) == Some(AnthropicModel.DefaultMaxTokens))
      },
      test("the one an instance was built with instead") {
        for
          (model, seen) <- asking(said, AnthropicModel.Config(maxTokens = 64))
          _             <- ask(model)
          asked         <- seen.get.map(_.map(_._1))
        yield assertTrue(asked.map(_.maxTokens) == Some(64))
      },
      test("asks for nothing else the conversation did not imply") {
        for
          (model, seen) <- asking(said)
          _             <- ask(model)
          asked         <- seen.get.map(_.map(_._1))
        yield assertTrue(
          asked.exists(request => request.temperature.isEmpty && request.toolChoice.isEmpty),
          asked.exists(_.thinking.isEmpty),
        )
      },
      test("what the instance always asks for, without the conversation saying so") {
        val warm = AnthropicModel.Config(temperature = Some(0.9), thinking = Some(Thinking.Enabled(512)))
        for
          (model, seen) <- asking(said, warm)
          _             <- ask(model)
          asked         <- seen.get.map(_.map(_._1))
        yield assertTrue(asked.flatMap(_.temperature) == Some(0.9), asked.exists(_.thinking.isDefined))
      },
      test("what the instance asks for beyond the API this adapter models") {
        val configured = AnthropicModel.Config(extra = Json.Obj("service_tier" -> Json.Str("standard")))
        for
          (model, seen) <- asking(said, configured)
          _             <- ask(model)
          carried       <- seen.get.map(_.map(_._2))
        yield assertTrue(carried == Some(Json.Obj("service_tier" -> Json.Str("standard"))))
      },
    ),
    suite("what it reshapes")(
      test("the instructions out of the conversation and into a field of their own") {
        val told = Model.Request(Chunk(Message.system(text("be brief")), Message.user(text("hi"))))
        for
          (model, seen) <- asking(said)
          _             <- ask(model, told)
          asked         <- seen.get.map(_.map(_._1))
        yield assertTrue(
          asked.flatMap(_.system) == Some("be brief"),
          asked.exists(_.messages.map(_.role) == List("user")),
        )
      }
    ),
    suite("what it reads")(
      test("the words, why it stopped, and what it consumed — with no cost, which this API does not report") {
        val body = """{"content":[{"type":"text","text":"12 degrees"}],"stop_reason":"end_turn",
                     |"usage":{"input_tokens":11,"output_tokens":3}}""".stripMargin
        for
          (model, _) <- asking(body)
          completion <- ask(model)
        yield assertTrue(
          completion.content == text("12 degrees"),
          completion.finish == Model.FinishReason.Stop,
          completion.usage == Model.Usage(11, 3, None),
        )
      },
      test("a call, whose arguments come back as the string the toolkit carries") {
        val body = """{"content":[{"type":"tool_use","id":"c1","name":"weather","input":{"city":"Hamburg"}}],
                     |"stop_reason":"tool_use"}""".stripMargin
        for
          (model, _) <- asking(body)
          completion <- ask(model)
        yield assertTrue(
          completion.calls.map(_.arguments) == Chunk("""{"city":"Hamburg"}"""),
          completion.finish == Model.FinishReason.ToolCalls,
          completion.content.isEmpty,
        )
      },
      test("this API's names for stopping are translated to the toolkit's") {
        for
          (model, _) <- asking("""{"content":[],"stop_reason":"max_tokens"}""")
          completion <- ask(model)
        yield assertTrue(completion.finish == Model.FinishReason.Length)
      },
    ),
    suite("what it refuses")(
      test("a body with neither content nor a reason has not answered") {
        for
          (model, _) <- asking("""{"content":[]}""")
          failure    <- ask(model).flip
        yield assertTrue(failure.isInstanceOf[AnthropicError.Malformed])
      }
    ),
  )
