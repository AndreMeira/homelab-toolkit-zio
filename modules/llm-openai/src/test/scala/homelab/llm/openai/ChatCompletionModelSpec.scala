package homelab.llm.openai


import homelab.llm.openai.error.ChatCompletionError
import homelab.llm.openai.request.CompletionRequest
import homelab.llm.openai.response.CompletionResponse
import homelab.llm.{Message, Model}
import zio.json.*
import zio.json.ast.Json
import zio.test.*
import zio.{Chunk, IO, Ref, Scope, ZIO}


/** What the port narrows, and what it leaves to whoever holds the client. */
object ChatCompletionModelSpec extends ZIOSpecDefault:

  private def decoded(body: String): CompletionResponse =
    body.fromJson[CompletionResponse].getOrElse(throw new IllegalArgumentException(s"unreadable fixture: $body"))

  /** A client that answers from a fixture, and records what it was asked for. */
  private final class Scripted(answer: CompletionResponse, seen: Ref[Option[CompletionRequest]])
      extends ChatCompletionClient:

    override def complete(request: CompletionRequest): IO[ChatCompletionError, CompletionResponse] =
      seen.set(Some(request)).as(answer)

  private def asking(body: String, config: ChatCompletionModel.Config = ChatCompletionModel.Config()) =
    for
      seen <- Ref.make(Option.empty[CompletionRequest])
      model = ChatCompletionModel(Scripted(decoded(body), seen), config)
    yield (model, seen)

  private def text(value: String): Chunk[Message.Content] = Chunk(Message.Content.Text(value))

  private val conversation = Model.Request(Chunk(Message.user(text("what is the weather?"))))

  private def ask(model: ChatCompletionModel, request: Model.Request = conversation) =
    model.complete(Model.Name("gpt-4o"), request)

  def spec: Spec[TestEnvironment & Scope, Any] = suite("ChatCompletionModel")(
    suite("what it narrows")(
      test("takes the first of several answers, because a completion holds one") {
        val body = """{"choices":[{"message":{"content":"first"},"finish_reason":"stop"},
                     |{"message":{"content":"second"},"finish_reason":"stop"}]}""".stripMargin
        for
          (model, _) <- asking(body)
          completion <- ask(model)
        yield assertTrue(completion.content == text("first"))
      },
      test("asks for nothing the conversation did not imply") {
        val body = """{"choices":[{"message":{"content":"ok"},"finish_reason":"stop"}]}"""
        for
          (model, seen) <- asking(body)
          _             <- ask(model)
          asked         <- seen.get
        yield assertTrue(
          asked.exists(request => request.n.isEmpty && request.maxTokens.isEmpty && request.temperature.isEmpty),
          asked.exists(_.model == "gpt-4o"),
        )
      },
      test("what the instance always asks for, without the conversation saying so") {
        val body = """{"choices":[{"message":{"content":"ok"},"finish_reason":"stop"}]}"""
        val cool = ChatCompletionModel.Config(temperature = Some(0.1), maxTokens = Some(256))
        for
          (model, seen) <- asking(body, cool)
          _             <- ask(model)
          asked         <- seen.get
        yield assertTrue(asked.flatMap(_.temperature) == Some(0.1), asked.flatMap(_.maxTokens) == Some(256))
      },
      test("carries a caller's own fields through, over the instance's own") {
        val body       = """{"choices":[{"message":{"content":"ok"},"finish_reason":"stop"}]}"""
        val configured = ChatCompletionModel.Config(extra = Json.Obj("seed" -> Json.Num(1)))
        for
          (model, seen) <- asking(body, configured)
          _             <- ask(model, conversation.copy(extra = Json.Obj("seed" -> Json.Num(7))))
          asked         <- seen.get
        yield assertTrue(asked.map(_.extra) == Some(Json.Obj("seed" -> Json.Num(7))))
      },
    ),
    suite("what it reads")(
      test("the words, why it stopped, and what the call cost") {
        val body = """{"choices":[{"message":{"content":"12 degrees"},"finish_reason":"stop"}],
                     |"usage":{"prompt_tokens":11,"completion_tokens":3,"cost":0.00021}}""".stripMargin
        for
          (model, _) <- asking(body)
          completion <- ask(model)
        yield assertTrue(
          completion.content == text("12 degrees"),
          completion.finish == Model.FinishReason.Stop,
          completion.usage == Model.Usage(11, 3, Some(BigDecimal("0.00021"))),
        )
      },
      test("the calls the model asked for, arguments still unparsed") {
        val body = """{"choices":[{"message":{"tool_calls":[{"id":"c1","type":"function",
                     |"function":{"name":"weather","arguments":"{\"city\":\"Hamburg\"}"}}]},
                     |"finish_reason":"tool_calls"}]}""".stripMargin
        for
          (model, _) <- asking(body)
          completion <- ask(model)
        yield assertTrue(
          completion.calls.map(_.name) == Chunk("weather"),
          completion.calls.map(_.arguments) == Chunk("""{"city":"Hamburg"}"""),
          completion.finish == Model.FinishReason.ToolCalls,
        )
      },
      test("a reason the toolkit does not name is kept as the provider spelled it") {
        val body = """{"choices":[{"message":{"content":"x"},"finish_reason":"error"}]}"""
        for
          (model, _) <- asking(body)
          completion <- ask(model)
        yield assertTrue(completion.finish == Model.FinishReason.Other("error"))
      },
    ),
    suite("what it refuses")(
      test("a body with no choices has not answered") {
        for
          (model, _) <- asking("""{"choices":[]}""")
          failure    <- ask(model).flip
        yield assertTrue(failure == ChatCompletionError.Malformed("the response carried no choices"))
      },
    ),
  )
