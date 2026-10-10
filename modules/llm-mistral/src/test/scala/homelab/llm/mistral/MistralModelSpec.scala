package homelab.llm.mistral


import homelab.llm.mistral.error.MistralError
import homelab.llm.mistral.request.{ CompletionRequest, ReasoningEffort }
import homelab.llm.mistral.response.CompletionResponse
import homelab.llm.{ Message, Model }
import zio.json.*
import zio.json.ast.Json
import zio.test.*
import zio.{ Chunk, IO, Ref, Scope, UIO, ZIO }


/** What the port narrows, and what it leaves to whoever holds the client. */
object MistralModelSpec extends ZIOSpecDefault:
  import Support.*

  def spec: Spec[TestEnvironment & Scope, Any] = suite("MistralModel")(
    suite("what it settles")(
      test("asks for nothing the conversation did not imply") {
        for
          client <- ClientStub.answering(said)
          _      <- MistralModel(client).complete(medium, conversation)
          asked  <- client.seen
        yield assertTrue(
          asked.exists(request => request.temperature.isEmpty && request.toolChoice.isEmpty),
          asked.exists(request => request.reasoningEffort.isEmpty && request.maxTokens.isEmpty),
        )
      },
      test("what the instance always asks for, without the conversation saying so") {
        for
          client <- ClientStub.answering(said)
          config  = MistralModel.Config(temperature = Some(0.3), topK = Some(40), reasoningEffort = Some(ReasoningEffort.High))
          _      <- MistralModel(client, config).complete(medium, conversation)
          asked  <- client.seen
        yield assertTrue(
          asked.flatMap(_.temperature).contains(0.3),
          asked.flatMap(_.topK).contains(40),
          asked.flatMap(_.reasoningEffort).contains(ReasoningEffort.High),
        )
      },
      test("what the instance asks for beyond the API this adapter models") {
        for
          client  <- ClientStub.answering(said)
          config   = MistralModel.Config(extra = Json.Obj("top_k" -> Json.Num(40)))
          _       <- MistralModel(client, config).complete(medium, conversation)
          carried <- client.carried
        yield assertTrue(carried.contains(Json.Obj("top_k" -> Json.Num(40))))
      },
    ),
    suite("what it reads")(
      test("the words, why it stopped, and what it consumed — with no cost, which this API does not report") {
        for
          client     <- ClientStub.answering(counted)
          completion <- MistralModel(client).complete(medium, conversation)
        yield assertTrue(
          completion.content == text("12 degrees"),
          completion.finish == Model.FinishReason.Stop,
          completion.usage == Model.Usage(11, 3, None),
        )
      },
      test("a call, under the id Mistral gave it and with the arguments it wrote") {
        for
          client     <- ClientStub.answering(called)
          completion <- MistralModel(client).complete(medium, conversation)
        yield assertTrue(
          completion.calls.map(call => call.id -> call.arguments) == Chunk("D681PevKs" -> """{"city":"Hamburg"}"""),
          completion.finish == Model.FinishReason.ToolCalls,
          completion.content.isEmpty,
        )
      },
      test("reasoning, kept whole beside the words so the next turn carries it back") {
        for
          client     <- ClientStub.answering(reasoned)
          completion <- MistralModel(client).complete(medium, conversation)
        yield assertTrue(
          completion.content.headOption.exists {
            case Message.Content.Raw(json) => json.toJson.contains(""""type":"thinking"""") && json.toJson.contains("weighing it up")
            case _                         => false
          },
          completion.content.lastOption.contains(Message.Content.Text("12 degrees")),
        )
      },
      test("an answer that stopped at the model's context length as having run out of room") {
        for
          client     <- ClientStub.answering(stoppedWith("model_length"))
          completion <- MistralModel(client).complete(medium, conversation)
        yield assertTrue(completion.finish == Model.FinishReason.Length)
      },
      test("an answer that stopped on an error, with the reason as the API gave it") {
        for
          client     <- ClientStub.answering(stoppedWith("error"))
          completion <- MistralModel(client).complete(medium, conversation)
        yield assertTrue(completion.finish == Model.FinishReason.Other("error"))
      },
      test("the first answer, when several came back") {
        for
          client     <- ClientStub.answering(twoAnswers)
          completion <- MistralModel(client).complete(medium, conversation)
        yield assertTrue(completion.content == text("12 degrees"))
      },
    ),
    suite("what it refuses")(
      test("a body with no choice has not answered") {
        for
          client  <- ClientStub.answering("""{"id":"cmpl-0","model":"mistral-medium-latest","created":0,"choices":[]}""")
          failure <- MistralModel(client).complete(medium, conversation).flip
        yield assertTrue(failure.isInstanceOf[MistralError.Malformed])
      }
    ),
  )

  /** What these tests ask for, what the API answers, and the client that stands in for it. */
  private object Support {

    /** The model every test names. */
    val medium: Model.Name = Model.Name("mistral-medium-latest")

    /** A one-turn conversation, enough to build a request from. */
    val conversation: Model.Request = Model.Request(Chunk(Message.user(text("what is the weather?"))))

    /** A plain answer, for a test that cares what was asked rather than what came back. */
    val said: String = stoppedWith("stop")

    /** An answer that reports what the call consumed. */
    val counted: String =
      """{"id":"cmpl-1","model":"mistral-medium-latest","created":0,
        |"choices":[{"index":0,"message":{"role":"assistant","content":"12 degrees"},"finish_reason":"stop"}],
        |"usage":{"prompt_tokens":11,"completion_tokens":3,"total_tokens":14}}""".stripMargin

    /** An answer that asks for a tool rather than saying anything. */
    val called: String =
      """{"id":"cmpl-2","model":"mistral-medium-latest","created":0,
        |"choices":[{"index":0,"message":{"role":"assistant","content":"","tool_calls":[{"id":"D681PevKs",
        |"function":{"name":"weather","arguments":"{\"city\":\"Hamburg\"}"}}]},"finish_reason":"tool_calls"}]}""".stripMargin

    /** An answer whose content is what the model reasoned, then what it said. */
    val reasoned: String =
      """{"id":"cmpl-3","model":"mistral-medium-latest","created":0,
        |"choices":[{"index":0,"message":{"role":"assistant","content":[
        |{"type":"thinking","thinking":[{"type":"text","text":"weighing it up"}]},
        |{"type":"text","text":"12 degrees"}]},"finish_reason":"stop"}]}""".stripMargin

    /** Two answers to one request. */
    val twoAnswers: String =
      """{"id":"cmpl-4","model":"mistral-medium-latest","created":0,"choices":[
        |{"index":0,"message":{"role":"assistant","content":"12 degrees"},"finish_reason":"stop"},
        |{"index":1,"message":{"role":"assistant","content":"about 12"},"finish_reason":"stop"}]}""".stripMargin

    /**
     * An answer that says 12 degrees and stopped for the given reason.
     *
     * @param reason what the API calls why it stopped
     * @return the body
     */
    def stoppedWith(reason: String): String =
      s"""{"id":"cmpl-5","model":"mistral-medium-latest","created":0,
         |"choices":[{"index":0,"message":{"role":"assistant","content":"12 degrees"},"finish_reason":"$reason"}]}""".stripMargin

    /** One text part, the shape a turn's content takes. */
    def text(value: String): Chunk[Message.Content] = Chunk(Message.Content.Text(value))

    /** A client that answers from a fixture, and holds what it was asked for. */
    final class ClientStub(answer: CompletionResponse, recorded: Ref[Option[(CompletionRequest, Json.Obj)]]) extends MistralClient:
      def seen: UIO[Option[CompletionRequest]] = recorded.get.map(_.map((request, _) => request))
      def carried: UIO[Option[Json.Obj]]       = recorded.get.map(_.map((_, extra) => extra))

      override def complete(request: CompletionRequest, extra: Json.Obj = Json.Obj()): IO[MistralError, CompletionResponse] =
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
