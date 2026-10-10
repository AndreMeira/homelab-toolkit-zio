package homelab.llm.mistral


import homelab.common.error.ApplicationError
import homelab.common.monitor.Monitor
import homelab.llm.Tool
import homelab.llm.mistral.error.MistralError
import homelab.llm.mistral.request.*
import homelab.llm.mistral.response.CompletionResponse
import sttp.client4.GenericRequest
import sttp.client4.impl.zio.RIOMonadAsyncError
import sttp.client4.testing.{ BackendStub, ResponseStub }
import sttp.model.StatusCode
import zio.test.*
import zio.json.ast.Json
import zio.{ Chunk, Ref, Scope, Task, UIO, ZIO }


/** The call itself: what goes out, and everything Mistral's answer carries back — without a network. */
object MistralClientSpec extends ZIOSpecDefault:
  import Support.*

  def spec: Spec[TestEnvironment & Scope, Any] = suite("MistralClient")(
    suite("what it reports")(
      test("one measured call, named for the client and tagged with the model it asked for") {
        for
          backend   = HttpClient.stub(response = plainAnswer)
          monitor  <- Monitoring.make
          _        <- MistralClient.make(backend, "api-key", MistralClient.Endpoint, monitor).complete(userMessage)
          recorded <- monitor.callNames.map(_.headOption)
          tags     <- monitor.callTags.map(_.headOption)
        yield assertTrue(
          recorded.contains("MistralClient.complete"),
          tags.contains(Map("resource" -> "llm", "model" -> "mistral-medium-latest")),
        )
      },
      test("a refused call is measured too, since a failure is what a dashboard is for") {
        for
          backend   = HttpClient.stub(response = """{"object":"error","message":"nope"}""", StatusCode.Unauthorized)
          monitor  <- Monitoring.make
          result   <- MistralClient.make(backend, "api-key", MistralClient.Endpoint, monitor).complete(userMessage).either
          recorded <- monitor.seen
        yield assertTrue(result.isLeft, recorded.size == 1)
      },
    ),
    suite("what it answers with")(
      test("a plain answer, as one text chunk, with why it stopped") {
        for
          backend   = HttpClient.stub(response = plainAnswer)
          response <- MistralClient.make(backend, "api-key", MistralClient.Endpoint, Monitor.Noop).complete(userMessage)
        yield assertTrue(
          response.choices.map(_.message.content) == Chunk(Chunk(ContentChunk.text("12 degrees"))),
          response.choices.flatMap(_.finishReason) == Chunk("stop"),
          response.id == "cmpl-1",
        )
      },
      test("a reasoned answer, as the thinking chunk and then the text") {
        for
          backend   = HttpClient.stub(response = reasonedAnswer)
          response <- MistralClient.make(backend, "api-key", MistralClient.Endpoint, Monitor.Noop).complete(userMessage)
          content   = response.choices.flatMap(_.message.content)
        yield assertTrue(
          content.headOption.exists {
            case ContentChunk.Decoded(ContentChunk.Kind.Thinking(thinking, _, _)) =>
              thinking == Chunk(ContentChunk.text("weighing it up"))
            case _                                                                => false
          },
          content.lastOption.contains(ContentChunk.text("12 degrees")),
        )
      },
      test("a chunk of a kind it does not name, kept whole so the next turn can carry it back") {
        for
          backend   = HttpClient.stub(response = citedAnswer)
          response <- MistralClient.make(backend, "api-key", MistralClient.Endpoint, Monitor.Noop).complete(userMessage)
          content   = response.choices.flatMap(_.message.content)
        yield assertTrue(
          content.headOption.exists {
            case ContentChunk.Raw(json) => json.toString.contains("citation")
            case _                      => false
          }
        )
      },
      test("a call the model made, with no words beside it") {
        for
          backend   = HttpClient.stub(response = calledAnswer)
          response <- MistralClient.make(backend, "api-key", MistralClient.Endpoint, Monitor.Noop).complete(userMessage)
          message   = response.choices.map(_.message)
        yield assertTrue(
          message.flatMap(_.content).isEmpty,
          message.flatMap(_.toolCalls.getOrElse(Chunk.empty)).map(call => call.id -> call.function.name) ==
            Chunk("D681PevKs" -> "weather"),
          response.choices.flatMap(_.finishReason) == Chunk("tool_calls"),
        )
      },
      test("a call's arguments as the JSON the model wrote, whether they arrive as a string or an object") {
        for
          stringed <- MistralClient
                        .make(HttpClient.stub(response = calledAnswer), "api-key", MistralClient.Endpoint, Monitor.Noop)
                        .complete(userMessage)
          objected <- MistralClient
                        .make(HttpClient.stub(response = calledWithAnObject), "api-key", MistralClient.Endpoint, Monitor.Noop)
                        .complete(userMessage)
        yield assertTrue(
          arguments(stringed) == Chunk("""{"city":"Hamburg"}"""),
          arguments(objected) == Chunk("""{"city":"Hamburg"}"""),
        )
      },
      test("how likely each token was, when the request asked") {
        for
          backend   = HttpClient.stub(response = withLogprobs)
          response <- MistralClient.make(backend, "api-key", MistralClient.Endpoint, Monitor.Noop).complete(userMessage)
          tokens    = response.choices.flatMap(_.logprobs.map(_.content).getOrElse(Chunk.empty))
        yield assertTrue(
          tokens.map(_.token) == Chunk("12"),
          tokens.flatMap(_.topLogprobs.getOrElse(Chunk.empty)).map(_.token) == Chunk("12", "11"),
        )
      },
      test("every choice, when several were asked for") {
        for
          backend   = HttpClient.stub(response = twoAnswers)
          response <- MistralClient.make(backend, "api-key", MistralClient.Endpoint, Monitor.Noop).complete(userMessage)
        yield assertTrue(response.choices.map(_.index) == Chunk(0, 1))
      },
      test("the tokens it reported, including those the prompt cache served") {
        for
          backend   = HttpClient.stub(response = cachedAnswer)
          response <- MistralClient.make(backend, "api-key", MistralClient.Endpoint, Monitor.Noop).complete(userMessage)
        yield assertTrue(
          response.usage.map(_.promptTokens).contains(11),
          response.usage.flatMap(_.promptTokensDetails).flatMap(_.cachedTokens).contains(8),
        )
      },
    ),
    suite("what it sends")(
      test("the credential as a bearer token, to the endpoint it was given") {
        for
          recorder <- Recorder.make
          backend   = recorder.httpClient(response = plainAnswer)
          _        <- MistralClient.make(backend, "api-key", MistralClient.Europe, Monitor.Noop).complete(userMessage)
          request  <- recorder.seen
        yield assertTrue(
          request.exists(_.uri == MistralClient.Europe),
          request.exists(_.headers.exists(header => header.name == "Authorization" && header.value == "Bearer api-key")),
        )
      },
      test("a request's own fields reach the body, each under the name Mistral uses") {
        val rich = userMessage.copy(
          randomSeed = Some(7),
          maxTokens = Some(64),
          toolChoice = Some(ToolChoice.Any),
          reasoningEffort = Some(ReasoningEffort.High),
          promptMode = Some(PromptMode.Reasoning),
          serviceTier = Some(ServiceTier.StandardOnly),
          prediction = Some(Prediction("draft")),
        )

        for
          recorder <- Recorder.make
          backend   = recorder.httpClient(response = plainAnswer)
          _        <- MistralClient.make(backend, "api-key", MistralClient.Endpoint, Monitor.Noop).complete(rich)
          body     <- recorder.requestBody
        yield assertTrue(
          body.exists(_.contains(""""random_seed":7""")),
          body.exists(_.contains(""""max_tokens":64""")),
          body.exists(_.contains(""""tool_choice":"any"""")),
          body.exists(_.contains(""""reasoning_effort":"high"""")),
          body.exists(_.contains(""""prompt_mode":"reasoning"""")),
          body.exists(_.contains(""""service_tier":"standard_only"""")),
          body.exists(_.contains(""""prediction":{"content":"draft","type":"content"}""")),
        )
      },
      test("the sampling and logprob fields, each under the name Mistral uses") {
        val sampled = userMessage.copy(
          minTokens = Some(4),
          repetitionPenalty = Some(1.1),
          topK = Some(40),
          logprobs = Some(true),
          topLogprobs = Some(2),
          promptLogprobs = Some(true),
          topPromptLogprobs = Some(1),
        )

        for
          recorder <- Recorder.make
          backend   = recorder.httpClient(response = plainAnswer)
          _        <- MistralClient.make(backend, "api-key", MistralClient.Endpoint, Monitor.Noop).complete(sampled)
          body     <- recorder.requestBody
        yield assertTrue(
          body.exists(_.contains(""""min_tokens":4""")),
          body.exists(_.contains(""""repetition_penalty":1.1""")),
          body.exists(_.contains(""""top_k":40""")),
          body.exists(_.contains(""""logprobs":true,"top_logprobs":2,"prompt_logprobs":true,"top_prompt_logprobs":1""")),
        )
      },
      test("a field left unset is absent rather than null, since the schema refuses what it does not expect") {
        for
          recorder <- Recorder.make
          backend   = recorder.httpClient(response = plainAnswer)
          _        <- MistralClient.make(backend, "api-key", MistralClient.Endpoint, Monitor.Noop).complete(userMessage)
          body     <- recorder.requestBody
        yield assertTrue(
          body.exists(_.contains("""{"model":"mistral-medium-latest","messages":[{"role":"user","content":[{"type":"text","text":"hi"}]}]}""")),
          body.exists(!_.contains("null")),
        )
      },
      test("a call and the tool message answering it, under the ids that pair them") {
        val conversation = userMessage.copy(messages =
          Chunk(
            MessageRequest.User(Chunk(ContentChunk.text("weather?"))),
            MessageRequest.Assistant(
              Chunk.empty,
              toolCalls = Some(Chunk(CallRequest(minted, CallRequest.Function("weather", """{"city":"Hamburg"}""")))),
            ),
            MessageRequest.Tool(minted, Chunk(ContentChunk.text("12 degrees")), name = Some("weather")),
          )
        )

        for
          recorder <- Recorder.make
          backend   = recorder.httpClient(response = plainAnswer)
          _        <- MistralClient.make(backend, "api-key", MistralClient.Endpoint, Monitor.Noop).complete(conversation)
          body     <- recorder.requestBody
        yield assertTrue(
          body.exists(
            _.contains(""""tool_calls":[{"id":"D681PevKs","function":{"name":"weather","arguments":"{\"city\":\"Hamburg\"}"},"type":"function"}]""")
          ),
          body.exists(_.contains(""""role":"tool","tool_call_id":"D681PevKs","content":[{"type":"text","text":"12 degrees"}],"name":"weather"""")),
        )
      },
      test("an assistant message marked as a prefix, for the model to continue") {
        val prefixed = userMessage.copy(messages =
          userMessage.messages :+ MessageRequest.Assistant(Chunk(ContentChunk.text("The capital is")), prefix = Some(true))
        )

        for
          recorder <- Recorder.make
          backend   = recorder.httpClient(response = plainAnswer)
          _        <- MistralClient.make(backend, "api-key", MistralClient.Endpoint, Monitor.Noop).complete(prefixed)
          body     <- recorder.requestBody
        yield assertTrue(body.exists(_.contains(""""prefix":true""")))
      },
      test("what a caller adds is merged over the request, for an API that has moved") {
        for
          recorder <- Recorder.make
          backend   = recorder.httpClient(response = plainAnswer)
          _        <- MistralClient
                        .make(backend, "api-key", MistralClient.Endpoint, Monitor.Noop)
                        .complete(userMessage, Json.Obj("top_k" -> Json.Num(40)))
          body     <- recorder.requestBody
        yield assertTrue(body.exists(_.contains(""""top_k":40""")))
      },
    ),
    suite("a transport of its own")(
      test("a caller with no opinion gets one, and it is closed with the scope") {
        ZIO.scoped(MistralClient.make("api-key")).map(client => assertTrue(client.isInstanceOf[MistralClient]))
      }
    ),
    suite("what it refuses")(
      test("a refused credential is not worth retrying") {
        for
          backend  = HttpClient.stub(response = unauthorised, StatusCode.Unauthorized)
          failure <- MistralClient.make(backend, "api-key", MistralClient.Endpoint, Monitor.Noop).complete(userMessage).flip
        yield assertTrue(
          failure == MistralError.Refused("Unauthorized"),
          !failure.isInstanceOf[ApplicationError.TransientError],
        )
      },
      test("nor is a 403 the API calls an authentication error") {
        for
          backend  = HttpClient.stub(response = forbiddenKey, StatusCode.Forbidden)
          failure <- MistralClient.make(backend, "api-key", MistralClient.Endpoint, Monitor.Noop).complete(userMessage).flip
        yield assertTrue(failure == MistralError.Refused("This key cannot use this model"))
      },
      test("any other 403 is the request refused, not the credential — a guardrail that blocked it, say") {
        for
          backend  = HttpClient.stub(response = blocked, StatusCode.Forbidden)
          failure <- MistralClient.make(backend, "api-key", MistralClient.Endpoint, Monitor.Noop).complete(userMessage).flip
        yield assertTrue(
          failure == MistralError.Rejected(403, "Request blocked"),
          !failure.isInstanceOf[ApplicationError.UnauthorisedError],
        )
      },
      test("a rate limit is, and says which limit") {
        for
          backend  = HttpClient.stub(response = rateLimited, StatusCode.TooManyRequests)
          failure <- MistralClient.make(backend, "api-key", MistralClient.Endpoint, Monitor.Noop).complete(userMessage).flip
        yield assertTrue(
          failure == MistralError.Unavailable("Rate limit exceeded (code 1300)"),
          failure.isInstanceOf[ApplicationError.TransientError],
        )
      },
      test("a tool-call id it will not take says what it called the problem, and its code") {
        for
          backend  = HttpClient.stub(response = foreignId, StatusCode.BadRequest)
          failure <- MistralClient.make(backend, "api-key", MistralClient.Endpoint, Monitor.Noop).complete(userMessage).flip
        yield assertTrue(
          failure == MistralError.Rejected(
            400,
            "Tool call id was call_0fypS1hVX but must be a-z, A-Z, 0-9, with a length of 9. (code 3280)",
          )
        )
      },
      test("a field the schema does not name is pointed at, when the problems come inside an error object") {
        for
          backend  = HttpClient.stub(response = nestedValidation, StatusCode.UnprocessableEntity)
          failure <- MistralClient.make(backend, "api-key", MistralClient.Endpoint, Monitor.Noop).complete(userMessage).flip
        yield assertTrue(
          failure == MistralError.Rejected(422, "body.max_completion_tokens: Extra inputs are not permitted")
        )
      },
      test("and when they come as a bare list") {
        for
          backend  = HttpClient.stub(response = bareValidation, StatusCode.UnprocessableEntity)
          failure <- MistralClient.make(backend, "api-key", MistralClient.Endpoint, Monitor.Noop).complete(userMessage).flip
        yield assertTrue(
          failure == MistralError.Rejected(422, "body.messages.1.name: Extra inputs are not permitted")
        )
      },
      test("a body it cannot read as an error is kept as text") {
        for
          backend  = HttpClient.stub(response = "upstream connect error", StatusCode.BadGateway)
          failure <- MistralClient.make(backend, "api-key", MistralClient.Endpoint, Monitor.Noop).complete(userMessage).flip
        yield assertTrue(failure == MistralError.Unavailable("upstream connect error"))
      },
      test("a call whose arguments are neither a string nor an object is malformed") {
        for
          backend  = HttpClient.stub(response = calledWithAList)
          failure <- MistralClient.make(backend, "api-key", MistralClient.Endpoint, Monitor.Noop).complete(userMessage).flip
        yield assertTrue(failure.isInstanceOf[MistralError.Malformed])
      },
      test("an answer that is not a completion is malformed") {
        for
          backend  = HttpClient.stub(response = """{"choices":"none"}""")
          failure <- MistralClient.make(backend, "api-key", MistralClient.Endpoint, Monitor.Noop).complete(userMessage).flip
        yield assertTrue(failure.isInstanceOf[MistralError.Malformed])
      },
    ),
  )

  /** What these tests send, what they get back, and the stubs that carry it. */
  private object Support {

    /** An id in the form Mistral mints, which goes out as it is. */
    val minted: CallId = CallId.from(Tool.Call.Id("D681PevKs"))

    /** The smallest request this API takes, naming the model the tag assertions expect. */
    val userMessage: CompletionRequest =
      CompletionRequest("mistral-medium-latest", Chunk(MessageRequest.User(Chunk(ContentChunk.text("hi")))))

    /** A complete answer: one string of text, a stop reason, and the tokens it cost. */
    val plainAnswer: String =
      """{"id":"cmpl-1","object":"chat.completion","model":"mistral-medium-latest","created":1760000000,
        |"choices":[{"index":0,"message":{"role":"assistant","content":"12 degrees"},"finish_reason":"stop"}],
        |"usage":{"prompt_tokens":11,"completion_tokens":3,"total_tokens":14}}""".stripMargin

    /** An answer whose content is chunks: what the model reasoned, then what it said. */
    val reasonedAnswer: String =
      """{"id":"cmpl-2","model":"mistral-medium-latest","created":1760000000,
        |"choices":[{"index":0,"message":{"role":"assistant","content":[
        |{"type":"thinking","thinking":[{"type":"text","text":"weighing it up"}]},
        |{"type":"text","text":"12 degrees"}]},"finish_reason":"stop"}]}""".stripMargin

    /** An answer whose first chunk is a kind the client does not name. */
    val citedAnswer: String =
      """{"id":"cmpl-3","model":"mistral-medium-latest","created":1760000000,
        |"choices":[{"index":0,"message":{"role":"assistant","content":[
        |{"type":"citation","source":"weather.example"},{"type":"text","text":"12 degrees"}]},
        |"finish_reason":"stop"}]}""".stripMargin

    /** An answer that only asks for a tool, with the empty string the API sends beside the call. */
    val calledAnswer: String =
      """{"id":"cmpl-4","model":"mistral-medium-latest","created":1760000000,
        |"choices":[{"index":0,"message":{"role":"assistant","content":"","tool_calls":[{"id":"D681PevKs",
        |"function":{"name":"weather","arguments":"{\"city\":\"Hamburg\"}"},"index":0}]},
        |"finish_reason":"tool_calls"}]}""".stripMargin

    /** The same call, with its arguments as an object, which the schema allows. */
    val calledWithAnObject: String =
      """{"id":"cmpl-5","model":"mistral-medium-latest","created":1760000000,
        |"choices":[{"index":0,"message":{"role":"assistant","content":null,"tool_calls":[{"id":"D681PevKs",
        |"function":{"name":"weather","arguments":{"city":"Hamburg"}}}]},"finish_reason":"tool_calls"}]}""".stripMargin

    /** The same call, with its arguments as a list, which the schema does not allow. */
    val calledWithAList: String =
      """{"id":"cmpl-8","model":"mistral-medium-latest","created":1760000000,
        |"choices":[{"index":0,"message":{"role":"assistant","content":"","tool_calls":[{"id":"D681PevKs",
        |"function":{"name":"weather","arguments":["Hamburg"]}}]},"finish_reason":"tool_calls"}]}""".stripMargin

    /** An answer that reports how likely its one token was, and the two likeliest at its position. */
    val withLogprobs: String =
      """{"id":"cmpl-9","model":"mistral-medium-latest","created":1760000000,
        |"choices":[{"index":0,"message":{"role":"assistant","content":"12"},"finish_reason":"stop",
        |"logprobs":{"content":[{"token":"12","logprob":-0.1,"bytes":[49,50],"token_id":1032,
        |"top_logprobs":[{"token":"12","logprob":-0.1,"bytes":[49,50],"token_id":1032},
        |{"token":"11","logprob":-2.4,"bytes":[49,49],"token_id":1031}]}]}}]}""".stripMargin

    /** A 403 the API calls an authentication error. */
    val forbiddenKey: String =
      """{"object":"error","message":"This key cannot use this model","type":"authentication_error","param":null,"code":null}"""

    /** A 403 for the request rather than the key, as a guardrail that blocked it might answer. */
    val blocked: String =
      """{"object":"error","message":"Request blocked","type":"invalid_request_error","param":null,"code":null}"""

    /** Two answers to one request. */
    val twoAnswers: String =
      """{"id":"cmpl-6","model":"mistral-medium-latest","created":1760000000,"choices":[
        |{"index":0,"message":{"role":"assistant","content":"12 degrees"},"finish_reason":"stop"},
        |{"index":1,"message":{"role":"assistant","content":"about 12"},"finish_reason":"stop"}]}""".stripMargin

    /** An answer whose prompt was partly served from the cache. */
    val cachedAnswer: String =
      """{"id":"cmpl-7","model":"mistral-medium-latest","created":1760000000,
        |"choices":[{"index":0,"message":{"role":"assistant","content":"12 degrees"},"finish_reason":"stop"}],
        |"usage":{"prompt_tokens":11,"completion_tokens":3,"total_tokens":14,
        |"prompt_tokens_details":{"cached_tokens":8}}}""".stripMargin

    /** The error object the API answers a refused credential with. */
    val unauthorised: String =
      """{"object":"error","message":"Unauthorized","type":"authentication_error","param":null,"code":null}"""

    /** The error object a rate limit comes back as. */
    val rateLimited: String =
      """{"object":"error","message":"Rate limit exceeded","type":"rate_limited","param":null,"code":"1300"}"""

    /** The error object the API answers a tool-call id it will not take with. */
    val foreignId: String =
      """{"object":"error","message":"Tool call id was call_0fypS1hVX but must be a-z, A-Z, 0-9, with a length of 9.",
        |"type":"invalid_function_call","param":null,"code":"3280","raw_status_code":400}""".stripMargin

    /** A validation failure, its problems nested inside an error object. */
    val nestedValidation: String =
      """{"object":"error","message":{"detail":[{"type":"extra_forbidden","loc":["body","max_completion_tokens"],
        |"msg":"Extra inputs are not permitted","input":8192}]},"type":"invalid_request_error","param":null,
        |"code":null,"raw_status_code":422}""".stripMargin

    /** A validation failure as the bare list of problems. */
    val bareValidation: String =
      """{"detail":[{"type":"extra_forbidden","loc":["body","messages",1,"name"],
        |"msg":"Extra inputs are not permitted","input":"weather"}]}""".stripMargin

    /** The arguments of every call in an answer, as the client read them. */
    def arguments(response: CompletionResponse): Chunk[String] =
      response.choices.flatMap(_.message.toolCalls.getOrElse(Chunk.empty)).map(_.function.arguments)

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
