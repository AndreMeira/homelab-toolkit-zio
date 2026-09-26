package homelab.llm.openai


import homelab.llm.openai.error.ChatCompletionError
import homelab.llm.openai.request.CompletionRequest
import homelab.llm.openai.response.{ CompletionResponse, FailureResponse }
import sttp.client4.*
import sttp.client4.httpclient.zio.HttpClientZioBackend
import sttp.model.{ StatusCode, Uri }
import zio.json.ast.Json
import zio.{ IO, Scope, Task, ZIO }


/**
 * The chat-completions call, in Scala types.
 *
 * One method, and nothing withheld: a request carries everything the protocol takes and a response gives
 * back everything it answered with — every choice, not the first, and the reason and usage as they came.
 * What a caller does with that is theirs; [[ChatCompletionModel]] is one thing to do with it, and holding
 * this instead is how to reach what a port has no field for.
 *
 * Streaming is deliberately absent: a request here is one call and one answer.
 */
trait ChatCompletionClient {

  /**
   * Ask for a completion.
   *
   * @param request what to ask for
   * @param extra fields merged over the request, for a protocol that has moved since [[CompletionRequest]]
   *              last did — a field this type already names belongs in the field
   * @return what the provider answered; aborts with what it or the transport refused
   */
  def complete(request: CompletionRequest, extra: Json.Obj = Json.Obj()): IO[ChatCompletionError, CompletionResponse]
}


object ChatCompletionClient:

  /** Where OpenRouter serves chat completions. */
  val OpenRouter: Uri = uri"https://openrouter.ai/api/v1/chat/completions"

  /** Where OpenAI serves them. */
  val OpenAi: Uri = uri"https://api.openai.com/v1/chat/completions"

  /**
   * OpenRouter, which fronts many providers and reports what a call cost.
   *
   * The transport is made here and closed when the scope ends, which is what a caller wants who has no
   * opinion about how to reach it. The overload taking a backend is for everyone else.
   *
   * @param apiKey the credential
   * @param referer what to be attributed as on OpenRouter's public rankings, where a caller wants that
   * @return the client, holding a transport for as long as the scope; aborts when one cannot be opened
   */
  def openRouter(apiKey: String, referer: Option[String] = None): ZIO[Scope, ChatCompletionError, ChatCompletionClient] =
    transport.map(backend => openRouter(backend, apiKey, referer))

  /**
   * OpenRouter, over a transport the caller holds.
   *
   * @param backend what sends the request
   * @param apiKey the credential
   * @param referer what to be attributed as, where a caller wants that
   * @return the client
   */
  def openRouter(backend: Backend[Task], apiKey: String, referer: Option[String]): ChatCompletionClient =
    val attribution = referer.fold(Map.empty[String, String])(site => Map("HTTP-Referer" -> site))
    HttpChatCompletionClient(backend, OpenRouter, bearer(apiKey) ++ attribution)

  /**
   * OpenAI itself.
   *
   * @param apiKey the credential
   * @param organisation which organisation to bill, where an account has more than one
   * @return the client, holding a transport for as long as the scope; aborts when one cannot be opened
   */
  def openAi(
    apiKey: String,
    organisation: Option[String] = None,
  ): ZIO[Scope, ChatCompletionError, ChatCompletionClient] =
    transport.map(backend => openAi(backend, apiKey, organisation))

  /**
   * OpenAI, over a transport the caller holds.
   *
   * @param backend what sends the request
   * @param apiKey the credential
   * @param organisation which organisation to bill, where an account has more than one
   * @return the client
   */
  def openAi(backend: Backend[Task], apiKey: String, organisation: Option[String]): ChatCompletionClient =
    val billed = organisation.fold(Map.empty[String, String])(org => Map("OpenAI-Organization" -> org))
    HttpChatCompletionClient(backend, OpenAi, bearer(apiKey) ++ billed)

  /**
   * Anything else that serves this protocol — a fast-inference host, or a server running locally.
   *
   * Azure is not one of these: it takes its credential in an `api-key` header and names a deployment in
   * the path, so it needs its own headers rather than a different endpoint.
   *
   * @param endpoint where that provider serves completions
   * @param apiKey the credential, where it wants one
   * @return the client, holding a transport for as long as the scope; aborts when one cannot be opened
   */
  def compatible(
    endpoint: Uri,
    apiKey: Option[String] = None,
  ): ZIO[Scope, ChatCompletionError, ChatCompletionClient] =
    transport.map(backend => compatible(backend, endpoint, apiKey))

  /**
   * Anything else that serves this protocol, over a transport the caller holds.
   *
   * @param backend what sends the request
   * @param endpoint where that provider serves completions
   * @param apiKey the credential, where it wants one
   * @return the client
   */
  def compatible(backend: Backend[Task], endpoint: Uri, apiKey: Option[String]): ChatCompletionClient =
    HttpChatCompletionClient(backend, endpoint, apiKey.fold(Map.empty[String, String])(bearer))

  /**
   * A transport for a caller who has no opinion about one: the JDK's own client, closed with the scope.
   *
   * @return the backend; aborts when one cannot be opened
   */
  private def transport: ZIO[Scope, ChatCompletionError, Backend[Task]] =
    HttpClientZioBackend.scoped().mapError(failure => ChatCompletionError.Unavailable(failure.getMessage))

  /**
   * The credential, as most of them take it.
   *
   * @param apiKey the key
   * @return the header
   */
  private def bearer(apiKey: String): Map[String, String] = Map("Authorization" -> s"Bearer $apiKey")
