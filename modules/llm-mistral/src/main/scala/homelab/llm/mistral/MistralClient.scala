package homelab.llm.mistral


import homelab.common.monitor.Monitor
import homelab.llm.mistral.error.MistralError
import homelab.llm.mistral.request.CompletionRequest
import homelab.llm.mistral.response.CompletionResponse
import sttp.client4.*
import sttp.client4.httpclient.zio.HttpClientZioBackend
import sttp.model.Uri
import zio.json.ast.Json
import zio.{ IO, Scope, Task, ZIO }


/**
 * Mistral's chat-completions call, in Scala types.
 *
 * One method, and nothing withheld: a request carries everything the API takes — a seed, an effort to
 * reason with, a forced tool, a prediction, guardrails — and a response gives back every choice, every
 * chunk of every answer, and the usage as it came.
 *
 * Streaming is deliberately absent: a request here is one call and one answer.
 */
trait MistralClient {

  /**
   * Ask for a completion.
   *
   * @param request what to ask for
   * @param extra fields merged over the request, for an API that has moved since [[request.CompletionRequest]]
   *              last did — a field this type already names belongs in the field
   * @return what the API answered; aborts with what it or the transport refused
   */
  def complete(request: CompletionRequest, extra: Json.Obj = Json.Obj()): IO[MistralError, CompletionResponse]
}


object MistralClient:

  /** Metric and span tag marking calls to a language model. */
  val Tag: (String, String) = "resource" -> "llm"

  /** Where Mistral serves chat completions. */
  val Endpoint: Uri = uri"https://api.mistral.ai/v1/chat/completions"

  /** Where Mistral serves them from the European Union. */
  val Europe: Uri = uri"https://api.eu.mistral.ai/v1/chat/completions"

  /** Where Mistral serves them from the United States. */
  val UnitedStates: Uri = uri"https://api.us.mistral.ai/v1/chat/completions"

  /**
   * Mistral, with a transport of its own.
   *
   * The transport is made here and closed when the scope ends, which is what a caller wants who has no
   * opinion about how to reach it. The overload taking a backend is for everyone else.
   *
   * @param apiKey the credential
   * @param endpoint where to post: [[Endpoint]], or a regional one where data has to stay in a region
   * @param monitor observes each call
   * @return the client, holding a transport for as long as the scope; aborts when one cannot be opened
   */
  def make(
    apiKey: String,
    endpoint: Uri = Endpoint,
    monitor: Monitor = Monitor.Noop,
  ): ZIO[Scope, MistralError, MistralClient] =
    transport.map(backend => make(backend, apiKey, endpoint, monitor))

  /**
   * Mistral, over a transport the caller holds.
   *
   * @param backend what sends the request
   * @param apiKey the credential
   * @param endpoint where to post
   * @param monitor observes each call
   * @return the client
   */
  def make(backend: Backend[Task], apiKey: String, endpoint: Uri, monitor: Monitor): MistralClient =
    HttpMistralClient(backend, apiKey, endpoint, monitor)

  /**
   * A transport for a caller who has no opinion about one: the JDK's own client, closed with the scope.
   *
   * @return the backend; aborts when one cannot be opened
   */
  private def transport: ZIO[Scope, MistralError, Backend[Task]] =
    HttpClientZioBackend.scoped().mapError(failure => MistralError.Unavailable(failure.getMessage))
