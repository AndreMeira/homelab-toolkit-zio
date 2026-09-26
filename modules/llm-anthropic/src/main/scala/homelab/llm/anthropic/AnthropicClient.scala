package homelab.llm.anthropic


import homelab.llm.anthropic.error.AnthropicError
import homelab.llm.anthropic.request.CompletionRequest
import homelab.llm.anthropic.response.CompletionResponse
import sttp.client4.*
import sttp.client4.httpclient.zio.HttpClientZioBackend
import sttp.model.Uri
import zio.json.ast.Json
import zio.{ IO, Scope, Task, ZIO }


/**
 * The Messages call, in Scala types.
 *
 * One method, and nothing withheld: a request carries everything the API takes — the `max_tokens` it
 * requires, a temperature, a forced tool, extended thinking — and a response gives back every block it
 * answered with. What a caller does with that is theirs; [[AnthropicModel]] is one thing to do with it,
 * and holding this instead is how to reach what a port has no field for.
 *
 * Streaming is deliberately absent: a request here is one call and one answer.
 */
trait AnthropicClient {

  /**
   * Ask for a completion.
   *
   * @param request what to ask for
   * @param extra fields merged over the request, for an API that has moved since [[CompletionRequest]]
   *              last did — a field this type already names belongs in the field
   * @return what the API answered; aborts with what it or the transport refused
   */
  def complete(request: CompletionRequest, extra: Json.Obj = Json.Obj()): IO[AnthropicError, CompletionResponse]
}


object AnthropicClient:

  /** Where Anthropic serves the Messages API. */
  val Endpoint: Uri = uri"https://api.anthropic.com/v1/messages"

  /** The version this adapter speaks, which every call has to name. */
  val Version: String = "2023-06-01"

  /**
   * Anthropic, with a transport of its own.
   *
   * The transport is made here and closed when the scope ends, which is what a caller wants who has no
   * opinion about how to reach it. The overload taking a backend is for everyone else.
   *
   * @param apiKey the credential
   * @return the client, holding a transport for as long as the scope; aborts when one cannot be opened
   */
  def make(apiKey: String): ZIO[Scope, AnthropicError, AnthropicClient] =
    transport.map(backend => make(backend, apiKey))

  /**
   * Anthropic, over a transport the caller holds.
   *
   * @param backend what sends the request
   * @param apiKey the credential
   * @return the client
   */
  def make(backend: Backend[Task], apiKey: String): AnthropicClient =
    HttpAnthropicClient(backend, apiKey, Endpoint, Version)

  /**
   * A transport for a caller who has no opinion about one: the JDK's own client, closed with the scope.
   *
   * @return the backend; aborts when one cannot be opened
   */
  private def transport: ZIO[Scope, AnthropicError, Backend[Task]] =
    HttpClientZioBackend.scoped().mapError(failure => AnthropicError.Unavailable(failure.getMessage))
