package homelab.llm.anthropic


import homelab.llm.Model
import homelab.llm.anthropic.request.MessagesRequest
import homelab.llm.anthropic.response.{ FailureResponse, MessagesResponse }
import sttp.client4.*
import sttp.client4.httpclient.zio.HttpClientZioBackend
import sttp.model.{ StatusCode, Uri }
import zio.json.*
import zio.{ IO, Scope, Task, ZIO }


/**
 * Anthropic's Messages API as a [[Model]]: one POST, one completion out.
 *
 * A different shape from the chat-completions protocol rather than a dialect of it — two roles, the
 * instructions in a field of their own, a tool's answer as a block inside a user turn, and a tool's
 * arguments as an object rather than a string. What makes it fit behind the same port is that all of that
 * is the adapter's work: [[homelab.llm.Message]] says what happened, and how a provider spells it is here.
 *
 * Streaming is deliberately absent: a request here is one call and one answer.
 *
 * @param backend what sends the request
 * @param apiKey the credential, sent in the header this API reads it from
 * @param endpoint where to post, which a proxy may differ on
 * @param version which version of the API to speak, which it requires on every call
 */
final class AnthropicModel(
  backend: Backend[Task],
  apiKey: String,
  endpoint: Uri = AnthropicModel.Endpoint,
  version: String = AnthropicModel.Version,
) extends Model[AnthropicError] {

  /**
   * Send a conversation and read what the model does next.
   *
   * @param model which model to ask for, as Anthropic names it
   * @param request the conversation and the tools on offer
   * @return what the model said, asked for, and consumed; aborts with what the API or the transport refused
   */
  override def complete(model: Model.Name, request: Model.Request): IO[AnthropicError, Model.Completion] =
    for
      response   <- send(MessagesRequest.body(model, request).toJson)
      body       <- read(response)
      completion <- ZIO.fromEither(MessagesResponse.completion(body))
    yield completion

  /**
   * Post one body and get whatever came back.
   *
   * @param body the request body, already rendered
   * @return the response, whatever its status; aborts when the call did not complete
   */
  private def send(body: String): IO[AnthropicError, Response[Either[String, String]]] =
    basicRequest
      .post(endpoint)
      .header("x-api-key", apiKey)
      .header("anthropic-version", version)
      .header("Content-Type", "application/json")
      .body(body)
      .send(backend)
      .mapError(failure => AnthropicError.Unavailable(failure.getMessage))

  /**
   * What a response means.
   *
   * The status is read before the body, because a 4xx and a 2xx do not carry the same shape and a decoder
   * pointed at the wrong one reports the wrong thing.
   *
   * @param response what the API answered
   * @return the decoded body; aborts with what its status and body say together
   */
  private def read(response: Response[Either[String, String]]): IO[AnthropicError, MessagesResponse] =
    response.body match
      case Right(body) => ZIO.fromEither(body.fromJson[MessagesResponse].left.map(unreadable(body)))
      case Left(body)  => ZIO.fail(refused(response.code, body))

  /**
   * What the API refused with.
   *
   * A 401 and a 403 are the credential; a 429 and a 5xx are worth another attempt; anything else is the
   * request itself, and repeating it unchanged will fail the same way.
   *
   * @param status what it answered with
   * @param body what it said, which is usually an error object and sometimes prose
   * @return the failure
   */
  private def refused(status: StatusCode, body: String): AnthropicError =
    val detail = body.fromJson[FailureResponse].map(_.error.message).getOrElse(body.take(200))
    if status == StatusCode.Unauthorized || status == StatusCode.Forbidden then AnthropicError.Refused(detail)
    else if status == StatusCode.TooManyRequests || status.isServerError then AnthropicError.Unavailable(detail)
    else AnthropicError.Rejected(status.code, detail)

  /**
   * What to say about a body that parsed as JSON and is not a completion.
   *
   * @param body what came back
   * @param reason what the decoder reported
   * @return the failure, carrying enough of the body to see what arrived
   */
  private def unreadable(body: String)(reason: String): AnthropicError =
    AnthropicError.Malformed(s"$reason, in ${body.take(200)}")
}


object AnthropicModel:

  /** Where Anthropic serves the Messages API. */
  val Endpoint: Uri = uri"https://api.anthropic.com/v1/messages"

  /** The version this adapter speaks, which every call has to name. */
  val Version: String = "2023-06-01"

  /**
   * A model with a transport of its own: the JDK's client, closed with the scope.
   *
   * @param apiKey the credential
   * @return the model, holding a transport for as long as the scope; aborts when one cannot be opened
   */
  def make(apiKey: String): ZIO[Scope, AnthropicError, AnthropicModel] =
    HttpClientZioBackend
      .scoped()
      .mapError(failure => AnthropicError.Unavailable(failure.getMessage))
      .map(backend => new AnthropicModel(backend, apiKey))
