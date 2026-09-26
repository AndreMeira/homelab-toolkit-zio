package homelab.llm.anthropic


import homelab.common.monitor.Monitor
import homelab.llm.anthropic.error.AnthropicError
import homelab.llm.anthropic.request.CompletionRequest
import homelab.llm.anthropic.response.{ CompletionResponse, FailureResponse }
import sttp.client4.*
import sttp.model.{ StatusCode, Uri }
import zio.json.*
import zio.json.ast.Json
import zio.{ IO, Task, ZIO }


/**
 * A Messages client over HTTP.
 *
 * The credential goes in a header of the API's own naming rather than as a bearer token, and every call
 * names the version it speaks — both of which are why this is its own client rather than a preset on the
 * chat-completions one.
 *
 * @param backend what sends the request
 * @param apiKey the credential, sent in the header this API reads it from
 * @param endpoint where to post, which a proxy may differ on
 * @param monitor observes each call, tagged with the model it asked for
 * @param version which version of the API to speak, which it requires on every call
 */
final class HttpAnthropicClient(
  backend: Backend[Task],
  apiKey: String,
  endpoint: Uri,
  version: String,
  monitor: Monitor = Monitor.Noop,
) extends AnthropicClient {

  /**
   * Ask for a completion.
   *
   * @param request what to ask for
   * @param extra fields merged over the request
   * @return what the API answered; aborts with what it or the transport refused
   */
  override def complete(
    request: CompletionRequest,
    extra: Json.Obj = Json.Obj(),
  ): IO[AnthropicError, CompletionResponse] =
    monitor.measure("AnthropicClient.complete", AnthropicClient.Tag, "model" -> request.model):
      send(CompletionRequest.body(request, extra).toJson).flatMap(read)

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
  private def read(response: Response[Either[String, String]]): IO[AnthropicError, CompletionResponse] =
    response.body match
      case Right(body) => ZIO.fromEither(body.fromJson[CompletionResponse].left.map(unreadable(body)))
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
