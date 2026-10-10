package homelab.llm.mistral


import homelab.common.monitor.Monitor
import homelab.llm.mistral.error.MistralError
import homelab.llm.mistral.request.CompletionRequest
import homelab.llm.mistral.response.{ CompletionResponse, FailureResponse }
import sttp.client4.*
import sttp.model.{ StatusCode, Uri }
import zio.json.*
import zio.json.ast.Json
import zio.{ IO, Task, ZIO }


/**
 * A Mistral client over HTTP.
 *
 * @param backend what sends the request
 * @param apiKey the credential, sent as a bearer token
 * @param endpoint where to post, which differs by region
 * @param monitor observes each call, tagged with the model it asked for
 */
final class HttpMistralClient(
  backend: Backend[Task],
  apiKey: String,
  endpoint: Uri,
  monitor: Monitor = Monitor.Noop,
) extends MistralClient {

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
  ): IO[MistralError, CompletionResponse] =
    monitor.measure("MistralClient.complete", MistralClient.Tag, "model" -> request.model):
      send(CompletionRequest.body(request, extra).toJson).flatMap(read)

  /**
   * Post one body and get whatever came back.
   *
   * @param body the request body, already rendered
   * @return the response, whatever its status; aborts when the call did not complete
   */
  private def send(body: String): IO[MistralError, Response[Either[String, String]]] =
    basicRequest
      .post(endpoint)
      .header("Authorization", s"Bearer $apiKey")
      .header("Content-Type", "application/json")
      .header("Accept", "application/json")
      .body(body)
      .send(backend)
      .mapError(failure => MistralError.Unavailable(failure.getMessage))

  /**
   * What a response means.
   *
   * The status is read before the body, because a 4xx and a 2xx do not carry the same shape and a decoder
   * pointed at the wrong one reports the wrong thing.
   *
   * @param response what the API answered
   * @return the decoded body; aborts with what its status and body say together
   */
  private def read(response: Response[Either[String, String]]): IO[MistralError, CompletionResponse] =
    response.body match
      case Right(body) => ZIO.fromEither(body.fromJson[CompletionResponse].left.map(unreadable(body)))
      case Left(body)  => ZIO.fail(refused(response.code, body))

  /**
   * What the API refused with.
   *
   * A 401 is the credential, and so is a 403 the API calls an authentication error; a 429 and a 5xx are
   * worth another attempt; anything else is the request itself, and repeating it unchanged will fail the
   * same way. That includes any other 403, which is what a guardrail answers when it blocks a request.
   *
   * @param status what it answered with
   * @param body what it said, which is usually an error object and sometimes a bare list of problems
   * @return the failure
   */
  private def refused(status: StatusCode, body: String): MistralError =
    val failure = body.fromJson[FailureResponse].toOption
    val detail  = failure.flatMap(_.explanation).getOrElse(body.take(200))
    if status == StatusCode.Unauthorized || credentialRefused(status, failure) then MistralError.Refused(detail)
    else if status == StatusCode.TooManyRequests || status.isServerError then MistralError.Unavailable(detail)
    else MistralError.Rejected(status.code, detail)

  /**
   * Whether a 403 is about the credential rather than about the request.
   *
   * @param status what the API answered with
   * @param failure what it said, where its body read as an error
   * @return true for a 403 whose error the API calls an authentication error
   */
  private def credentialRefused(status: StatusCode, failure: Option[FailureResponse]): Boolean =
    status == StatusCode.Forbidden && failure.flatMap(_.kind).contains("authentication_error")

  /**
   * What to say about a body that parsed as JSON and is not a completion.
   *
   * @param body what came back
   * @param reason what the decoder reported
   * @return the failure, carrying enough of the body to see what arrived
   */
  private def unreadable(body: String)(reason: String): MistralError =
    MistralError.Malformed(s"$reason, in ${body.take(200)}")
}
