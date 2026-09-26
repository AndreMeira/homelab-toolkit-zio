package homelab.llm.openai


import homelab.llm.openai.error.ChatCompletionError
import homelab.llm.openai.request.CompletionRequest
import homelab.llm.openai.response.{ CompletionResponse, FailureResponse }
import sttp.client4.*
import sttp.model.{ StatusCode, Uri }
import zio.json.*
import zio.json.ast.Json
import zio.{ IO, Task, ZIO }


/**
 * A chat-completions client over HTTP.
 *
 * What differs between providers is where to post and what to send with it, which is why those are
 * arguments and the presets that fill them in live on [[ChatCompletionClient]].
 *
 * @param backend what sends the request
 * @param endpoint where to post
 * @param headers what to send with it — the credential, and whatever else a provider attributes a call by
 */
final class HttpChatCompletionClient(
  backend: Backend[Task],
  endpoint: Uri,
  headers: Map[String, String],
) extends ChatCompletionClient {

  /**
   * Ask for a completion.
   *
   * @param request what to ask for
   * @param extra fields merged over the request
   * @return what the provider answered; aborts with what it or the transport refused
   */
  override def complete(
    request: CompletionRequest,
    extra: Json.Obj = Json.Obj(),
  ): IO[ChatCompletionError, CompletionResponse] =
    send(CompletionRequest.body(request, extra).toJson).flatMap(read)

  /**
   * Post one body and get whatever came back.
   *
   * @param body the request body, already rendered
   * @return the response, whatever its status; aborts when the call did not complete
   */
  private def send(body: String): IO[ChatCompletionError, Response[Either[String, String]]] =
    basicRequest
      .post(endpoint)
      .headers(headers)
      .header("Content-Type", "application/json")
      .body(body)
      .send(backend)
      .mapError(failure => ChatCompletionError.Unavailable(failure.getMessage))

  /**
   * What a response means.
   *
   * The status is read before the body, because a 4xx and a 2xx do not carry the same shape and a decoder
   * pointed at the wrong one reports the wrong thing.
   *
   * @param response what the provider answered
   * @return the decoded body; aborts with what its status and body say together
   */
  private def read(response: Response[Either[String, String]]): IO[ChatCompletionError, CompletionResponse] =
    response.body match
      case Right(body) => ZIO.fromEither(body.fromJson[CompletionResponse].left.map(unreadable(body)))
      case Left(body)  => ZIO.fail(refused(response.code, body))

  /**
   * What the provider refused with.
   *
   * A 401 and a 403 are the credential; a 429 and a 5xx are worth another attempt; anything else is the
   * request itself, and repeating it unchanged will fail the same way.
   *
   * @param status what it answered with
   * @param body what it said, which is usually an error object and sometimes prose
   * @return the failure
   */
  private def refused(status: StatusCode, body: String): ChatCompletionError =
    val detail = body.fromJson[FailureResponse].map(_.error.message).getOrElse(body.take(200))
    if status == StatusCode.Unauthorized || status == StatusCode.Forbidden then ChatCompletionError.Refused(detail)
    else if status == StatusCode.TooManyRequests || status.isServerError then ChatCompletionError.Unavailable(detail)
    else ChatCompletionError.Rejected(status.code, detail)

  /**
   * What to say about a body that parsed as JSON and is not a completion.
   *
   * @param body what came back
   * @param reason what the decoder reported
   * @return the failure, carrying enough of the body to see what arrived
   */
  private def unreadable(body: String)(reason: String): ChatCompletionError =
    ChatCompletionError.Malformed(s"$reason, in ${body.take(200)}")
}
