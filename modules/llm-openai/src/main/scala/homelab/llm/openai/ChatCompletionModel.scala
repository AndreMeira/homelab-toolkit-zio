package homelab.llm.openai


import homelab.llm.Model
import homelab.llm.openai.request.CompletionRequest
import homelab.llm.openai.response.{ CompletionResponse, FailureResponse }
import sttp.client4.*
import sttp.model.{ StatusCode, Uri }
import zio.json.*
import zio.{ IO, Task, ZIO }


/**
 * A chat-completions endpoint as a [[Model]]: one POST, one completion out.
 *
 * The protocol OpenAI defined and many now serve — the gateways, the fast-inference hosts, and the servers
 * people run locally. What differs between them is where to post and what to send with it, which is why
 * those are constructor arguments and the companion holds a preset per provider rather than a subclass.
 *
 * Streaming is deliberately absent: a request here is one call and one answer.
 *
 * @param backend what sends the request
 * @param endpoint where to post
 * @param headers what to send with it — the credential, and whatever else a provider attributes a call by
 */
final class ChatCompletionModel(
  backend: Backend[Task],
  endpoint: Uri,
  headers: Map[String, String],
) extends Model[ChatCompletionError] {

  /**
   * Send a conversation and read what the model does next.
   *
   * @param model which model to ask for, as this provider names it
   * @param request the conversation and the tools on offer
   * @return what the model said, asked for, and cost; aborts with what the provider or the transport refused
   */
  override def complete(model: Model.Name, request: Model.Request): IO[ChatCompletionError, Model.Completion] =
    for
      response   <- send(CompletionRequest.body(model, request).toJson)
      body       <- read(response)
      completion <- ZIO.fromEither(CompletionResponse.completion(body))
    yield completion

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


object ChatCompletionModel:

  /** Where OpenRouter serves chat completions. */
  val OpenRouter: Uri = uri"https://openrouter.ai/api/v1/chat/completions"

  /** Where OpenAI serves them. */
  val OpenAi: Uri = uri"https://api.openai.com/v1/chat/completions"

  /**
   * OpenRouter, which fronts many providers and reports what a call cost.
   *
   * The referer is what it attributes a call to on its public rankings, and is the one field of
   * [[CompletionResponse.Usage]] that no direct provider fills in.
   *
   * @param backend what sends the request
   * @param apiKey the credential
   * @param referer what to be attributed as, where a caller wants that
   * @return the model
   */
  def openRouter(backend: Backend[Task], apiKey: String, referer: Option[String] = None): ChatCompletionModel =
    val attribution = referer.fold(Map.empty[String, String])(site => Map("HTTP-Referer" -> site))
    new ChatCompletionModel(backend, OpenRouter, bearer(apiKey) ++ attribution)

  /**
   * OpenAI itself.
   *
   * @param backend what sends the request
   * @param apiKey the credential
   * @param organisation which organisation to bill, where an account has more than one
   * @return the model
   */
  def openAi(backend: Backend[Task], apiKey: String, organisation: Option[String] = None): ChatCompletionModel =
    val billed = organisation.fold(Map.empty[String, String])(org => Map("OpenAI-Organization" -> org))
    new ChatCompletionModel(backend, OpenAi, bearer(apiKey) ++ billed)

  /**
   * Anything else that serves this protocol — a fast-inference host, or a server running locally.
   *
   * Azure is not one of these: it takes its credential in an `api-key` header and names a deployment in the
   * path, so it needs its own headers rather than a different endpoint.
   *
   * @param backend what sends the request
   * @param endpoint where that provider serves completions
   * @param apiKey the credential, where it wants one
   * @return the model
   */
  def compatible(backend: Backend[Task], endpoint: Uri, apiKey: Option[String] = None): ChatCompletionModel =
    new ChatCompletionModel(backend, endpoint, apiKey.fold(Map.empty[String, String])(bearer))

  /**
   * The credential, as most of them take it.
   *
   * @param apiKey the key
   * @return the header
   */
  private def bearer(apiKey: String): Map[String, String] = Map("Authorization" -> s"Bearer $apiKey")
