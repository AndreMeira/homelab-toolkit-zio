package homelab.llm.mistral


import homelab.common.monitor.Monitor
import homelab.llm.Model
import homelab.llm.mistral.error.MistralError
import homelab.llm.mistral.request.*
import homelab.llm.mistral.response.CompletionResponse
import sttp.client4.Backend
import sttp.model.Uri
import zio.json.ast.Json
import zio.{ Chunk, IO, Scope, Task, ZIO }


/**
 * Mistral's chat completions as a `Model`.
 *
 * The general case over [[MistralClient]], which is the whole call. What this does is narrow: several
 * answers become the first, a tool-call id another provider minted goes out in the form Mistral takes, and
 * what the API offers beyond a conversation is settled once, in the [[MistralModel.Config]] this was built
 * with, rather than passed at each call.
 *
 * @param client what the call is made through
 * @param config what every call this makes asks for, beyond the conversation itself
 */
final class MistralModel(
  client: MistralClient,
  config: MistralModel.Config = MistralModel.Config(),
) extends Model[MistralError] {

  /**
   * Send a conversation and read what the model does next.
   *
   * @param model which model to ask for, as Mistral names it
   * @param request the conversation and the tools on offer
   * @return what the model said, asked for, and consumed; aborts with what the API or the transport refused,
   *         which includes a conversation whose messages come in an order the API does not accept
   */
  override def complete(model: Model.Name, request: Model.Request): IO[MistralError, Model.Completion] =
    client.complete(CompletionRequest.from(model, request, config), config.extra).flatMap(answer)

  /**
   * One completion, out of everything the API answered.
   *
   * @param response what the API answered
   * @return the completion; aborts when the body carries no choice to read
   */
  private def answer(response: CompletionResponse): IO[MistralError, Model.Completion] =
    ZIO.fromEither(CompletionResponse.completion(response))
}


object MistralModel:

  /**
   * What an instance asks for on every call, beyond the conversation itself.
   *
   * Settled once, so a caller chooses a cooler model for classification, a forced tool for extraction, or
   * how hard a reasoning model thinks, and a caller who wants none of it builds the default. The tools come
   * from the session instead, which decides what a caller may use. `n` and `prediction` are on the client:
   * a completion holds one answer, and a prediction belongs to one conversation rather than to an instance.
   *
   * @param toolChoice whether the model may call a tool, must call one, or must call a named one
   * @param maxTokens the most the model may produce
   * @param temperature how much to let it wander, from 0 to 1.5
   * @param topP the nucleus to sample from, an alternative to temperature
   * @param stop what to stop on, beyond the model deciding to
   * @param randomSeed what to seed sampling with, for answers that repeat
   * @param responseFormat what shape the answer must take
   * @param presencePenalty how much to discourage a token for having appeared at all
   * @param frequencyPenalty how much to discourage a token for having appeared often
   * @param parallelToolCalls whether it may ask for several tools in one turn
   * @param reasoningEffort how much it reasons first, on a model that reasons
   * @param promptMode whether Mistral puts its system prompt for reasoning in front of the conversation
   * @param promptCacheKey what to group requests by for prompt caching
   * @param serviceTier whether it may be served from priority capacity
   * @param guardrails the moderation to run on every call, as the API's guardrail objects
   * @param safePrompt whether to put Mistral's safety prompt in front of the conversation
   * @param metadata what to record against every call
   * @param extra fields merged over every call, for an API that has moved since
   *              [[homelab.llm.mistral.request.CompletionRequest]] last did; a field that type names belongs
   *              in the field
   */
  final case class Config(
    toolChoice: Option[ToolChoice] = None,
    maxTokens: Option[Int] = None,
    temperature: Option[Double] = None,
    topP: Option[Double] = None,
    stop: Option[Chunk[String]] = None,
    randomSeed: Option[Int] = None,
    responseFormat: Option[ResponseFormat] = None,
    presencePenalty: Option[Double] = None,
    frequencyPenalty: Option[Double] = None,
    parallelToolCalls: Option[Boolean] = None,
    reasoningEffort: Option[ReasoningEffort] = None,
    promptMode: Option[PromptMode] = None,
    promptCacheKey: Option[String] = None,
    serviceTier: Option[ServiceTier] = None,
    guardrails: Option[Chunk[Json]] = None,
    safePrompt: Option[Boolean] = None,
    metadata: Option[Json.Obj] = None,
    extra: Json.Obj = Json.Obj(),
  )

  /**
   * Mistral, with a transport of its own.
   *
   * @param apiKey the credential
   * @param endpoint where to post: [[MistralClient.Endpoint]], or a regional one
   * @param config what every call asks for, beyond the conversation
   * @param monitor observes each call
   * @return the model, holding a transport for as long as the scope; aborts when one cannot be opened
   */
  def make(
    apiKey: String,
    endpoint: Uri = MistralClient.Endpoint,
    config: Config = Config(),
    monitor: Monitor = Monitor.Noop,
  ): ZIO[Scope, MistralError, MistralModel] =
    MistralClient.make(apiKey, endpoint, monitor).map(MistralModel(_, config))

  /**
   * Mistral, over a transport the caller holds.
   *
   * @param backend what sends the request
   * @param apiKey the credential
   * @param endpoint where to post
   * @param config what every call asks for, beyond the conversation
   * @param monitor observes each call
   * @return the model
   */
  def make(backend: Backend[Task], apiKey: String, endpoint: Uri, config: Config, monitor: Monitor): MistralModel =
    MistralModel(MistralClient.make(backend, apiKey, endpoint, monitor), config)
