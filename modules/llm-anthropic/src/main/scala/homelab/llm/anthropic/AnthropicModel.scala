package homelab.llm.anthropic


import homelab.llm.Model
import homelab.llm.anthropic.error.AnthropicError
import homelab.llm.anthropic.request.{ CompletionRequest, MessageRequest, ToolRequest }
import homelab.llm.anthropic.response.CompletionResponse
import sttp.client4.Backend
import zio.json.ast.Json
import zio.{ IO, Scope, Task, ZIO }


/**
 * Anthropic's Messages API as a [[Model]].
 *
 * The general case over [[AnthropicClient]], which is the whole call. What this does is narrow: a
 * conversation's four roles become the API's two and its instructions become a field, and what the API
 * asks for beyond a conversation is settled once, in the [[AnthropicModel.Config]] this was built with,
 * rather than passed at each call.
 *
 * @param client what the call is made through
 * @param config what every call this makes asks for, beyond the conversation itself
 */
final class AnthropicModel(
  client: AnthropicClient,
  config: AnthropicModel.Config = AnthropicModel.Config(),
) extends Model[AnthropicError] {

  /**
   * Send a conversation and read what the model does next.
   *
   * @param model which model to ask for, as Anthropic names it
   * @param request the conversation and the tools on offer
   * @return what the model said, asked for, and consumed; aborts with what the API or the transport
   *         refused
   */
  override def complete(model: Model.Name, request: Model.Request): IO[AnthropicError, Model.Completion] =
    client.complete(asked(model, request)).flatMap(answer)

  /**
   * A conversation as the API asks for it.
   *
   * Two sources meet here and neither is the other's business: the call brings the model, the conversation
   * and the tools a session permits, and the config brings everything this instance always asks for. The
   * instructions are lifted out of the sequence and the tool results folded into user turns, both by
   * [[MessageRequest.conversation]].
   *
   * @param model which model to ask for
   * @param request the conversation and the tools on offer
   * @return what to ask the client for
   */
  private def asked(model: Model.Name, request: Model.Request): CompletionRequest =
    val (system, messages) = MessageRequest.conversation(request.messages)
    CompletionRequest(
      model = model,
      maxTokens = config.maxTokens,
      messages = messages,
      system = system,
      tools = Option.when(request.tools.nonEmpty)(request.tools.map(ToolRequest.from)),
      toolChoice = config.toolChoice,
      temperature = config.temperature,
      topP = config.topP,
      stopSequences = config.stopSequences,
      thinking = config.thinking,
      metadata = config.metadata,
      extra = config.extra.merge(request.extra),
    )

  /**
   * One completion, out of everything the API answered.
   *
   * @param response what the API answered
   * @return the completion; aborts when the body carries nothing to read
   */
  private def answer(response: CompletionResponse): IO[AnthropicError, Model.Completion] =
    ZIO.fromEither(CompletionResponse.completion(response))
}


object AnthropicModel:

  /**
   * What a run may produce when a caller did not say.
   *
   * A number rather than a refusal, because the API requires one and a toolkit that made every call fail
   * until a caller set it would be trading a working default for a lesson.
   */
  val DefaultMaxTokens: Int = 4096

  /**
   * What an instance asks for on every call, beyond the conversation itself.
   *
   * The API takes more than a conversation implies, and none of it belongs in
   * [[homelab.llm.Model.Request]], which is the general case. Settling it here means a caller chooses once
   * — extended thinking for a reasoning agent, a forced tool for extraction, a ceiling on what any one
   * answer may cost — rather than at each call, and a caller who wants none of it builds the default.
   *
   * The tools are absent deliberately: they come from the session, which decides what a caller may use, so
   * they are not an instance's to fix. And `maxTokens` is the one field with no `None`, because the API
   * will not answer without it.
   *
   * @param maxTokens the most the model may produce, which this API requires
   * @param toolChoice whether and which tool to force, as the API spells it
   * @param temperature how much to let it wander
   * @param topP the nucleus to sample from, an alternative to temperature
   * @param stopSequences what to stop on, beyond the model deciding to
   * @param thinking whether to let it reason first, and for how long, as the API spells it
   * @param metadata what the API records about who this is for
   * @param extra fields every call carries, which [[homelab.llm.Model.Request.extra]] is merged over
   */
  final case class Config(
    maxTokens: Int = DefaultMaxTokens,
    toolChoice: Option[Json] = None,
    temperature: Option[Double] = None,
    topP: Option[Double] = None,
    stopSequences: Option[List[String]] = None,
    thinking: Option[Json] = None,
    metadata: Option[Json] = None,
    extra: Json.Obj = Json.Obj(),
  )

  /**
   * Anthropic, with a transport of its own.
   *
   * @param apiKey the credential
   * @param config what every call asks for, beyond the conversation
   * @return the model, holding a transport for as long as the scope; aborts when one cannot be opened
   */
  def make(apiKey: String, config: Config = Config()): ZIO[Scope, AnthropicError, AnthropicModel] =
    AnthropicClient.make(apiKey).map(AnthropicModel(_, config))

  /**
   * Anthropic, over a transport the caller holds.
   *
   * @param backend what sends the request
   * @param apiKey the credential
   * @param config what every call asks for, beyond the conversation
   * @return the model
   */
  def make(backend: Backend[Task], apiKey: String, config: Config): AnthropicModel =
    AnthropicModel(AnthropicClient.make(backend, apiKey), config)
