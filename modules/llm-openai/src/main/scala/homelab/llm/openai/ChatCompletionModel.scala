package homelab.llm.openai


import homelab.llm.openai.error.ChatCompletionError
import homelab.llm.openai.request.{ CompletionRequest, MessageRequest, ResponseFormat, ToolChoice, ToolRequest }
import homelab.llm.openai.response.CompletionResponse
import homelab.llm.{ Message, Model }
import sttp.model.Uri
import zio.json.ast.Json
import zio.{ IO, Scope, ZIO }


/**
 * A chat-completions endpoint as a [[Model]].
 *
 * The general case over [[ChatCompletionClient]], which is the whole call. A port holds one conversation
 * and answers one completion, so what this does is narrow: several answers become the first, and what the
 * protocol offers beyond a conversation is settled once, in the [[ChatCompletionModel.Config]] this was
 * built with, rather than passed at each call.
 *
 * @param client what the call is made through
 * @param config what every call this makes asks for, beyond the conversation itself
 */
final class ChatCompletionModel(
  client: ChatCompletionClient,
  config: ChatCompletionModel.Config = ChatCompletionModel.Config(),
) extends Model[ChatCompletionError] {

  /**
   * Send a conversation and read what the model does next.
   *
   * @param model which model to ask for, as this provider names it
   * @param request the conversation and the tools on offer
   * @return what the model said, asked for, and cost; aborts with what the provider or the transport
   *         refused
   */
  override def complete(model: Model.Name, request: Model.Request): IO[ChatCompletionError, Model.Completion] =
    client.complete(asked(model, request), config.extra).flatMap(answer)

  /**
   * A conversation as the protocol asks for it.
   *
   * Two sources meet here and neither is the other's business: the call brings the model, the conversation
   * and the tools a session permits, and the config brings everything this instance always asks for.
   *
   * @param model which model to ask for
   * @param request the conversation and the tools on offer
   * @return what to ask the client for
   */
  private def asked(model: Model.Name, request: Model.Request): CompletionRequest = CompletionRequest(
    model = model,
    messages = request.messages.map(MessageRequest.from).toList,
    tools = Option.when(request.tools.nonEmpty)(request.tools.map(ToolRequest.from)),
    toolChoice = config.toolChoice,
    maxTokens = config.maxTokens,
    temperature = config.temperature,
    topP = config.topP,
    stop = config.stop,
    responseFormat = config.responseFormat,
    seed = config.seed,
    user = config.user,
    parallelToolCalls = config.parallelToolCalls,
    frequencyPenalty = config.frequencyPenalty,
    presencePenalty = config.presencePenalty,
    logitBias = config.logitBias,
  )

  /**
   * One completion, out of everything the provider answered.
   *
   * @param response what the provider answered
   * @return the completion; aborts when the body carries no choice to read
   */
  private def answer(response: CompletionResponse): IO[ChatCompletionError, Model.Completion] =
    ZIO.fromEither(CompletionResponse.completion(response))
}


object ChatCompletionModel:

  /**
   * What an instance asks for on every call, beyond the conversation itself.
   *
   * The protocol takes more than a conversation implies, and none of it belongs in
   * [[homelab.llm.Model.Request]], which is the general case. Settling it here means a caller chooses once
   * — a cooler model for classification, a forced tool for extraction, a ceiling on what any one answer
   * may cost — rather than at each call, and a caller who wants none of it builds the default.
   *
   * What is absent is as deliberate. The tools come from the session, which decides what a caller may use,
   * so they are not an instance's to fix. And `n` is not here because a [[homelab.llm.Model.Completion]]
   * holds one answer: an instance asking for three would pay for three and discard two, every call. A
   * caller who wants alternatives holds [[ChatCompletionClient]], where `n` is a field of the request.
   *
   * @param toolChoice whether the model may call a tool, must call one, or must call a named one
   * @param maxTokens the most the model may produce
   * @param temperature how much to let it wander
   * @param topP the nucleus to sample from, an alternative to temperature
   * @param stop what to stop on, beyond the model deciding to
   * @param responseFormat what shape the answer must take
   * @param seed what to seed sampling with, where a provider offers repeatability
   * @param user who this is on behalf of, which some providers use for abuse signals
   * @param parallelToolCalls whether it may ask for several tools in one turn
   * @param frequencyPenalty how much to discourage a token for having appeared often
   * @param presencePenalty how much to discourage a token for having appeared at all
   * @param logitBias what to make more or less likely, by token
   * @param extra fields merged over every call, for a protocol that has moved since
   *              [[homelab.llm.openai.request.CompletionRequest]] last did; a field that type names
   *              belongs in the field
   */
  final case class Config(
    toolChoice: Option[ToolChoice] = None,
    maxTokens: Option[Int] = None,
    temperature: Option[Double] = None,
    topP: Option[Double] = None,
    stop: Option[List[String]] = None,
    responseFormat: Option[ResponseFormat] = None,
    seed: Option[Int] = None,
    user: Option[String] = None,
    parallelToolCalls: Option[Boolean] = None,
    frequencyPenalty: Option[Double] = None,
    presencePenalty: Option[Double] = None,
    logitBias: Option[Map[String, Int]] = None,
    extra: Json.Obj = Json.Obj(),
  )

  /**
   * OpenRouter, with a transport of its own.
   *
   * @param apiKey the credential
   * @param referer what to be attributed as on OpenRouter's rankings, where a caller wants that
   * @param config what every call asks for, beyond the conversation
   * @return the model, holding a transport for as long as the scope; aborts when one cannot be opened
   */
  def openRouter(
    apiKey: String,
    referer: Option[String] = None,
    config: Config = Config(),
  ): ZIO[Scope, ChatCompletionError, ChatCompletionModel] =
    ChatCompletionClient.openRouter(apiKey, referer).map(ChatCompletionModel(_, config))

  /**
   * OpenAI itself, with a transport of its own.
   *
   * @param apiKey the credential
   * @param organisation which organisation to bill, where an account has more than one
   * @param config what every call asks for, beyond the conversation
   * @return the model, holding a transport for as long as the scope; aborts when one cannot be opened
   */
  def openAi(
    apiKey: String,
    organisation: Option[String] = None,
    config: Config = Config(),
  ): ZIO[Scope, ChatCompletionError, ChatCompletionModel] =
    ChatCompletionClient.openAi(apiKey, organisation).map(ChatCompletionModel(_, config))

  /**
   * Anything else that serves this protocol, with a transport of its own.
   *
   * @param endpoint where that provider serves completions
   * @param apiKey the credential, where it wants one
   * @param config what every call asks for, beyond the conversation
   * @return the model, holding a transport for as long as the scope; aborts when one cannot be opened
   */
  def compatible(
    endpoint: Uri,
    apiKey: Option[String] = None,
    config: Config = Config(),
  ): ZIO[Scope, ChatCompletionError, ChatCompletionModel] =
    ChatCompletionClient.compatible(endpoint, apiKey).map(ChatCompletionModel(_, config))
