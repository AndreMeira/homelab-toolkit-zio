package homelab.llm.openai.request


import homelab.llm.Model
import homelab.llm.openai.ChatCompletionModel
import zio.json.*
import zio.json.ast.Json


/**
 * A chat-completions request, as the protocol defines it.
 *
 * What the API takes, not what a port can carry: a caller holding this can ask for several answers, cap
 * what the model produces, force a particular tool, or fix a seed — none of which [[homelab.llm.Model]]
 * has a field for, and all of which are lost if the only way to reach the provider is through it.
 *
 * Everything but the first two is optional and omitted when absent, because a provider offered a null
 * where it expected nothing may answer differently from one offered nothing at all.
 *
 * A field the protocol has and this does not is a field to add here. Until it is,
 * [[ChatCompletionClient.complete]] takes what to merge over this, which is a hedge against the protocol
 * moving rather than a place to put what is already known.
 *
 * @param model which model to ask for, as this provider names it
 * @param messages the conversation, oldest first
 * @param tools the tools on offer
 * @param toolChoice whether the model may call a tool, must call one, or must call a named one
 * @param maxTokens the most the model may produce
 * @param temperature how much to let it wander
 * @param topP the nucleus to sample from, an alternative to temperature
 * @param n how many answers to produce
 * @param stop what to stop on, beyond the model deciding to
 * @param responseFormat what shape the answer must take
 * @param seed what to seed sampling with, where a provider offers repeatability
 * @param user who this is on behalf of, which some providers use for abuse signals
 * @param parallelToolCalls whether it may ask for several tools in one turn, which tools that write may
 *                          want off
 * @param frequencyPenalty how much to discourage a token for having appeared often
 * @param presencePenalty how much to discourage a token for having appeared at all
 * @param logitBias what to make more or less likely, by token
 */
@jsonMemberNames(SnakeCase)
final case class CompletionRequest(
  model: String,
  messages: List[MessageRequest],
  tools: Option[List[ToolRequest]] = None,
  toolChoice: Option[ToolChoice] = None,
  maxTokens: Option[Int] = None,
  temperature: Option[Double] = None,
  topP: Option[Double] = None,
  n: Option[Int] = None,
  stop: Option[List[String]] = None,
  responseFormat: Option[ResponseFormat] = None,
  seed: Option[Int] = None,
  user: Option[String] = None,
  parallelToolCalls: Option[Boolean] = None,
  frequencyPenalty: Option[Double] = None,
  presencePenalty: Option[Double] = None,
  logitBias: Option[Map[String, Int]] = None,
) derives JsonEncoder


object CompletionRequest:

  /**
   * A conversation as the protocol asks for it.
   *
   * Two sources meet here and neither is the other's business: the call brings the model, the conversation
   * and the tools a session permits, and the config brings everything an instance always asks for.
   *
   * @param model which model to ask for
   * @param request the conversation and the tools on offer
   * @param config what the instance asking always asks for
   * @return what to send
   */
  def from(model: Model.Name, request: Model.Request, config: ChatCompletionModel.Config): CompletionRequest =
    CompletionRequest(
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
   * The body to post, with whatever a caller adds merged over it.
   *
   * The second argument is a hedge against the protocol moving: a field shipped last week has somewhere to
   * go before this type has one for it. A field this type already names belongs in the field.
   *
   * @param request what to ask for
   * @param extra fields merged over it, which win where they collide
   * @return the object to send
   */
  def body(request: CompletionRequest, extra: Json.Obj = Json.Obj()): Json.Obj =
    request.toJsonAST.toOption match
      case Some(encoded: Json.Obj) => encoded.merge(extra)
      case _                       => extra
