package homelab.llm.mistral.request


import homelab.llm.Model
import homelab.llm.mistral.MistralModel
import zio.Chunk
import zio.json.*
import zio.json.ast.Json


/**
 * A chat-completions request, as Mistral's API takes it.
 *
 * Every field the API names and none it does not, since its schema refuses an unknown field at any depth
 * with a 422. Everything but the first two is optional and omitted when absent. Streaming is not asked
 * for: a request here is one call and one answer.
 *
 * A field the API has and this does not is a field to add here. Until it is, [[homelab.llm.mistral.MistralClient.complete]]
 * takes what to merge over this, which is a hedge against the API moving rather than a place to put what is
 * already known.
 *
 * @param model which model to ask for, as Mistral names it
 * @param messages the conversation, oldest first
 * @param tools the tools on offer, at most 128
 * @param toolChoice whether the model may call a tool, must call one, or must call a named one
 * @param maxTokens the most the model may produce, which with the conversation must fit its context
 * @param temperature how much to let it wander, from 0 to 1.5
 * @param topP the nucleus to sample from, an alternative to temperature
 * @param n how many answers to produce, the conversation being charged once
 * @param stop what to stop on, beyond the model deciding to, and which the answer does not include
 * @param randomSeed what to seed sampling with, for answers that repeat
 * @param responseFormat what shape the answer must take
 * @param presencePenalty how much to discourage a token for having appeared at all
 * @param frequencyPenalty how much to discourage a token for having appeared often
 * @param minTokens the least the model may produce
 * @param repetitionPenalty how much to discourage repeating what has already been said
 * @param topK how many of the likeliest tokens to sample from
 * @param logprobs whether to report how likely each token of the answer was
 * @param topLogprobs how many of the likeliest tokens to report at each position of the answer
 * @param promptLogprobs whether to report how likely each token of the prompt was
 * @param topPromptLogprobs how many of the likeliest tokens to report at each position of the prompt
 * @param parallelToolCalls whether it may ask for several tools in one turn
 * @param prediction what the answer is expected to contain, for an answer that edits it
 * @param reasoningEffort how much it reasons first, on a model that reasons
 * @param promptMode whether Mistral puts its system prompt for reasoning in front of the conversation
 * @param promptCacheKey what to group requests by for prompt caching, where a caller chooses
 * @param serviceTier whether it may be served from priority capacity
 * @param guardrails the moderation to run on the call, as the API's guardrail objects
 * @param safePrompt whether to put Mistral's safety prompt in front of the conversation
 * @param metadata what to record against the call
 */
@jsonMemberNames(SnakeCase)
final case class CompletionRequest(
  model: String,
  messages: Chunk[MessageRequest],
  tools: Option[Chunk[ToolRequest]] = None,
  toolChoice: Option[ToolChoice] = None,
  maxTokens: Option[Int] = None,
  temperature: Option[Double] = None,
  topP: Option[Double] = None,
  n: Option[Int] = None,
  stop: Option[Chunk[String]] = None,
  randomSeed: Option[Int] = None,
  responseFormat: Option[ResponseFormat] = None,
  presencePenalty: Option[Double] = None,
  frequencyPenalty: Option[Double] = None,
  minTokens: Option[Int] = None,
  repetitionPenalty: Option[Double] = None,
  topK: Option[Int] = None,
  logprobs: Option[Boolean] = None,
  topLogprobs: Option[Int] = None,
  promptLogprobs: Option[Boolean] = None,
  topPromptLogprobs: Option[Int] = None,
  parallelToolCalls: Option[Boolean] = None,
  prediction: Option[Prediction] = None,
  reasoningEffort: Option[ReasoningEffort] = None,
  promptMode: Option[PromptMode] = None,
  promptCacheKey: Option[String] = None,
  serviceTier: Option[ServiceTier] = None,
  guardrails: Option[Chunk[Json]] = None,
  safePrompt: Option[Boolean] = None,
  metadata: Option[Json.Obj] = None,
) derives JsonEncoder


object CompletionRequest:

  /**
   * A conversation as the API asks for it.
   *
   * Two sources meet here and neither is the other's business: the call brings the model, the conversation
   * and the tools a session permits, and the config brings everything an instance always asks for.
   *
   * @param model which model to ask for
   * @param request the conversation and the tools on offer
   * @param config what the instance asking always asks for
   * @return what to send
   */
  def from(model: Model.Name, request: Model.Request, config: MistralModel.Config): CompletionRequest =
    CompletionRequest(
      model = model,
      messages = request.messages.map(MessageRequest.from),
      tools = Option.when(request.tools.nonEmpty)(request.tools.map(ToolRequest.from)),
      toolChoice = config.toolChoice,
      maxTokens = config.maxTokens,
      temperature = config.temperature,
      topP = config.topP,
      stop = config.stop,
      randomSeed = config.randomSeed,
      responseFormat = config.responseFormat,
      presencePenalty = config.presencePenalty,
      frequencyPenalty = config.frequencyPenalty,
      minTokens = config.minTokens,
      repetitionPenalty = config.repetitionPenalty,
      topK = config.topK,
      parallelToolCalls = config.parallelToolCalls,
      reasoningEffort = config.reasoningEffort,
      promptMode = config.promptMode,
      promptCacheKey = config.promptCacheKey,
      serviceTier = config.serviceTier,
      guardrails = config.guardrails,
      safePrompt = config.safePrompt,
      metadata = config.metadata,
    )

  /**
   * The body to post, with whatever a caller adds merged over it.
   *
   * The second argument is a hedge against the API moving: a field shipped last week has somewhere to go
   * before this type has one for it. A field this type already names belongs in the field.
   *
   * @param request what to ask for
   * @param extra fields merged over it, which win where they collide
   * @return the object to send
   */
  def body(request: CompletionRequest, extra: Json.Obj = Json.Obj()): Json.Obj =
    request.toJsonAST.toOption match
      case Some(encoded: Json.Obj) => encoded.merge(extra)
      case _                       => extra
