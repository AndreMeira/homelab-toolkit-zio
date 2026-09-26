package homelab.llm.anthropic.request


import homelab.llm.Model
import homelab.llm.anthropic.AnthropicModel
import zio.json.*
import zio.json.ast.Json
import zio.Chunk


/**
 * A request, as Anthropic's Messages API takes it.
 *
 * What the API takes, not what a port can carry: `max_tokens` is required here and has no field on
 * [[homelab.llm.Model.Request]], and a caller reaching this directly can also set a temperature, force a
 * tool, ask for extended thinking, or stop on a sequence — none of which a conversation implies.
 *
 * The instructions are a field rather than a turn, and the messages have two roles: see
 * [[MessageRequest.conversation]], which is what turns a toolkit conversation into both.
 *
 * A field the API has and this does not is a field to add here. Until it is, [[AnthropicClient.complete]]
 * takes what to merge over this, which is a hedge against the API moving rather than a place to put what
 * is already known.
 *
 * @param model which model to ask for, as Anthropic names it
 * @param maxTokens the most it may produce, which this API will not do without
 * @param messages the conversation, oldest first, in the two roles this API has
 * @param system what the model is told before the conversation
 * @param tools the tools on offer
 * @param toolChoice whether the model may call a tool, must call one, or must call a named one
 * @param temperature how much to let it wander
 * @param topP the nucleus to sample from, an alternative to temperature
 * @param stopSequences what to stop on, beyond the model deciding to
 * @param thinking whether it reasons before it answers, and how much room it has to
 * @param topK how many of the likeliest tokens to sample from
 * @param metadata what the API records about who this is for
 */
@jsonMemberNames(SnakeCase)
final case class CompletionRequest(
  model: String,
  maxTokens: Int,
  messages: Chunk[MessageRequest],
  system: Option[String] = None,
  tools: Option[Chunk[ToolRequest]] = None,
  toolChoice: Option[ToolChoice] = None,
  temperature: Option[Double] = None,
  topP: Option[Double] = None,
  stopSequences: Option[Chunk[String]] = None,
  topK: Option[Int] = None,
  thinking: Option[Thinking] = None,
  metadata: Option[Metadata] = None,
) derives JsonEncoder


object CompletionRequest:

  /**
   * A conversation as the API asks for it.
   *
   * Two sources meet here and neither is the other's business: the call brings the model, the conversation
   * and the tools a session permits, and the config brings everything an instance always asks for. The
   * instructions are lifted out of the sequence and the tool results folded into user turns, both by
   * [[MessageRequest.conversation]].
   *
   * @param model which model to ask for
   * @param request the conversation and the tools on offer
   * @param config what the instance asking always asks for
   * @return what to send
   */
  def from(model: Model.Name, request: Model.Request, config: AnthropicModel.Config): CompletionRequest =
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
      topK = config.topK,
      thinking = config.thinking,
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
