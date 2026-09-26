package homelab.llm.anthropic.request


import zio.json.*
import zio.json.ast.Json


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
 * @param model which model to ask for, as Anthropic names it
 * @param maxTokens the most it may produce, which this API will not do without
 * @param messages the conversation, oldest first, in the two roles this API has
 * @param system what the model is told before the conversation
 * @param tools the tools on offer
 * @param toolChoice whether and which tool to force, as the API spells it
 * @param temperature how much to let it wander
 * @param topP the nucleus to sample from, an alternative to temperature
 * @param stopSequences what to stop on, beyond the model deciding to
 * @param thinking whether to let it reason first, and for how long, as the API spells it
 * @param metadata what the API records about who this is for
 * @param extra fields merged over the rest, for what this type does not name
 */
@jsonMemberNames(SnakeCase)
final case class CompletionRequest(
  model: String,
  maxTokens: Int,
  messages: List[MessageRequest],
  system: Option[String] = None,
  tools: Option[List[ToolRequest]] = None,
  toolChoice: Option[Json] = None,
  temperature: Option[Double] = None,
  topP: Option[Double] = None,
  stopSequences: Option[List[String]] = None,
  thinking: Option[Json] = None,
  metadata: Option[Json] = None,
  @jsonExclude extra: Json.Obj = Json.Obj(),
) derives JsonEncoder


object CompletionRequest:

  /**
   * The body to post.
   *
   * [[CompletionRequest.extra]] is merged last and wins, which is what makes it an escape hatch rather
   * than a field: a `max_tokens` a caller chose replaces the one asked for here, and anything the API
   * takes that this does not name needs no place of its own.
   *
   * @param request what to ask for
   * @return the object to send
   */
  def body(request: CompletionRequest): Json.Obj = request.toJsonAST.toOption match
    case Some(encoded: Json.Obj) => encoded.merge(request.extra)
    case _                       => request.extra
