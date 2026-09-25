package homelab.llm.anthropic.request


import homelab.llm.Model
import zio.json.*
import zio.json.ast.Json


/**
 * The Messages request, as Anthropic receives it.
 *
 * `max_tokens` is required by the API and is not something [[Model.Request]] carries, so it is settled
 * here: a caller that has an opinion says so through `extra`, which is merged last, and one that does not
 * gets [[MessagesRequest.DefaultMaxTokens]] rather than a rejected call.
 *
 * @param model which model to ask for
 * @param maxTokens the most it may produce, which this API will not do without
 * @param messages the conversation, oldest first, in the two roles this API has
 * @param system what the model is told before the conversation, absent when it was told nothing
 * @param tools the tools on offer, absent when there are none
 */
@jsonMemberNames(SnakeCase)
final case class MessagesRequest(
  model: String,
  maxTokens: Int,
  messages: List[MessageRequest],
  system: Option[String],
  tools: Option[List[ToolRequest]],
) derives JsonEncoder


object MessagesRequest:

  /**
   * What a run may produce when a caller did not say.
   *
   * A number rather than a refusal, because the API requires one and a toolkit that made every call fail
   * until a caller set it would be trading a working default for a lesson.
   */
  val DefaultMaxTokens: Int = 4096

  /**
   * What to post for one call.
   *
   * A caller's own fields are merged last and win — `max_tokens`, `temperature`, `thinking`, `tool_choice`,
   * a stop sequence — so what this type does not model is still reachable.
   *
   * @param model which model to ask for
   * @param request the conversation and the tools on offer
   * @return the body, as the object to send
   */
  def body(model: Model.Name, request: Model.Request): Json.Obj =
    val (system, messages) = MessageRequest.conversation(request.messages)
    val core               = MessagesRequest(
      model = model,
      maxTokens = DefaultMaxTokens,
      messages = messages,
      system = system,
      tools = Option.when(request.tools.nonEmpty)(request.tools.map(ToolRequest.from)),
    )
    merged(core.toJsonAST.toOption, request.extra)

  /**
   * The body with a caller's own fields over it.
   *
   * @param encoded what this type produced, absent only if it could not be written at all
   * @param extra what the caller added
   * @return the object to send
   */
  private def merged(encoded: Option[Json], extra: Json.Obj): Json.Obj = encoded match
    case Some(body: Json.Obj) => body.merge(extra)
    case _                    => extra
