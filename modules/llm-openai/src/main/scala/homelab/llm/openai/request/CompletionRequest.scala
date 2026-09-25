package homelab.llm.openai.request


import homelab.llm.{ Model, Tool }
import zio.json.*
import zio.json.ast.Json


/**
 * The chat-completions request, as OpenRouter receives it.
 *
 * The tools are carried as the objects they already are: a session advertises them provider-shaped, and
 * this passes them through rather than describing them a second time. They are absent when there are none,
 * because a provider offered an empty array may answer that it was given no tools.
 *
 * @param model which model to ask for
 * @param messages the conversation, oldest first
 * @param tools the tools on offer, absent when there are none
 */
final case class CompletionRequest(
  model: String,
  messages: List[MessageRequest],
  tools: Option[List[Json]],
) derives JsonEncoder


object CompletionRequest:

  /**
   * What to post for one call.
   *
   * A caller's own fields are merged last and win, which is what makes [[Model.Request.extra]] useful: a
   * temperature, a provider preference or a forced `tool_choice` needs no field here, and one that replaces
   * something here is a caller overriding it deliberately.
   *
   * @param model which model to ask for
   * @param request the conversation and the tools on offer
   * @return the body, as the object to send
   */
  def body(model: Model.Name, request: Model.Request): Json.Obj =
    val core = CompletionRequest(
      model = model,
      messages = request.messages.map(MessageRequest.from).toList,
      tools = Option.when(request.tools.nonEmpty)(request.tools),
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
