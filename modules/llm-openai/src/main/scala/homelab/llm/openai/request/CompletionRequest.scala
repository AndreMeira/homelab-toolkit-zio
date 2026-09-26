package homelab.llm.openai.request


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
 * @param model which model to ask for, as this provider names it
 * @param messages the conversation, oldest first
 * @param tools the tools on offer
 * @param toolChoice whether and which tool to force, as the provider spells it
 * @param maxTokens the most the model may produce
 * @param temperature how much to let it wander
 * @param topP the nucleus to sample from, an alternative to temperature
 * @param n how many answers to produce
 * @param stop what to stop on, beyond the model deciding to
 * @param responseFormat what shape the answer must take, as the provider spells it
 * @param seed what to seed sampling with, where a provider offers repeatability
 * @param user who this is on behalf of, which some providers use for abuse signals
 * @param extra fields merged over the rest, for what this type does not name
 */
@jsonMemberNames(SnakeCase)
final case class CompletionRequest(
  model: String,
  messages: List[MessageRequest],
  tools: Option[List[ToolRequest]] = None,
  toolChoice: Option[Json] = None,
  maxTokens: Option[Int] = None,
  temperature: Option[Double] = None,
  topP: Option[Double] = None,
  n: Option[Int] = None,
  stop: Option[List[String]] = None,
  responseFormat: Option[Json] = None,
  seed: Option[Int] = None,
  user: Option[String] = None,
  @jsonExclude extra: Json.Obj = Json.Obj(),
) derives JsonEncoder


object CompletionRequest:

  /**
   * The body to post.
   *
   * [[CompletionRequest.extra]] is merged last and wins, which is what makes it an escape hatch rather
   * than a field: a provider's own option needs no place here, and one that replaces something named above
   * is a caller overriding it deliberately.
   *
   * @param request what to ask for
   * @return the object to send
   */
  def body(request: CompletionRequest): Json.Obj = request.toJsonAST.toOption match
    case Some(encoded: Json.Obj) => encoded.merge(request.extra)
    case _                       => request.extra
