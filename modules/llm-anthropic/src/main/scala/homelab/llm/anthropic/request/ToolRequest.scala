package homelab.llm.anthropic.request


import homelab.llm.Advertised
import zio.json.*
import zio.json.ast.Json


/**
 * One tool, as the Messages API advertises it.
 *
 * The same three things every provider is told, flat rather than wrapped in a function object, and with the
 * schema under `input_schema`. That difference from the chat-completions spelling is why a session says
 * what a tool *is* and leaves the shaping here.
 *
 * @param name the name the model calls it by
 * @param description what it is for, in the words the model reads
 * @param inputSchema what it takes
 */
@jsonMemberNames(SnakeCase)
final case class ToolRequest(name: String, description: String, inputSchema: Json) derives JsonEncoder


object ToolRequest:

  /**
   * One tool, as this API carries it.
   *
   * @param advertised what a session says about the tool
   * @return what to send for it
   */
  def from(advertised: Advertised): ToolRequest =
    ToolRequest(advertised.name, advertised.description, advertised.arguments.json)
