package homelab.llm.openai.request


import homelab.llm.Advertised
import zio.json.*
import zio.json.ast.Json


/**
 * One tool, as this protocol advertises it.
 *
 * The three things a provider is told wrapped in a function object, which is how OpenAI spells it and how
 * everything serving its protocol expects to receive it.
 *
 * @param `type` what kind of tool it is, which is always a function here
 * @param function what it is called, what it is for, and what it takes
 */
final case class ToolRequest(`type`: String, function: ToolRequest.Function) derives JsonEncoder


object ToolRequest:

  /**
   * The function a tool call names.
   *
   * @param name the name the model calls it by
   * @param description what it is for, in the words the model reads
   * @param parameters what it takes, which this protocol calls `parameters`
   */
  final case class Function(name: String, description: String, parameters: Json) derives JsonEncoder

  /**
   * One tool, as this protocol carries it.
   *
   * @param advertised what a session says about the tool
   * @return what to send for it
   */
  def from(advertised: Advertised): ToolRequest =
    ToolRequest("function", Function(advertised.name, advertised.description, advertised.arguments.json))
