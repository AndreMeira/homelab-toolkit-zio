package homelab.llm.mistral.request


import homelab.llm.Advertised
import zio.Chunk
import zio.json.*
import zio.json.ast.Json


/**
 * One tool, as a Mistral request offers it.
 *
 * A function is a tool the caller runs. The rest are tools Mistral runs on its own side, each named by its
 * `type`; their `tool_configuration` is carried as JSON.
 */
@jsonDiscriminator("type")
enum ToolRequest derives JsonEncoder {

  /**
   * A tool the caller runs when the model asks for it.
   *
   * @param function what it is called, what it is for, and what it takes
   */
  @jsonHint("function") case Function(function: ToolRequest.Definition)

  /**
   * Mistral's web search.
   *
   * @param toolConfiguration how it is set up, where a caller says
   */
  @jsonHint("web_search") @jsonMemberNames(SnakeCase) case WebSearch(toolConfiguration: Option[Json] = None)

  /**
   * Mistral's premium web search.
   *
   * @param toolConfiguration how it is set up, where a caller says
   */
  @jsonHint("web_search_premium") @jsonMemberNames(SnakeCase) case WebSearchPremium(toolConfiguration: Option[Json] = None)

  /**
   * Code run in Mistral's sandbox.
   *
   * @param toolConfiguration how it is set up, where a caller says
   */
  @jsonHint("code_interpreter") @jsonMemberNames(SnakeCase) case CodeInterpreter(toolConfiguration: Option[Json] = None)

  /**
   * Images generated on Mistral's side.
   *
   * @param toolConfiguration how it is set up, where a caller says
   */
  @jsonHint("image_generation") @jsonMemberNames(SnakeCase) case ImageGeneration(toolConfiguration: Option[Json] = None)

  /**
   * Search over document libraries held by Mistral.
   *
   * @param libraryIds which libraries, at least one
   * @param toolConfiguration how it is set up, where a caller says
   */
  @jsonHint("document_library") @jsonMemberNames(SnakeCase) case DocumentLibrary(
    libraryIds: Chunk[String],
    toolConfiguration: Option[Json] = None,
  )

  /**
   * A connector registered with Mistral.
   *
   * @param connectorId which connector
   * @param authorization the credential it calls out with, where it needs one
   * @param toolConfiguration how it is set up, where a caller says
   */
  @jsonHint("connector") @jsonMemberNames(SnakeCase) case Connector(
    connectorId: String,
    authorization: Option[Json] = None,
    toolConfiguration: Option[Json] = None,
  )
}


object ToolRequest:

  /**
   * A function the model may ask for.
   *
   * @param name the name the model calls it by: letters, digits, `_` and `-`, at most 64
   * @param parameters what it takes, as a JSON Schema, which the API requires even when it takes nothing
   * @param description what it is for, in the words the model reads
   * @param strict whether the model's arguments must conform to the schema exactly
   */
  final case class Definition(
    name: String,
    parameters: Json,
    description: Option[String] = None,
    strict: Option[Boolean] = None,
  ) derives JsonEncoder

  /**
   * One tool a session offers, as a function the caller runs.
   *
   * @param advertised what a session says about the tool
   * @return what to send for it
   */
  def from(advertised: Advertised): ToolRequest =
    Function(Definition(advertised.name, advertised.arguments.json, Some(advertised.description)))
