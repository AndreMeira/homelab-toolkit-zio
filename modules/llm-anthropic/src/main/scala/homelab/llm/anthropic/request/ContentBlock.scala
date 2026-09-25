package homelab.llm.anthropic.request


import homelab.llm.{ Message, Tool }
import zio.Chunk
import zio.json.*
import zio.json.ast.Json


/**
 * One block of a message's content.
 *
 * Where the chat-completions protocol has a role per kind of thing, this API has a block per kind: what the
 * model asked to run and what a tool answered are both blocks, inside an assistant turn and a user turn
 * respectively.
 */
enum ContentBlock {

  /**
   * Words.
   *
   * @param text what they are
   */
  case Text(text: String)

  /**
   * What the model asked to have run.
   *
   * The input is an object here, where the chat-completions protocol sends the same thing as a string. What
   * the toolkit holds is the string the model wrote, so this parses it back — and keeps it as text when it
   * will not parse, since a model that wrote something unparseable should see its own words again.
   *
   * @param id what the model named the call
   * @param name which tool it asked for
   * @param input the arguments it wrote
   */
  case ToolUse(id: String, name: String, input: Json)

  /**
   * What a tool answered.
   *
   * @param toolUseId the id of the call this answers
   * @param content the answer, as the model will read it
   */
  case ToolResult(toolUseId: String, content: String)

  /**
   * A block this adapter does not model, which the caller built and which goes out as it was written.
   *
   * @param json the block
   */
  case Raw(json: Json)
}


object ContentBlock:

  /**
   * How a block is written.
   *
   * Each kind carries its own `type`, and a [[Raw]] is spliced rather than wrapped — what the caller built
   * is the block, not a field of one.
   */
  given JsonEncoder[ContentBlock] = JsonEncoder[Json].contramap {
    case Text(text)                    => Json.Obj("type" -> Json.Str("text"), "text" -> Json.Str(text))
    case ToolUse(id, name, input)      =>
      Json.Obj(
        "type"  -> Json.Str("tool_use"),
        "id"    -> Json.Str(id),
        "name"  -> Json.Str(name),
        "input" -> input,
      )
    case ToolResult(toolUseId, content) =>
      Json.Obj(
        "type"        -> Json.Str("tool_result"),
        "tool_use_id" -> Json.Str(toolUseId),
        "content"     -> Json.Str(content),
      )
    case Raw(json)                     => json
  }

  /**
   * The blocks of a message's content.
   *
   * @param content what the toolkit holds
   * @return the blocks to send
   */
  def from(content: Chunk[Message.Content]): List[ContentBlock] = content.map(block).toList

  /**
   * One call the model made, as this API restates it.
   *
   * @param call what the model asked for
   * @return the block
   */
  def asked(call: Tool.Call.Raw): ContentBlock =
    ToolUse(call.id, call.name, call.arguments.fromJson[Json].getOrElse(Json.Str(call.arguments)))

  /**
   * The words of a message, for a tool result, which takes text rather than blocks.
   *
   * @param content the parts
   * @return their words, run together
   */
  def text(content: Chunk[Message.Content]): String =
    content.collect { case Message.Content.Text(said) => said }.mkString

  /**
   * One content part.
   *
   * @param content what the toolkit holds
   * @return the block to send
   */
  private def block(content: Message.Content): ContentBlock = content match
    case Message.Content.Text(text) => Text(text)
    case Message.Content.Raw(json)  => Raw(json)
