package homelab.llm.anthropic.request


import homelab.llm.{ Message, Tool }
import zio.Chunk
import zio.json.*
import zio.json.ast.Json


/**
 * One block this adapter writes.
 *
 * Where the chat-completions protocol has a role per kind of thing, this API has a block per kind: what the
 * model asked to run and what a tool answered are both blocks, inside an assistant turn and a user turn
 * respectively. Each carries its own `type`, which is what [[ContentBlock.from]] reads them back by.
 *
 * A block the caller built is not one of these — it is already JSON, and goes out as written. That is why
 * it is not a case here and why [[ContentBlock.blocks]] answers in `Json`.
 */
@jsonDiscriminator("type")
enum ContentBlock derives JsonEncoder {

  /**
   * Words.
   *
   * @param text what they are
   */
  @jsonHint("text") case Text(text: String)

  /**
   * What the model asked to have run.
   *
   * The input is an object here, where the chat-completions protocol sends the same thing as a string.
   *
   * @param id what the model named the call
   * @param name which tool it asked for
   * @param input the arguments it wrote
   */
  @jsonHint("tool_use") case ToolUse(id: String, name: String, input: Json)

  /**
   * What a tool answered.
   *
   * @param toolUseId the id of the call this answers
   * @param content the answer, as the model will read it
   */
  @jsonHint("tool_result") @jsonMemberNames(SnakeCase) case ToolResult(toolUseId: String, content: String)
}


object ContentBlock:

  /**
   * The blocks of a message's content.
   *
   * A part the caller built passes through as it was written; a part this adapter models is written by the
   * derived encoder.
   *
   * @param content what the toolkit holds
   * @return the blocks to send
   */
  def blocks(content: Chunk[Message.Content]): List[Json] = content.map(part).toList

  /**
   * One call the model made, as this API restates it.
   *
   * What the toolkit holds is the string the model wrote, so it is parsed back — and kept as text when it
   * will not parse, since a model that wrote something unreadable should see its own words again.
   *
   * @param call what the model asked for
   * @return the block
   */
  def asked(call: Tool.Call.Raw): Json =
    written(ToolUse(call.id, call.name, call.arguments.fromJson[Json].getOrElse(Json.Str(call.arguments))))

  /**
   * What a tool answered, as a block.
   *
   * @param callId the id of the call this answers
   * @param content the answer
   * @return the block
   */
  def answered(callId: Tool.Call.Id, content: Chunk[Message.Content]): Json =
    written(ToolResult(callId, text(content)))

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
  private def part(content: Message.Content): Json = content match
    case Message.Content.Text(said) => written(Text(said))
    case Message.Content.Raw(json)  => json

  /**
   * One block this adapter wrote, as JSON.
   *
   * The derived encoder answers an `Either`, and the left is unreachable: these are three case classes of
   * strings and a `Json`, and nothing about them can fail to be written.
   *
   * @param block the block
   * @return it, written
   */
  private def written(block: ContentBlock): Json = block.toJsonAST.getOrElse(Json.Null)
