package homelab.llm.mistral.request


import homelab.llm.Message
import zio.Chunk
import zio.json.*


/**
 * One message, as a Mistral request carries it.
 *
 * Four roles, each with the fields that role has. Content is always sent as chunks: the API takes a bare
 * string too, and a string is one text chunk.
 *
 * The API checks the order the roles come in, and refuses a conversation whose last message is not a user
 * or tool message, or an assistant message marked as a prefix.
 */
@jsonDiscriminator("role")
enum MessageRequest derives JsonEncoder {

  /**
   * Instructions that hold for the conversation.
   *
   * @param content what they are, as text and thinking chunks only
   */
  @jsonHint("system") case System(content: Chunk[ContentChunk])

  /**
   * What the caller said.
   *
   * @param content what they said, which may include images, documents, files and audio
   */
  @jsonHint("user") case User(content: Chunk[ContentChunk])

  /**
   * What the model said, and what it asked to have run.
   *
   * @param content what it said, including any reasoning it should see again
   * @param toolCalls what it asked for, absent when it asked for nothing
   * @param prefix whether this is the start of an answer the model is to continue, which only the last
   *               message of a conversation may be
   */
  @jsonHint("assistant") @jsonMemberNames(SnakeCase) case Assistant(
    content: Chunk[ContentChunk],
    toolCalls: Option[Chunk[CallRequest]] = None,
    prefix: Option[Boolean] = None,
  )

  /**
   * What a tool answered, paired to the call that asked.
   *
   * @param toolCallId the id of the call this answers, which must match the call's id exactly
   * @param content the answer
   * @param name which tool answered, where a caller says
   */
  @jsonHint("tool") @jsonMemberNames(SnakeCase) case Tool(
    toolCallId: CallId,
    content: Chunk[ContentChunk],
    name: Option[String] = None,
  )
}


object MessageRequest:

  /**
   * One message of a conversation, as the API carries it.
   *
   * The roles map one to one. A tool's answer goes out as its content alone: the API has no field for a
   * tool that failed, and the content already says so in words. Ids go out in the form the API takes, the
   * same in a call and in the answer to it.
   *
   * @param message what the toolkit holds
   * @return what to send for it
   */
  def from(message: Message): MessageRequest = message match
    case Message.System(content)                => System(content.map(ContentChunk.from))
    case Message.User(content)                  => User(content.map(ContentChunk.from))
    case Message.ToolResult(callId, content, _) => Tool(CallId.from(callId), content.map(ContentChunk.from))
    case Message.Assistant(content, calls)      =>
      Assistant(content.map(ContentChunk.from), Option.when(calls.nonEmpty)(calls.map(CallRequest.from)))
