package homelab.llm.openai.request


import homelab.llm.Message
import zio.Chunk
import zio.json.*
import zio.json.ast.Json


/**
 * One message, as a chat-completions request carries it.
 *
 * The roles a provider names, each with the fields that role actually has. What makes this worth a type of
 * its own rather than a rendering of [[Message]] is the last case: a tool's answer is a *string*, where
 * every other role's content is an array of parts. That is a difference in shape, and a case with different
 * fields is where a difference in shape belongs.
 */
@jsonDiscriminator("role")
enum MessageRequest derives JsonEncoder {

  /**
   * Instructions that hold for the conversation.
   *
   * @param content what they are
   */
  @jsonHint("system") case System(content: List[ContentPart])

  /**
   * What the caller said.
   *
   * @param content what they said
   */
  @jsonHint("user") case User(content: List[ContentPart])

  /**
   * What the model said, and what it asked to have run.
   *
   * The calls are absent rather than empty when it asked for none: a provider given an empty array may read
   * it as having been offered nothing to say.
   *
   * @param content what it said
   * @param toolCalls what it asked for, absent when it asked for nothing
   */
  @jsonHint("assistant") @jsonMemberNames(SnakeCase) case Assistant(
    content: List[ContentPart],
    toolCalls: Option[List[CallRequest]],
  )

  /**
   * What a tool answered, paired to the call that asked.
   *
   * @param toolCallId the id of the call this answers
   * @param content the answer, as one string rather than as parts
   */
  @jsonHint("tool") @jsonMemberNames(SnakeCase) case Tool(toolCallId: String, content: String)
}


object MessageRequest:

  /**
   * One message of a conversation, as the wire carries it.
   *
   * @param message what the toolkit holds
   * @return what to send for it
   */
  def from(message: Message): MessageRequest = message match
    case Message.System(content)             => System(ContentPart.from(content))
    case Message.User(content)               => User(ContentPart.from(content))
    case Message.ToolResult(callId, content) => Tool(callId, ContentPart.text(content))
    case Message.Assistant(content, calls)   =>
      Assistant(ContentPart.from(content), Option.when(calls.nonEmpty)(calls.map(CallRequest.from).toList))
