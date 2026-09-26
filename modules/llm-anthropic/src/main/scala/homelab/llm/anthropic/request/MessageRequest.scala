package homelab.llm.anthropic.request


import homelab.llm.{ Message, Tool }
import zio.Chunk
import zio.json.*
import zio.json.ast.Json


/**
 * One turn, as the Messages API carries it.
 *
 * Two roles only. What the chat-completions protocol says with a `system` role goes in the request's own
 * field, and what it says with a `tool` role is a block inside a user turn — so a conversation the toolkit
 * holds does not map one message to one of these.
 *
 * @param role who is speaking, which is `user` or `assistant` and nothing else
 * @param content what they said, as blocks
 */
final case class MessageRequest(role: String, content: List[Json]) derives JsonEncoder


object MessageRequest:

  /**
   * A conversation as this API receives it: the instructions, and the turns.
   *
   * The instructions are taken out because the request carries them in a field of its own, and several of
   * them run together — a conversation may have been told more than once.
   *
   * @param messages the conversation the toolkit holds
   * @return what the model is told before the conversation, and the conversation
   */
  def conversation(messages: Chunk[Message]): (Option[String], List[MessageRequest]) =
    val instructions = messages.collect { case Message.System(content) => ContentBlock.text(content) }
    (Option.when(instructions.nonEmpty)(instructions.mkString("\n")), turns(messages))

  /**
   * The turns of a conversation, with each tool result folded into a user turn.
   *
   * Consecutive results become one turn, which is what the API requires of a conversation that alternates:
   * a model that asked for three tools is answered once, with three blocks.
   *
   * @param messages the conversation the toolkit holds
   * @return the turns to send, oldest first
   */
  private def turns(messages: Chunk[Message]): List[MessageRequest] =
    messages
      .foldLeft(Chunk.empty[MessageRequest]) {
        case built -> Message.System(_)     => built
        case built -> Message.User(content) => built :+ user(ContentBlock.blocks(content))

        case built -> Message.Assistant(content, calls) =>
          built :+ assistant(ContentBlock.blocks(content) ++ calls.map(ContentBlock.asked))

        case (before :+ last) -> Message.ToolResult(callId, content) if last.role == User =>
          before :+ last.copy(content = last.content :+ ContentBlock.answered(callId, content))

        case built -> Message.ToolResult(callId, content) =>
          built :+ user(List(ContentBlock.answered(callId, content)))
      }
      .toList

  /** What the API calls the caller's side. */
  private val User: String = "user"

  /** What the API calls the model's side. */
  private val Assistant: String = "assistant"

  /**
   * A turn the caller takes.
   *
   * @param content its blocks
   * @return the turn
   */
  private def user(content: List[Json]): MessageRequest = MessageRequest(User, content)

  /**
   * A turn the model takes.
   *
   * @param content its blocks
   * @return the turn
   */
  private def assistant(content: List[Json]): MessageRequest = MessageRequest(Assistant, content)
