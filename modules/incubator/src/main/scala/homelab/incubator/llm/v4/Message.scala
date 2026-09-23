package homelab.incubator.llm.v4


import zio.Chunk
import zio.json.ast.Json


/**
 * A turn of the conversation, one-to-one with what a request carries.
 *
 * Every case is something a provider has a field for, which is what lets a stored conversation be sent
 * as it stands. What a store knows beyond that — when a message landed, what it is named by — is the
 * store's row, not this.
 *
 * @see [[Model.Request.messages]]
 */
enum Message:

  /**
   * Instructions that hold for the conversation.
   *
   * @param content what they are
   */
  case System(content: Chunk[Message.Content])

  /**
   * What the caller said.
   *
   * @param content what they said
   */
  case User(content: Chunk[Message.Content])

  /**
   * What the model said, and what it asked to have run.
   *
   * @param content what it said, which is empty when it only asked for tools
   * @param calls what it asked for, which is empty when it only answered
   */
  case Assistant(content: Chunk[Message.Content], calls: Chunk[Tool.Call])

  /**
   * What a tool answered, paired to the call that asked.
   *
   * @param callId the id of the call this answers
   * @param content the answer, as the model will read it
   */
  case ToolResult(callId: Tool.Call.Id, content: Chunk[Message.Content])

object Message:

  /**
   * A piece of a message.
   *
   * Text is the part a loop reads. Anything else a provider accepts in a message — an image, a document, a
   * cache marker — travels as [[Raw]] and reaches the wire as it was written, so a caller can use what a
   * provider offers without waiting for it to be modelled here.
   */
  enum Content:

    /**
     * Words.
     *
     * @param text what they are
     */
    case Text(text: String)

    /**
     * A content part this type does not model, carried as the provider will receive it.
     *
     * @param json the part, as it goes on the wire
     */
    case Raw(json: Json)
