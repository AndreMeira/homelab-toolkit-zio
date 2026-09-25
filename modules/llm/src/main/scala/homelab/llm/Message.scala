package homelab.llm


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
  case Assistant(content: Chunk[Message.Content], calls: Chunk[Tool.Call.Raw])

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
   * Text is the part a loop reads. Anything else a provider accepts travels as [[Raw]] and reaches the wire
   * as it was written, so a caller can use what a provider offers without waiting for it to be modelled
   * here — and nothing here reads a [[Raw]], so there is nothing for it to be misread as.
   *
   * Every role uses it, and for different things: a [[Message.User]] carries images, audio and documents; a
   * [[Message.Assistant]] carries reasoning blocks, which some providers require be handed back unaltered
   * on the following turn; a [[Message.System]] carries text annotated to be cached, which is why it is not
   * a plain string; a [[Message.ToolResult]] carries images where a provider takes blocks rather than text.
   *
   * Which parts a role may legally carry is the provider's to say and differs between them, so a message
   * built with one it does not accept is refused there rather than here.
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

  /**
   * Instructions that hold for the conversation.
   *
   * @param content what they are
   * @return the message, as itself rather than as a [[Message]]
   */
  def system(content: Chunk[Content]): System = System(content)

  /**
   * Instructions that hold for the conversation.
   *
   * @param content what they are
   * @return the message, as itself rather than as a [[Message]]
   */
  def system(content: Content): System = system(Chunk(content))

  /**
   * What the caller said.
   *
   * @param content what they said
   * @return the message, as itself rather than as a [[Message]]
   */
  def user(content: Chunk[Content]): User = User(content)

  /**
   * What the caller said.
   *
   * @param content what they said
   * @return the message, as itself rather than as a [[Message]]
   */
  def user(content: Content): User = user(Chunk(content))

  /**
   * What the model said, and what it asked to have run.
   *
   * @param content what it said, which is empty when it only asked for tools
   * @param calls what it asked for, which is empty when it only answered
   * @return the message, as itself rather than as a [[Message]]
   */
  def assistant(content: Chunk[Content], calls: Chunk[Tool.Call.Raw] = Chunk.empty): Assistant =
    Assistant(content, calls)

  /**
   * What a tool answered, paired to the call that asked.
   *
   * @param callId the id of the call this answers
   * @param content the answer, as the model will read it
   * @return the message, as itself rather than as a [[Message]]
   */
  def toolResult(callId: Tool.Call.Id, content: Chunk[Content]): ToolResult = ToolResult(callId, content)

  /**
   * Adjacent messages of one role joined into one, for a provider that will not take them separately.
   *
   * Only [[System]] and [[User]] are joined, because content is all they carry. An [[Assistant]] turn also
   * carries the calls it made, and a [[ToolResult]] is paired to one call by its id, so each of those stays
   * as it was however many of them sit together.
   *
   * For an adapter to call on the way out rather than something to store. What a conversation holds is what
   * happened, and two adjacent messages of one role usually mean a run that appended one and then failed
   * before it was answered.
   *
   * @param messages the conversation, oldest first
   * @return the same conversation with adjacent system or user messages joined
   */
  def merged(messages: Chunk[Message]): Chunk[Message] =
    messages.foldLeft(Chunk.empty[Message]) {
      case (before :+ System(said)) -> System(more) => before :+ System(said ++ more)
      case (before :+ User(said)) -> User(more)     => before :+ User(said ++ more)
      case kept -> next                             => kept :+ next
    }

  /**
   * What the model said, as the conversation carries it.
   *
   * A turn goes back as it came — words and calls both — so what the model reads next is what it wrote.
   * Why it stopped and what it cost are facts about the call rather than about the turn, and stay behind.
   *
   * @param completion what the model returned
   * @return the assistant message to append
   */
  def fromCompletion(completion: Model.Completion): Assistant =
    Assistant(completion.content, completion.calls)

  /**
   * What a tool answered, as the conversation carries it.
   *
   * The text is whatever the result renders to, a failure included — everything a model can react to
   * reaches it as words.
   *
   * @param outcome what dispatch produced
   * @return the tool message to append
   */
  def fromOutcome(outcome: Outcome): ToolResult =
    ToolResult(outcome.call.id, Chunk(Content.Text(outcome.result.render)))
