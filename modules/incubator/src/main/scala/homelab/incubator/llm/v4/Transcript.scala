package homelab.incubator.llm.v4


import homelab.incubator.llm.v4.Model.Message
import zio.{ Chunk, NonEmptyChunk }


/**
 * Reading a conversation for what has to happen next.
 *
 * Nothing records where a turn has got to; the messages say it. A runner that picks up a conversation — for
 * the first time, after a crash, or because something it was waiting for finished — asks the same question
 * of the same data and gets the same answer, so there is no second record to keep in step.
 */
object Transcript {

  /**
   * What a conversation is waiting for.
   *
   * The two waits are the interesting ones and they are not alike: one is answered by calling the model,
   * the other by running tools that were asked for and have not answered.
   */
  enum Progress:

    /** Nothing has been said, so there is nothing to send. */
    case Empty

    /** Everything asked has been answered; the model's move. */
    case AwaitingModel

    /**
     * Calls the model made that have no result yet.
     *
     * @param pending the calls still owed an answer, in the order they were asked
     */
    case AwaitingTools(pending: NonEmptyChunk[Tool.Call])

    /**
     * The conversation ended with an answer.
     *
     * @param answer what the model said last
     */
    case Finished(answer: Chunk[Model.Content])

  /**
   * What the model said, as the conversation carries it.
   *
   * A turn goes back as it came — words and calls both — so what the model reads next is what it wrote.
   * Why it stopped and what it cost are facts about the call rather than about the turn, and stay behind.
   *
   * @param completion what the model returned
   * @return the assistant message to append
   */
  def said(completion: Model.Completion): Message.Assistant =
    Message.Assistant(completion.content, completion.calls)

  /**
   * What a tool answered, as the conversation carries it.
   *
   * The text is whatever the result renders to, a failure included — everything a model can react to
   * reaches it as words.
   *
   * @param outcome what dispatch produced
   * @return the tool message to append
   */
  def answered(outcome: Outcome): Message.ToolResult =
    Message.ToolResult(outcome.callId, Chunk(Model.Content.Text(outcome.result.render)))

  /**
   * One whole turn: what the model said, and an answer to every call it made.
   *
   * The protocol needs a result for each call id before the next model call, so the pair is built together
   * rather than appended separately. The order is the model's: its turn first, then the outcomes as
   * dispatch returned them.
   *
   * @param completion what the model returned
   * @param outcomes one outcome per call the completion asked for
   * @return the messages to append, in order
   */
  def turn(completion: Model.Completion, outcomes: Chunk[Outcome]): Chunk[Message] =
    said(completion) +: outcomes.map(answered)

  /**
   * What the conversation is waiting for, read from its messages.
   *
   * The last assistant turn decides it, so the messages are read once and each turn replaces what the one
   * before it left: a turn whose calls are all answered has moved on, one with a call outstanding has not,
   * and a turn that asked for nothing and was followed by nothing is where the conversation stopped.
   *
   * @param messages the conversation so far, oldest first
   * @return what has to happen next
   */
  def progress(messages: Chunk[Message]): Progress =
    val reading = messages.foldLeft(Reading.start)(advance)
    (messages.lastOption, reading.turn) match
      case (None, _)                => Progress.Empty
      case (_, None)                => Progress.AwaitingModel
      case (Some(last), Some(turn)) =>
        NonEmptyChunk.fromChunk(turn.calls.filter(reading.unanswered)) match
          case Some(pending)               => Progress.AwaitingTools(pending)
          case None if spokenByModel(last) => Progress.Finished(turn.content)
          case None                        => Progress.AwaitingModel

  /**
   * Whether a message is the model speaking, which is the only kind a turn can end on.
   *
   * @param message the message
   * @return true when the model wrote it
   */
  private def spokenByModel(message: Message): Boolean = message match
    case Message.Assistant(_, _) => true
    case _                       => false

  /**
   * What reading the messages so far has established.
   *
   * @param turn the most recent turn the model spoke, absent until it has
   * @param answered the ids answered since that turn
   */
  final private case class Reading(turn: Option[Message.Assistant], answered: Set[Tool.Call.Id]):

    /**
     * Whether a call of the current turn is still owed an answer.
     *
     * @param call one of the turn's calls
     * @return true when nothing has answered it
     */
    def unanswered(call: Tool.Call): Boolean = !answered.contains(call.id)

  private object Reading:

    /** Nothing read yet. */
    val start: Reading = Reading(None, Set.empty)

  /**
   * Read one more message.
   *
   * An assistant turn replaces what came before it, because only the last one decides, and a tool result
   * answers one of that turn's calls.
   *
   * @param reading what the messages so far established
   * @param message the next message
   * @return what they establish together
   */
  private def advance(reading: Reading, message: Message): Reading = message match
    case turn: Message.Assistant       => Reading(Some(turn), Set.empty)
    case Message.ToolResult(callId, _) => reading.copy(answered = reading.answered + callId)
    case _                             => reading
}
