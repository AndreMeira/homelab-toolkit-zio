package homelab.llm

import zio.{ Chunk, NonEmptyChunk }


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
  case AwaitingTools(pending: NonEmptyChunk[Tool.Call.Raw])

  /**
   * The conversation ended with an answer.
   *
   * @param answer what the model said last
   */
  case Finished(answer: Message.Assistant)


object Progress {

  /**
   * What a conversation is waiting for, read from its messages.
   *
   * Nothing records where a turn has got to; the messages say it, so a runner picking a conversation up —
   * for the first time, after a crash, or because something it was waiting for finished — asks the same
   * question of the same data and gets the same answer. The last turn the model spoke decides it: one whose
   * calls are all answered has moved on, one with a call outstanding has not, and one followed by nothing
   * is where the conversation stopped.
   *
   * @param messages the conversation so far, oldest first
   * @return what has to happen next
   */
  def from(messages: Chunk[Message]): Progress =
    val reading = messages.foldLeft(Reading.start)(advance)
    (messages.lastOption, reading.turn) match
      case (None, _)                => Progress.Empty
      case (_, None)                => Progress.AwaitingModel
      case (Some(last), Some(turn)) =>
        NonEmptyChunk.fromChunk(turn.calls.filter(reading.unanswered)) match
          case Some(pending)               => Progress.AwaitingTools(pending)
          case None if spokenByModel(last) => Progress.Finished(turn)
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
    def unanswered(call: Tool.Call.Raw): Boolean = !answered.contains(call.id)

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
    case turn: Message.Assistant          => Reading(Some(turn), Set.empty)
    case Message.ToolResult(callId, _, _) => reading.copy(answered = reading.answered + callId)
    case _                                => reading
}
