package homelab.incubator.llm.v3


import homelab.incubator.llm.v3.Model.Message
import zio.{ Chunk, NonEmptyChunk }


/**
 * Reading a conversation's messages for what has to happen next.
 *
 * Nothing records where a turn has got to; the messages say it. A runner that picks up a conversation —
 * for the first time, after a crash, or because something it was waiting for finished — asks the same
 * question of the same data and gets the same answer, so there is no second record to keep in step.
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
   * Work a conversation started that has not come back.
   *
   * A different question from [[Progress]], and deliberately a different answer: a conversation can have
   * stopped with an answer and still be owed results, which is what a subagent nobody waited for looks
   * like.
   *
   * @param handles what was promised and not delivered
   */
  final case class Outstanding(handles: Set[String]):

    /**
     * Whether anything is still owed.
     *
     * @return true when something was promised and has not come back
     */
    def pending: Boolean = handles.nonEmpty

  /**
   * What the conversation started and has not been given, read from its messages.
   *
   * A result that promises names work outliving the call that asked for it; a later result delivers it.
   * What is still owed is the one set less the other, and a delivery of something never promised answers
   * for nothing.
   *
   * @param messages the conversation so far, oldest first
   * @return what is still owed
   */
  def outstanding(messages: Chunk[Message]): Outstanding =
    Outstanding(messages.foldLeft(Set.empty[String])(settle))

  /**
   * Read one more message for what it opens or closes.
   *
   * @param opened what is owed so far
   * @param message the next message
   * @return what is owed after it
   */
  private def settle(opened: Set[String], message: Message): Set[String] = message match
    case Message.ToolResult(_, _, Tool.Result.Standing.Promised(handles))  => opened ++ handles
    case Message.ToolResult(_, _, Tool.Result.Standing.Delivered(handles)) => opened -- handles
    case _                                                                 => opened

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
    reading.turn match
      case None       => if messages.isEmpty then Progress.Empty else Progress.AwaitingModel
      case Some(turn) =>
        NonEmptyChunk.fromChunk(turn.calls.filterNot(call => reading.answered.contains(call.id))) match
          case Some(pending)         => Progress.AwaitingTools(pending)
          case None if reading.since => Progress.AwaitingModel
          case None                  => Progress.Finished(turn.content)

  /**
   * What reading the messages so far has established.
   *
   * @param turn the most recent assistant turn, absent until the model has spoken
   * @param answered the ids answered since that turn
   * @param since whether anything at all was said after it
   */
  final private case class Reading(turn: Option[Message.Assistant], answered: Set[String], since: Boolean)

  private object Reading:

    /** Nothing read yet. */
    val start: Reading = Reading(None, Set.empty, false)

  /**
   * Read one more message.
   *
   * An assistant turn replaces what came before it, because only the last one decides. Anything else is
   * something that happened after it, and a result is also an answer to one of its calls.
   *
   * @param reading what the messages so far established
   * @param message the next message
   * @return what they establish together
   */
  private def advance(reading: Reading, message: Message): Reading = message match
    case turn: Message.Assistant          => Reading(Some(turn), Set.empty, since = false)
    case Message.ToolResult(callId, _, _) => reading.copy(answered = reading.answered + callId, since = true)
    case _                                => reading.copy(since = true)
}
