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

  /**
   * The messages cannot be sent as they stand: one of them is out of place.
   *
   * @param from the first message out of place
   */
  case Broken(from: Message)


object Progress {

  /** Where a reading of the messages has got to, one message at a time. */
  private enum Cursor:

    /** Nothing read yet. */
    case Empty

    /**
     * Someone other than the model spoke last.
     *
     * @param message what they said
     */
    case One(message: Message.System | Message.User)

    /**
     * A turn of the model, and the calls it made that are still owed a result.
     *
     * @param message the turn
     * @param unanswered the calls with no result yet, in the order they were asked; empty only when it made none
     */
    case Aggregate(message: Message.Assistant, unanswered: List[Tool.Call.Raw])

    /**
     * A turn of the model whose calls have all been answered.
     *
     * @param message the turn
     */
    case Answered(message: Message.Assistant)

    /**
     * A message out of place. Nothing read after it changes the reading.
     *
     * @param from the message
     */
    case Broken(from: Message)

  /** Reading messages into a [[Cursor]]. */
  private object Cursor:

    /**
     * Read messages from the first, stopping at the first one out of place.
     *
     * @param messages the conversation, oldest first
     * @return where the reading got to
     */
    def read(messages: Chunk[Message]): Cursor =
      messages.foldWhile(Cursor.Empty) {
        case Cursor.Broken(_) => false
        case _                => true
      } {
        case Cursor.Empty -> message                                     => init(message)
        case Cursor.One(_) -> message                                    => init(message)
        case Cursor.Aggregate(_, Nil) -> message                         => init(message)
        case Cursor.Answered(_) -> message                               => init(message)
        case Cursor.Broken(from) -> message                              => Cursor.Broken(from)
        case (cursor: Cursor.Aggregate) -> (message: Message.ToolResult) => resolve(cursor, message)
        case (cursor: Cursor.Aggregate) -> message                       => Cursor.Broken(message)
      }

    /**
     * Read a message when nothing is owed.
     *
     * @param message the message
     * @return a turn of the model with the calls it made, or anyone else's message as the last word; broken
     *         for a result, since nothing is owed one
     */
    private def init(message: Message): Cursor = message match
      case msg: (Message.System | Message.User) => Cursor.One(msg)
      case Message.ToolResult(_, _, _)          => Cursor.Broken(message)
      case msg @ Message.Assistant(_, calls)    => Cursor.Aggregate(msg, calls.toList)

    /**
     * Read a result while a turn's calls are owed one.
     *
     * @param cursor the turn, and the calls it is still owed results for
     * @param message the result
     * @return the turn owing one call fewer when it answers the first call owed, answered when that call was
     *         the last; broken when it answers any other
     */
    private def resolve(cursor: Cursor.Aggregate, message: Message.ToolResult): Cursor = cursor match
      case Cursor.Aggregate(msg, call :: Nil) if call.id == message.callId  => Cursor.Answered(msg)
      case Cursor.Aggregate(msg, call :: rest) if call.id == message.callId => Cursor.Aggregate(msg, rest)
      case _                                                                => Cursor.Broken(message)

  /**
   * What a conversation is waiting for, read from its messages.
   *
   * Nothing records where a turn has got to; the messages say it, so a runner picking a conversation up —
   * for the first time, after a crash, or because something it was waiting for finished — asks the same
   * question of the same data and gets the same answer. The last turn the model spoke decides it: one whose
   * calls are all answered has moved on, one with a call outstanding has not, and one followed by nothing
   * is where the conversation stopped.
   *
   * A turn's results come straight after it, in the order its calls were made. Any other message in their
   * place, or a result nothing asked for, breaks the conversation, and reading stops there.
   *
   * @param messages the conversation so far, oldest first
   * @return what has to happen next
   */
  def from(messages: Chunk[Message]): Progress = Cursor.read(messages) match
    case Cursor.Empty                         => Progress.Empty
    case Cursor.One(_)                        => Progress.AwaitingModel
    case Cursor.Answered(_)                   => Progress.AwaitingModel
    case Cursor.Broken(from)                  => Progress.Broken(from)
    case Cursor.Aggregate(turn, Nil)          => Progress.Finished(turn)
    case Cursor.Aggregate(turn, head :: rest) => Progress.AwaitingTools(NonEmptyChunk(head, rest*))
}
