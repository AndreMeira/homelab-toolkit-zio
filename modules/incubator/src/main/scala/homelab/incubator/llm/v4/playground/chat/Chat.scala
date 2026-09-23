package homelab.incubator.llm.v4.playground.chat


import homelab.common.error.ApplicationError
import homelab.common.processing.Workflow
import homelab.common.processing.Workflow.Step
import homelab.incubator.llm.v4.*
import zio.{ Chunk, IO, NonEmptyChunk, ZIO }


/**
 * An agent whose conversation lives in a repository rather than in its own state.
 *
 * The workflow's state is which conversation, not the conversation: every step reads it back and writes
 * what it did. Two things follow that a self-contained agent does not offer. A run can be given a
 * conversation that already has messages in it, so asking a second question continues the first. And
 * anything else holding the repository sees a turn the moment it lands, rather than when the run ends.
 *
 * What it costs is a read per step and a conversation that two concurrent runs can interleave — `serialised`
 * on the same id is the answer to the second, and this does not apply it, because which runs may overlap is
 * the caller's to say.
 *
 * Unimplemented where it touches storage: [[MessageRepository]] is a port and nothing here satisfies it.
 *
 * @param repository where the messages are kept
 * @param model what is asked for the next move
 * @param tools what the model may call
 * @param systemPrompt what the model is told before the conversation, on every call
 * @param budget the most turns the model may take in one run
 */
final class Chat(
  repository: MessageRepository,
  model: Model.Fixed[ApplicationError.AdapterError],
  tools: Registry[Unit],
  systemPrompt: String,
  budget: Int = 16,
) extends Workflow[Any, ApplicationError, Chat.Ask, Conversation, Chunk[Message.Content]] {

  private type State = Step.Current[Chat.Ask, Conversation]
  private type Next  = Step.Next[Conversation, Chunk[Message.Content]]

  override val name: String = "chat"

  /**
   * Put a question to a conversation, or take it one step further.
   *
   * @return the transition; aborts when the repository, the model or a tool does
   */
  def next: State => IO[ApplicationError, Next] =
    case Step.Init(ask)              => open(ask)
    case Step.Continue(conversation) => advance(conversation)

  /**
   * Add a question to a conversation, new or already running.
   *
   * @param ask which conversation, and what is being asked of it
   * @return the conversation to advance; aborts when the repository does
   */
  private def open(ask: Chat.Ask): IO[ApplicationError, Next] =
    repository
      .add(ask.conversation, Chunk(Message.User(text(ask.question))))
      .as(Step.Continue(ask.conversation))

  /**
   * Do whatever the conversation is waiting for, reading it back to find out.
   *
   * @param conversation which conversation to move on
   * @return the next step; aborts when the repository, the model or a tool does
   */
  private def advance(conversation: Conversation): IO[ApplicationError, Next] =
    repository.get(conversation).flatMap(messages => act(conversation, messages))

  /**
   * Act on what a conversation's messages say is outstanding.
   *
   * @param conversation which conversation is being moved on
   * @param messages what it holds right now
   * @return the next step; aborts when the model or a tool does
   */
  private def act(conversation: Conversation, messages: Chunk[Message]): IO[ApplicationError, Next] =
    Progress.from(messages) match
      case Progress.Finished(answer)       => ZIO.succeed(Step.Done(answer))
      case Progress.AwaitingTools(pending) => answer(conversation, pending)
      case Progress.Empty                  => ask(conversation, messages)
      case Progress.AwaitingModel          => ask(conversation, messages)

  /**
   * Ask the model what to do next, and record its turn.
   *
   * @param conversation which conversation is being moved on
   * @param messages what it holds right now
   * @return the same conversation, one turn further on; aborts with [[Chat.Exhausted]] when the model has
   *         had its budget of turns, and with the model's own error when it refuses
   */
  private def ask(conversation: Conversation, messages: Chunk[Message]): IO[ApplicationError, Next] =
    if turns(messages) >= budget then ZIO.fail(Chat.Exhausted(conversation, budget))
    else
      for
        session    <- tools.forSession(())
        completion <- model.complete(Model.Request(instructed(messages), session.advertised))
        _          <- repository.add(conversation, Chunk(Message.from(completion)))
      yield Step.Continue(conversation)

  /**
   * Run the calls the model is waiting on, and record their answers.
   *
   * @param conversation which conversation is being moved on
   * @param pending the calls with no result yet
   * @return the same conversation, with every call answered; aborts when a tool cannot say whether it
   *         permits this caller
   */
  private def answer(conversation: Conversation, pending: NonEmptyChunk[Tool.Call]): IO[ApplicationError, Next] =
    for
      session  <- tools.forSession(())
      outcomes <- session.dispatchAll(pending.toList)
      _        <- repository.add(conversation, Chunk.fromIterable(outcomes).map(answered))
    yield Step.Continue(conversation)

  /**
   * A conversation as the model is asked to read it: the standing instructions, then what happened.
   *
   * The instructions are not stored, so they are not part of what a conversation is, and a question asked
   * tomorrow is answered under tomorrow's. What the repository holds is what was said.
   *
   * @param messages the conversation as it is stored
   * @return the messages to send
   */
  private def instructed(messages: Chunk[Message]): Chunk[Message] =
    Message.System(text(systemPrompt)) +: messages

  /**
   * One tool's answer, as the conversation carries it.
   *
   * @param outcome what dispatch produced
   * @return the message to store
   */
  private def answered(outcome: Outcome): Message = Message.from(outcome)

  /**
   * How many turns the model has taken, counted from the conversation rather than kept beside it.
   *
   * @param messages the conversation so far
   * @return the number of turns the model has spoken
   */
  private def turns(messages: Chunk[Message]): Int = messages.count(spoken)

  /**
   * Whether a message is one the model wrote.
   *
   * @param message the message
   * @return true when the model wrote it
   */
  private def spoken(message: Message): Boolean = message match
    case Message.Assistant(_, _) => true
    case _                       => false

  /**
   * Words, as a message carries them.
   *
   * @param value what to say
   * @return the one content part that says it
   */
  private def text(value: String): Chunk[Message.Content] = Chunk(Message.Content.Text(value))
}


object Chat:

  /**
   * A question put to a conversation.
   *
   * @param conversation which conversation to put it to, new or already running
   * @param question what is being asked
   */
  final case class Ask(conversation: Conversation, question: String)

  /**
   * A run stopped because the model had taken every turn it was allowed.
   *
   * The conversation keeps everything the run wrote, so raising the budget and asking again resumes from
   * where it stopped rather than starting over.
   *
   * @param conversation which conversation ran out
   * @param budget how many turns it was allowed
   */
  final case class Exhausted(conversation: Conversation, budget: Int) extends ApplicationError.DomainError:

    /**
     * What went wrong, for whoever set the budget.
     *
     * @return the reason, naming the conversation and its budget
     */
    override def message: String = s"conversation '$conversation' took its $budget turns without answering"
