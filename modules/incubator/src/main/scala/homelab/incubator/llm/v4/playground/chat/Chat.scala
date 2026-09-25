package homelab.incubator.llm.v4.playground.chat


import homelab.common.error.ApplicationError
import homelab.common.processing.Workflow
import homelab.common.processing.Workflow.Step
import homelab.incubator.llm.v4.*
import zio.{ Chunk, IO, NonEmptyChunk, ZIO }


/**
 * An agent whose conversation outlives the run that adds to it.
 *
 * The conversation is read once, when a question is put to it, and written a turn at a time as it grows.
 * Two things follow that a self-contained agent does not offer: a run can be given a conversation that
 * already has messages, so asking a second question continues the first, and anything else holding the
 * repository sees a turn the moment it lands rather than when the run ends.
 *
 * A run reads once and then works from what it has, so turns another writer adds while it is running are
 * not in what it sends. Two runs on one conversation therefore interleave — `serialised` on the id is the
 * answer, and this does not apply it, because which runs may overlap is the caller's to say.
 *
 * Unimplemented where it touches storage: [[MessageRepository]] is a port and nothing here satisfies it.
 *
 * @param repository where the messages are kept
 * @param model what is asked for the next move
 * @param tools what the model may call, bound to the conversation it is called in
 * @param systemPrompt what the model is told before the conversation, on every call
 * @param budget the most turns the model may take in one run
 */
final class Chat(
  repository: MessageRepository,
  model: Model.Fixed[ApplicationError.AdapterError],
  tools: Registry[Conversation],
  systemPrompt: String,
  budget: Int = 16,
) extends Workflow[Any, ApplicationError, Chat.Ask, Chat.Ongoing, Chunk[Message.Content]] {

  private type State = Step.Current[Chat.Ask, Chat.Ongoing]
  private type Next  = Step.Next[Chat.Ongoing, Chunk[Message.Content]]

  override val name: String = "chat"

  /**
   * Put a question to a conversation, or take it one step further.
   *
   * @return the transition; aborts when the repository, the model or a tool does
   */
  def next: State => IO[ApplicationError, Next] =
    case Step.Init(ask)         => open(ask)
    case Step.Continue(ongoing) => advance(ongoing)

  /**
   * Add a question to a conversation, new or already running.
   *
   * @param ask which conversation, and what is being asked of it
   * @return the conversation to advance; aborts when the repository does
   */
  private def open(ask: Chat.Ask): IO[ApplicationError, Next] =
    val question = Chunk(Message.User(text(ask.question)))
    for existing <- repository.get(ask.conversation)
    yield Step.Continue(Chat.Ongoing(ask.conversation, existing ++ question))

  /**
   * Do whatever the conversation is waiting for.
   *
   * @param ongoing the conversation and what it holds
   * @return the next step; aborts when the repository, the model or a tool does
   */
  private def advance(ongoing: Chat.Ongoing): IO[ApplicationError, Next] =
    Progress.from(ongoing.messages) match
      case Progress.Finished(answer)       => ZIO.succeed(Step.Done(answer))
      case Progress.AwaitingTools(pending) => answer(ongoing, pending)
      case Progress.Empty                  => ask(ongoing)
      case Progress.AwaitingModel          => ask(ongoing)

  /**
   * Ask the model what to do next, and record its turn.
   *
   * @param ongoing the conversation and what it holds
   * @return the same conversation, one turn further on; aborts with [[Chat.Exhausted]] when the model has
   *         had its budget of turns, and with the model's own error when it refuses
   */
  private def ask(ongoing: Chat.Ongoing): IO[ApplicationError, Next] =
    if turns(ongoing.messages) >= budget then ZIO.fail(Chat.Exhausted(ongoing.conversation, budget))
    else
      for
        session    <- tools.forSession(ongoing.conversation)
        completion <- model.complete(Model.Request(instructed(ongoing.messages), session.advertised))
        spoken      = Chunk(Message.fromCompletion(completion))
      yield Step.Continue(ongoing.and(spoken))

  /**
   * Run the calls the model is waiting on, and record their answers.
   *
   * @param ongoing the conversation and what it holds
   * @param pending the calls with no result yet
   * @return the same conversation, with every call answered; aborts when a tool cannot say whether it
   *         permits this caller
   */
  private def answer(ongoing: Chat.Ongoing, pending: NonEmptyChunk[Tool.Call.Raw]): IO[ApplicationError, Next] =
    for
      session  <- tools.forSession(ongoing.conversation)
      outcomes <- session.dispatchAll(pending.toList)
      answers   = Chunk.fromIterable(outcomes).map(answered)
    yield Step.Continue(ongoing.and(answers))

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
  private def answered(outcome: Outcome): Message =
    Message.fromOutcome(outcome)

  /**
   * How many turns the model has taken, counted from the conversation rather than kept beside it.
   *
   * @param messages the conversation so far
   * @return the number of turns the model has spoken
   */
  private def turns(messages: Chunk[Message]): Int =
    messages.count:
      case Message.Assistant(_, _) => true
      case _                       => false

  /**
   * Words, as a message carries them.
   *
   * @param value what to say
   * @return the one content part that says it
   */
  private def text(value: String): Chunk[Message.Content] =
    Chunk(Message.Content.Text(value))
}


object Chat:

  /**
   * A conversation part-way through a run: which one, and everything said in it so far.
   *
   * The messages are carried rather than re-read because a run adds to them and nothing else does while it
   * holds them. What is added is written to the repository as it happens, so the two say the same thing.
   *
   * @param conversation which conversation
   * @param messages everything it holds, oldest first
   */
  final case class Ongoing(conversation: Conversation, messages: Chunk[Message]):

    /**
     * The same conversation with a turn added.
     *
     * @param added what just happened, in the order it happened
     * @return the conversation including it
     */
    def and(added: Chunk[Message]): Ongoing = copy(messages = messages ++ added)

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
