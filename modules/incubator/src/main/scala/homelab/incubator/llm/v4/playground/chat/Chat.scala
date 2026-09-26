package homelab.incubator.llm.v4.playground.chat


import homelab.common.error.ApplicationError
import homelab.common.processing.Workflow
import homelab.common.processing.Workflow.Step
import homelab.llm.*
import homelab.incubator.llm.v4.playground.chat.Chat.{ Ask, BudgetExhausted, Ongoing }
import zio.{ Chunk, IO, NonEmptyChunk, ZIO }


/**
 * An agent whose conversation outlives the run that adds to it.
 *
 * The conversation is read once, when a question is put to it, and written once, when the model has
 * answered. What follows that a self-contained agent does not offer is that a run can be given a
 * conversation that already has messages, so asking a second question continues the first.
 *
 * The single write is what makes a failed run cost nothing: one that aborts before the answer leaves the
 * conversation as it found it, so the same question delivered again is answered from the beginning rather
 * than continued from half of one. What it costs is that nothing sees a turn until the run ends, and that
 * a run works from what it read — turns another writer adds meanwhile are not in what it sends. Two runs on
 * one conversation therefore interleave, and `serialised` on the id is the answer; this does not apply it,
 * because which runs may overlap is the caller's to say.
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
) extends Workflow[Any, ApplicationError, Ask, Ongoing, Message.Assistant] {

  private type State = Step.Current[Ask, Ongoing]
  private type Next  = Step.Next[Ongoing, Message.Assistant]

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
   * Read a conversation and put a question to it, new or already running.
   *
   * Nothing is stored here. The question is the first thing this run adds, and it is written with the rest
   * when the model has answered.
   *
   * @param ask which conversation, and what is being asked of it
   * @return the conversation to advance; aborts when the repository does
   */
  private def open(ask: Ask): IO[ApplicationError, Next] =
    for
      question  = Message.user(text(ask.question))
      system    = Message.system(text(systemPrompt))
      existing <- repository.get(ask.conversation)
      state     = Ongoing(ask.conversation, system +: existing, Chunk(question))
    yield Step.Continue(state)

  /**
   * Do whatever the conversation is waiting for.
   *
   * @param ongoing the conversation and what it holds
   * @return the next step; aborts when the repository, the model or a tool does
   */
  private def advance(ongoing: Ongoing): IO[ApplicationError, Next] =
    Progress.from(ongoing.transcript) match
      case Progress.Finished(answer)       => finish(ongoing, answer)
      case Progress.AwaitingTools(pending) => answer(ongoing, pending)
      case Progress.Empty                  => ask(ongoing)
      case Progress.AwaitingModel          => ask(ongoing)

  /**
   * Ask the model what to do next, and record its turn.
   *
   * @param ongoing the conversation and what it holds
   * @return the same conversation, one turn further on; aborts with [[Chat.BudgetExhausted]] when the
   *         model has had its budget of turns, and with the model's own error when it refuses
   */
  private def ask(ongoing: Ongoing): IO[ApplicationError, Next] =
    for
      _          <- verifyBudget(ongoing)
      session    <- tools.forSession(ongoing.conversation)
      completion <- model.complete(ongoing.transcript, session.advertised)
      spoken      = Chunk(Message.fromCompletion(completion))
    yield Step.Continue(ongoing ++ spoken)

  /**
   * Run the calls the model is waiting on, and record their answers.
   *
   * @param ongoing the conversation and what it holds
   * @param pending the calls with no result yet
   * @return the same conversation, with every call answered; 
   *         aborts when a tool cannot say whether it permits this caller
   */
  private def answer(ongoing: Ongoing, pending: NonEmptyChunk[Tool.Call.Raw]): IO[ApplicationError, Next] =
    for
      session  <- tools.forSession(ongoing.conversation)
      outcomes <- session.dispatchAll(pending.toChunk)
      answers   = outcomes.map(answered)
    yield Step.Continue(ongoing ++ answers)

  /**
   * Store what this run added to the conversation, and answer with the model's last word.
   *
   * The one write a run makes. A run that fails before here leaves the conversation as it found it, so a
   * redelivery re-runs it rather than continuing half of it.
   *
   * @param ongoing the conversation and what this run added to it
   * @param answer the model's last turn, already the last of what is being stored
   * @return the answer; aborts when the repository does
   */
  private def finish(ongoing: Ongoing, answer: Message.Assistant): IO[ApplicationError, Next] =
    repository.add(ongoing.conversation, ongoing.running).as(Step.Done(answer))

  /**
   * One tool's answer, as the conversation carries it.
   *
   * @param outcome what dispatch produced
   * @return the message to store
   */
  private def answered(outcome: Outcome): Message.ToolResult =
    Message.fromOutcome(outcome)

  /**
   * Refuse to ask again once the model has had its turns.
   *
   * Counted over what this run has added rather than over the whole conversation, so the budget is what one
   * question may cost and a long conversation does not arrive already spent.
   *
   * @param ongoing the conversation and what this run has added to it
   * @return noop while turns remain; aborts with [[Chat.BudgetExhausted]] once they are spent
   */
  private def verifyBudget(ongoing: Ongoing): IO[BudgetExhausted, Unit] =
    if ongoing.turns < budget then ZIO.unit
    else ZIO.fail(BudgetExhausted(ongoing.conversation, budget))

  /**
   * Words, as a message carries them.
   *
   * @param value what to say
   * @return the one content part that says it
   */
  private def text(value: String): Message.Content =
    Message.Content.Text(value)
}


object Chat:

  /**
   * What a run adds to a conversation — everything but the standing instructions, which are prepended to
   * each request and never stored.
   */
  private type Running = Message.User | Message.Assistant | Message.ToolResult

  /**
   * A conversation part-way through a run: which one, and everything said in it so far.
   *
   * What was stored and what this run has added are kept apart, because only the second is written back.
   *
   * @param conversation which conversation
   * @param messages the standing instructions and what was already stored, oldest first
   * @param running what this run has added, and what it will store when it finishes
   */
  final case class Ongoing(
    conversation: Conversation,
    messages: Chunk[Message],
    running: Chunk[Running] = Chunk.empty,
  ) {

    /**
     * The same conversation with a turn added to what this run has done.
     *
     * @param added what just happened, in the order it happened
     * @return the conversation including it
     */
    def ++(added: Chunk[Running]): Ongoing = copy(running = running ++ added)

    /**
     * The conversation as the model is asked to read it: the standing instructions, what was stored, then
     * what this run has added.
     *
     * @return every message to send
     */
    def transcript: Chunk[Message] = messages ++ running

    /**
     * How many turns the model has taken in this run, counted from what it has said rather than kept
     * beside it.
     *
     * @return the number of turns the model has spoken since the question was put
     */
    def turns: Int = running.count:
      case Message.Assistant(_, _) => true
      case _                       => false
  }

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
  final case class BudgetExhausted(conversation: Conversation, budget: Int) extends ApplicationError.DomainError:

    /**
     * What went wrong, for whoever set the budget.
     *
     * @return the reason, naming the conversation and its budget
     */
    override def message: String = s"conversation '$conversation' took its $budget turns without answering"
