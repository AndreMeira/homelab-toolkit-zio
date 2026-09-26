package homelab.incubator.llm.v4.playground.agent


import homelab.common.error.ApplicationError
import homelab.common.processing.Workflow
import homelab.common.processing.Workflow.Step
import homelab.llm.{ Message, Model, Outcome, Progress, Registry, Tool }
import zio.{ Chunk, IO, NonEmptyChunk, ZIO }


/**
 * The simplest agent there is: a question answered by a model that may call tools on the way.
 *
 * Four things make one: what the model is told before anything else, what it may call, what answers, and
 * the caller every call is confined to. The loop itself is here and cannot be overridden — it is the same
 * loop for every agent of this shape, and what differs between them is the four.
 *
 * A [[Workflow]], so it is run in memory by `run`, made durable by `persisted`, and serialised per question
 * by `serialised`, none of which this knows about. The state is the conversation, so a run resumed from a
 * checkpoint resumes by reading it — there is nothing else to restore.
 *
 * @tparam Ctx the caller's context every tool call carries
 */
trait Basic[Ctx] extends Workflow[Any, ApplicationError, String, Chunk[Message], Message.Assistant] {

  private type State = Step.Current[String, Chunk[Message]]
  private type Next  = Step.Next[Chunk[Message], Message.Assistant]

  /** What the model is told before the question, and reads on every call. */
  def systemPrompt: String

  /** What the model may call. A tool it may not use is one it is never told about. */
  def tools: Registry[Ctx]

  /** What is asked for the next move, with the model to ask already chosen. */
  def model: Model.Fixed[ApplicationError.AdapterError]

  /** The caller every tool call is confined to, and which the model is never told about. */
  def context: Ctx

  /** The most turns the model may take before the run is refused. */
  def budget: Int = 16

  /**
   * Seed the conversation, or take it one step further.
   *
   * @return the transition; aborts when the model, a tool, or the registry does
   */
  final def next: State => IO[ApplicationError, Next] =
    case Step.Init(question)     => ZIO.succeed(Step.Continue(opening(question)))
    case Step.Continue(messages) => advance(messages)

  /**
   * The conversation a question starts with.
   *
   * @param question what was asked
   * @return the standing instructions, then the question
   */
  private def opening(question: String): Chunk[Message] = Chunk(
    Message.System(Chunk(Message.Content.Text(systemPrompt))),
    Message.User(Chunk(Message.Content.Text(question))),
  )

  /**
   * Do whatever the conversation is waiting for.
   *
   * What that is comes from the messages rather than from anything this carries, so a run picked up from a
   * checkpoint takes the same step the run that wrote it would have taken next.
   *
   * @param messages the conversation so far
   * @return the next step; aborts when the model or a tool does
   */
  private def advance(messages: Chunk[Message]): IO[ApplicationError, Next] =
    Progress.from(messages) match
      case Progress.Finished(answer)       => ZIO.succeed(Step.Done(answer))
      case Progress.AwaitingTools(pending) => answer(messages, pending)
      case Progress.Empty                  => ask(messages)
      case Progress.AwaitingModel          => ask(messages)

  /**
   * Ask the model what to do next, offering what this caller may use.
   *
   * @param messages the conversation so far
   * @return the conversation with the model's turn appended; aborts with [[Basic.Exhausted]] when the
   *         model has already had its budget of turns, and with the model's own error when it refuses
   */
  private def ask(messages: Chunk[Message]): IO[ApplicationError, Next] =
    if turns(messages) >= budget
    then ZIO.fail(Basic.Exhausted(name, budget))
    else
      for
        session    <- tools.forSession(context)
        completion <- model.complete(Model.Request(messages, session.advertised))
      yield Step.Continue(messages :+ Message.fromCompletion(completion))

  /**
   * Run the calls the model is waiting on.
   *
   * The session is built again rather than carried, so what a caller may use is decided afresh each time
   * and a permission withdrawn mid-run is a tool that stops answering.
   *
   * @param messages the conversation so far
   * @param pending the calls with no result yet
   * @return the conversation with one answer per call; aborts when a tool cannot say whether it permits
   *         this caller
   */
  private def answer(messages: Chunk[Message], pending: NonEmptyChunk[Tool.Call.Raw]): IO[ApplicationError, Next] =
    for
      session  <- tools.forSession(context)
      outcomes <- session.dispatchAll(pending.toChunk)
    yield Step.Continue(messages ++ outcomes.map(answered))

  /**
   * One tool's answer, as the conversation carries it.
   *
   * @param outcome what dispatch produced
   * @return the message to append
   */
  private def answered(outcome: Outcome): Message = Message.fromOutcome(outcome)

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
}


object Basic:

  /**
   * A run stopped because the model had taken every turn it was allowed.
   *
   * A [[ApplicationError.DomainError]] rather than a defect: the budget is a constraint a caller chose, and
   * raising it and asking again is a reasonable answer.
   *
   * @param agent which agent ran out
   * @param budget how many turns it was allowed
   */
  final case class Exhausted(agent: String, budget: Int) extends ApplicationError.DomainError:

    /**
     * What went wrong, for whoever set the budget.
     *
     * @return the reason, naming the agent and its budget
     */
    override def message: String = s"agent '$agent' took its $budget turns without answering"
