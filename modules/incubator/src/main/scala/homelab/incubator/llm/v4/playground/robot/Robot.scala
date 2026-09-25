package homelab.incubator.llm.v4.playground.robot

import homelab.common.error.ApplicationError
import homelab.common.processing.Workflow
import homelab.common.processing.Workflow.Step
import homelab.llm.*
import zio.*


/**
 * One task, driven to an end: the GPT-Policy loop written with the toolkit's pieces.
 *
 * A step is an observation, one model call, and whatever the model asked for. What differs from a
 * text-only agent is that the observation is rebuilt from the rig every step rather than carried forward —
 * a camera frame from six steps ago says nothing about where the arm is now — so the transcript holds the
 * decisions and the results, and the *current* state arrives fresh each time.
 *
 * Unimplemented where it touches hardware. What it is for is to see which of the toolkit's pieces fit; the
 * places they do not are written down as they come up.
 *
 * [[Progress.from]] is deliberately not used. It reads a turn that called no tools as the end of a
 * conversation, which is right for a text agent and wrong here: this run ends when a terminal tool is
 * called, so a turn of prose is a step that did nothing.
 *
 * @param model what is asked for the next move
 * @param tools the capabilities on offer, bound to a rig per run
 * @param rig the hardware this run is bound to
 * @param observe what the rig looks like right now, in the words the model reads
 * @param budget the most decisions a run may take before it is stopped and the arm parked
 */
final class Robot(
  model: Model[ApplicationError.AdapterError],
  tools: Registry[Rig],
  rig: Rig,
  observe: Observation,
  budget: Int,
) extends Workflow[Any, ApplicationError, Robot.Task, Robot.State, Robot.Ending] {
  private type State = Step.Current[Robot.Task, Robot.State]
  private type Next  = Step.Next[Robot.State, Robot.Ending]

  override val name: String = "policy"

  /**
   * Seed a run, or take one more decision.
   *
   * @return the next step; aborts when the model call or the rig does
   */
  def next: State => IO[ApplicationError, Next] =
    case Step.Init(task)    => seed(task)
    case Step.Continue(run) => advance(run)

  /**
   * Begin: the instructions the rig imposes, then what was asked for.
   *
   * @param task what the operator wants done
   * @return the first state to advance from
   */
  private def seed(task: Robot.Task): UIO[Next] =
    Step.Continue.succeed {
      val opening = Chunk(
        Message.System(Chunk(Message.Content.Text(instructions))),
        Message.User(Chunk(Message.Content.Text(task.instruction))),
      )
      Robot.State(task, opening, decisions = 0)
    }

  /**
   * Take one decision: look, ask, act.
   *
   * @param state what has happened so far
   * @return the next step; aborts when the model call or a tool does
   */
  private def advance(state: Robot.State): IO[ApplicationError, Next] =
    if state.decisions >= budget
    then Step.Done.succeed(Robot.Ending.Exhausted(state.decisions))
    else
      for
        sighting   <- observe.now(rig)
        session    <- tools.forSession(rig)
        asked       = state.transcript :+ Message.User(Chunk(Message.Content.Text(sighting)))
        completion <- model.complete(state.task.model, Model.Request(asked, session.advertised))
        step       <- act(state, asked, session, completion)
      yield step

  /**
   * Do what the model asked, and read off the outcomes whether it asked to stop.
   *
   * Every call is dispatched, the one that ends a run included — it does nothing but hand its argument
   * back, and [[ending]] reads that value. A turn that called no tools has answered in words, which for a
   * physical task is not an ending: the run ends when the model says so through a tool, so a turn of prose
   * is a step that did nothing and the loop goes round. That is the opposite of a text agent, where prose
   * is exactly how a run ends.
   *
   * @param state what has happened so far
   * @param asked the transcript including the observation just sent
   * @param session the tools this run may use
   * @param completion what the model returned
   * @return the next step; aborts when a tool does
   */
  private def act(
    state: Robot.State,
    asked: Chunk[Message],
    session: Session[Rig],
    completion: Model.Completion,
  ): IO[ApplicationError, Next] =
    for
      outcomes <- session.dispatchAll(completion.calls.toList)
      answers   = Chunk.fromIterable(outcomes).map(answered)
      appended  = asked ++ (Message.fromCompletion(completion) +: answers)
      taken     = state.decisions + 1
    yield outcomes.flatMap(ending).headOption match
      case Some(RigTools.Ending.Done(_, _))        => Step.Done(Robot.Ending.Concluded(taken))
      case Some(RigTools.Ending.GiveUp(reason, _)) => Step.Done(Robot.Ending.GaveUp(taken, reason))
      case None                                    => Step.Continue(state.copy(transcript = appended, decisions = taken))

  /**
   * One tool's answer, as the conversation carries it.
   *
   * @param outcome what dispatch produced
   * @return the message to append
   */
  private def answered(outcome: Outcome): Message = Message.fromOutcome(outcome)

  /**
   * The ending an outcome carries, when it carries one.
   *
   * Read off what the model asked for rather than off anything a tool produced, and by type rather than by
   * name — so the loop needs no list of names, and the tool that ends a run needs no result worth having.
   *
   * @param outcome what dispatch produced
   * @return the ending, or nothing when this outcome was some other tool's
   */
  private def ending(outcome: Outcome): Option[RigTools.Ending] = outcome.call match
    case Tool.Call.Decoded(_, _, asked: RigTools.Terminate) => Some(asked.ending)
    case _                                                  => None

  /**
   * What the rig imposes on every run, in the words the model reads first.
   *
   * @return the standing instructions
   */
  private def instructions: String =
    (s"${rig.calibration.frame}\n" +: rig.calibration.hazards.map(hazard => s"- $hazard")).mkString("\n")
}


object Robot:

  /**
   * What a run was asked to do.
   *
   * @param instruction what the operator wants done, in their words
   * @param model which model to ask
   */
  final case class Task(instruction: String, model: Model.Name)

  /**
   * What a run has done so far.
   *
   * The transcript grows by whole turns; the rig's current state is not in it, because it is re-read every
   * step rather than remembered.
   *
   * @param task what was asked
   * @param transcript the decisions and their results, oldest first
   * @param decisions how many have been taken
   */
  final case class State(task: Task, transcript: Chunk[Message], decisions: Int)

  /**
   * How a run ended.
   *
   * None of these is a claim that the task physically succeeded: the model's conclusion is a conclusion,
   * and a person assigns the outcome after the arm has stopped.
   */
  enum Ending:

    /**
     * The model called the tool that says it is done.
     *
     * @param decisions how many it took
     */
    case Concluded(decisions: Int)

    /**
     * The model called the tool that says it cannot be.
     *
     * @param decisions how many it took
     * @param reason why not, in the model's words
     */
    case GaveUp(decisions: Int, reason: String)

    /**
     * The budget ran out first.
     *
     * @param decisions how many were allowed
     */
    case Exhausted(decisions: Int)


/**
 * What the rig looks like right now, in the words the model reads.
 *
 * Unimplemented. A real one reads the arm's measured pose and the cameras, and renders both plus the
 * calibration into the text of one observation. It is a port rather than a method on [[Rig]] because what
 * a model is told about a scene is a prompt-engineering decision that changes far more often than the
 * hardware does.
 */
trait Observation:

  /**
   * Look at the rig.
   *
   * @param rig what to look at
   * @return what it looks like, as the model will read it; aborts when a camera or the arm does not answer
   */
  def now(rig: Rig): IO[ApplicationError, String]
