package homelab.incubator.llm.v3.playground


import homelab.common.error.ApplicationError
import homelab.common.processing.Workflow
import homelab.common.processing.Workflow.Step
import homelab.incubator.llm.v3.{ Model, Tool }
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
 * `Transcript.progress` is deliberately not used. It reads a turn that called no tools as the end of a
 * conversation, which is right for a text agent and wrong here: this run ends when a terminal tool is
 * called, so a turn of prose is a step that did nothing.
 *
 * @param model what is asked for the next move
 * @param tools the capabilities on offer, bound to a rig per run
 * @param rig the hardware this run is bound to
 * @param observe what the rig looks like right now, in the words the model reads
 * @param budget the most decisions a run may take before it is stopped and the arm parked
 */
final class Policy(
  model: Model,
  tools: Tool.Registry[Rig],
  rig: Rig,
  observe: Observation,
  budget: Int,
) extends Workflow[Any, ApplicationError, Policy.Task, Policy.Run, Policy.Ending] {

  override val name: String = "policy"

  /**
   * Seed a run, or take one more decision.
   *
   * @return the next step; aborts when the model call or the rig does
   */
  def next: Step.Pending[Policy.Task, Policy.Run] => IO[ApplicationError, Step[Policy.Task, Policy.Run, Policy.Ending]] =
    case Step.Init(task)    => seed(task)
    case Step.Continue(run) => advance(run)

  /**
   * Begin: the instructions the rig imposes, then what was asked for.
   *
   * @param task what the operator wants done
   * @return the first state to advance from
   */
  private def seed(task: Policy.Task): UIO[Step.Continue[Policy.Run]] =
    val opening = Chunk(
      Model.Message.System(Chunk(Model.Content.Text(instructions))),
      Model.Message.User(Chunk(Model.Content.Text(task.instruction))),
    )
    ZIO.succeed(Step.Continue(Policy.Run(task, opening, decisions = 0)))

  /**
   * Take one decision: look, ask, act.
   *
   * @param run what has happened so far
   * @return the next step; aborts when the model call or a tool does
   */
  private def advance(run: Policy.Run): IO[ApplicationError, Step[Policy.Task, Policy.Run, Policy.Ending]] =
    if run.decisions >= budget then ZIO.succeed(Step.Done(Policy.Ending.Exhausted(run.decisions)))
    else
      for
        sighting   <- observe.now(rig)
        session    <- tools.forSession(rig)
        asked       = run.transcript :+ Model.Message.User(Chunk(Model.Content.Text(sighting)))
        completion <- model.complete(Model.Request(run.task.model, asked, session.advertised))
        step       <- act(run, asked, session, completion)
      yield step

  /**
   * Do what the model asked, and read off the outcomes whether it asked to stop.
   *
   * The ending is the value one tool returned, tested for by type after everything has run — so the loop
   * needs no list of names, and a tool that ends the run is dispatched like any other, which is harmless
   * because that tool does nothing but hand its argument back. The result reaches the transcript rendered;
   * the loop reads it before that, while it is still the value the tool made. A turn that called no tools has answered in
   * words, which for a physical task is not an ending: the run ends when the model says so through a tool,
   * so a turn of prose is a step that did nothing and the loop goes round. That is the opposite of a text
   * agent, where prose is exactly how a run ends.
   *
   * @param run what has happened so far
   * @param asked the transcript including the observation just sent
   * @param session the tools this run may use
   * @param completion what the model returned
   * @return the next step; aborts when a tool does
   */
  private def act(
    run: Policy.Run,
    asked: Chunk[Model.Message],
    session: Tool.Session[Rig],
    completion: Model.Completion,
  ): IO[ApplicationError, Step[Policy.Task, Policy.Run, Policy.Ending]] =
    for
      outcomes <- session.dispatchAll(completion.calls.toList)
      appended  = asked ++ Model.Message.turn(completion, Chunk.fromIterable(outcomes))
      taken     = run.decisions + 1
    yield outcomes.collectFirst { case Tool.Outcome(_, Tool.Result.Succeeded(e: RigTools.Ending, _)) => e } match
      case Some(RigTools.Ending.Done(_, _))        => Step.Done(Policy.Ending.Concluded(taken))
      case Some(RigTools.Ending.GiveUp(reason, _)) => Step.Done(Policy.Ending.GaveUp(taken, reason))
      case None                                    => Step.Continue(run.copy(transcript = appended, decisions = taken))

  /**
   * What the rig imposes on every run, in the words the model reads first.
   *
   * @return the standing instructions
   */
  private def instructions: String =
    (s"${rig.calibration.frame}\n" +: rig.calibration.hazards.map(hazard => s"- $hazard")).mkString("\n")
}


object Policy:

  /**
   * What a run was asked to do.
   *
   * @param instruction what the operator wants done, in their words
   * @param model which model to ask
   */
  final case class Task(instruction: String, model: String)

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
  final case class Run(task: Task, transcript: Chunk[Model.Message], decisions: Int)

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
