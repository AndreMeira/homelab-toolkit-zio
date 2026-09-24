package homelab.incubator.llm.v4.playground.robot

import homelab.common.error.ApplicationError
import homelab.incubator.llm.v4.Tool.Result
import homelab.incubator.llm.v4.{ Registry, Tool }
import zio.*
import zio.schema.annotation.discriminatorName
import zio.schema.{ Schema, derived }


/**
 * The capabilities a run offers the model, one `Tool` each.
 *
 * Every argument type here is what the model writes, and it is described to the model as a JSON Schema
 * derived from the type. What the model never sees is [[Rig]], which is where the arm is — so a call can
 * only ever reach the arm this run was bound to.
 *
 * Nothing is implemented: each handler reaches a port that aborts.
 */
object RigTools {

  /**
   * Move to one pose.
   *
   * @param target where to put the tool point
   * @param note why, in the model's own words, which is recorded and never read back
   */
  final case class MoveTo(target: Pose, note: String) derives Schema

  /**
   * Move through a path.
   *
   * @param poses the path, in order
   * @param note why, in the model's own words
   */
  final case class Follow(poses: List[Pose], note: String) derives Schema

  /**
   * Set the gripper.
   *
   * @param gripper zero closed, one open
   * @param note why, in the model's own words
   */
  final case class Grip(gripper: Double, note: String) derives Schema

  /**
   * Ask where a pixel is.
   *
   * @param camera which view
   * @param pixelXy the pixel, as x then y
   * @param against the same feature in an earlier step, when there is one
   * @param note why, in the model's own words
   */
  final case class Locate(
    camera: String,
    pixelXy: List[Double],
    against: Option[Localiser.Reference],
    note: String,
  ) derives Schema

  /**
   * How a run ends, as the model states it and as the loop reads it back.
   *
   * The same type goes in and comes out. In, it is what the model wrote, and the two cases carry different
   * required fields so each kind of ending has to be justified on its own terms. Out, it is what the loop
   * tests the outcome for — the one tool whose produced value the loop reads, since it is the one tool
   * whose result the loop has to act on rather than hand back.
   */
  @discriminatorName("kind")
  enum Ending derives Schema:

    /**
     * The task is done, in the model's judgement. A person assigns the outcome afterwards.
     *
     * @param summary what the fresh observations show
     * @param hindsight what it would do differently
     */
    case Done(summary: String, hindsight: String)

    /**
     * The task cannot be done.
     *
     * @param reason why not
     * @param hindsight what it would do differently
     */
    case GiveUp(reason: String, hindsight: String)

  /**
   * What a call that named no poses amounts to.
   *
   * @param message what was wrong
   */
  final private case class EmptyPath(message: String) extends ApplicationError.AdapterError

  /** The one pose a path must have; a path of none is a call the model got wrong. */
  private val emptyPath: ApplicationError = EmptyPath("a path needs at least one pose")

  /**
   * The poses of a call, once it is known to have named any.
   *
   * @param input the call's arguments
   * @return the path; aborts when the model asked for a move through nowhere
   */
  private def path(input: Follow): IO[ApplicationError, NonEmptyChunk[Pose]] =
    ZIO.fromOption(NonEmptyChunk.fromIterableOption(input.poses)).orElseFail(emptyPath)

  /** Move the tool point to one pose. */
  val moveTo: Tool[Rig, MoveTo, Arm.Reached] = Tool.Definition(
    "move_to",
    "Move to one absolute calibrated pose. The host solves and times the path.",
  ) { (ctx: Rig) => (input: MoveTo) =>
    ctx.arm.moveTo(input.target).map(Result.success)
  }

  /** Move the tool point through a path, in order. */
  val follow: Tool[Rig, Follow, Arm.Reached] = Tool.Definition(
    "move_eef_chunk",
    "Move through absolute calibrated poses in order. Every pose is preserved.",
  ) { (ctx: Rig) => (input: Follow) =>
    path(input).flatMap(ctx.arm.follow).map(Result.success)
  }

  /** Open or close the gripper. */
  val grip: Tool[Rig, Grip, Arm.Grip] = Tool.Definition(
    "set_gripper",
    "Set a normalised gripper opening: zero closed, one open.",
  ) { (ctx: Rig) => (input: Grip) =>
    ctx.arm.grip(input.gripper).map(Result.success)
  }

  /** Solve a path without moving, so reachability can be tested before acting. */
  val check: Tool[Rig, Follow, Arm.Solved] = Tool.Definition(
    "check_path",
    "Solve poses with the same planner and command nothing. Proves neither clearance nor tracking.",
  ) { (ctx: Rig) => (input: Follow) =>
    path(input).flatMap(ctx.arm.check).map(Result.success)
  }

  /** Turn a pixel into a direction, and into metres where two views allow it. */
  val locate: Tool[Rig, Locate, Localiser.Located] = Tool.Definition(
    "locate_point",
    "Resolve a visible pixel to a ray, and to a position when a second view of the same feature exists.",
  ) { (ctx: Rig) => (input: Locate) =>
    ctx.localiser.locate(input.camera, input.pixelXy, input.against).map(Result.success)
  }

  /**
   * The request that ends a run.
   *
   * An object holding the ending rather than the ending itself: a tool's arguments must be described as one
   * object, and a sum type is not one. This is the wire's rule, enforced at registration.
   *
   * @param ending how the run ends
   */
  final case class Terminate(ending: Ending) derives Schema

  /**
   * End the run. Nothing is done to the rig: the ending goes back as the produced value, and the loop reads
   * it there.
   */
  val terminate: Tool[Rig, Terminate, Ending] = Tool.Definition(
    "terminate",
    "End the task: done when fresh observations establish the outcome, give_up when it cannot be completed.",
  ) { (_: Rig) => (input: Terminate) =>
    ZIO.succeed(Result.success(input.ending))
  }

  /**
   * Every tool a run offers, registered.
   *
   * A value rather than an effect: a registry is built by adding to one, and what a session offers is what
   * this expression put there. A tool whose arguments cannot be described is carried as a rejection and
   * reported by [[Registry.forSession]], so the whole set is named at once rather than the first bad one.
   */
  val registry: Registry[Rig] = moveTo + follow + grip + check + locate + terminate
}
