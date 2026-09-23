package homelab.incubator.llm.v4.playground


import homelab.common.error.ApplicationError
import zio.*
import zio.schema.{ Schema, derived }


/**
 * The hardware a run is bound to, and the half of every tool call the model does not write.
 *
 * Read alongside `research/agent/gpt-policy.md`: this is GPT-Policy's tool surface expressed with the
 * toolkit's pieces, to see which of them fit and which are missing. Nothing here is implemented.
 *
 * The arm, the camera rig and the calibration are `Ctx` rather than arguments. A model that is never told
 * an arm exists cannot name a second one, which is the same reason a tenant id stays out of a schema — and
 * in a single-robot rig it buys nothing, since there is no second arm to address. It is kept because the
 * split costs nothing and the day there are two arms it is already right.
 *
 * @param arm what moves
 * @param localiser what turns a pixel into a direction, and sometimes into metres
 * @param calibration facts about the rig that no camera frame shows
 */
final case class Rig(arm: Arm, localiser: Localiser, calibration: Rig.Calibration)


object Rig:

  /**
   * What the model is told about the rig in words, because no image carries it.
   *
   * @param frame how a pose is to be read: axes, units, and where the tool point sits
   * @param hazards obstacles that are in the scene whether or not a camera shows them
   */
  final case class Calibration(frame: String, hazards: List[String])


/**
 * An absolute end-effector pose in the arm's own base frame.
 *
 * Seven numbers: position in metres, then a unit quaternion mapping the tool's axes into that frame. Plain
 * numbers rather than a named type, because this is what a model writes and nothing has checked it yet.
 *
 * @param poseXyzquat x, y, z, qx, qy, qz, qw
 */
final case class Pose(poseXyzquat: List[Double]) derives Schema


/**
 * What moves, and what reports where it got to.
 *
 * Unimplemented. A real one wraps a vendor SDK: it seeds inverse kinematics from the live joint positions,
 * densifies the path between poses, and retimes it without moving any pose the caller gave.
 */
trait Arm:

  /**
   * Move to one pose.
   *
   * @param pose where the tool point should end up
   * @return where it got to; aborts when the pose cannot be solved or the move does not complete
   */
  def moveTo(pose: Pose): IO[ApplicationError, Arm.Reached]

  /**
   * Move through several poses in the order given.
   *
   * @param poses the path, at least one pose long
   * @return where it got to; aborts when any pose cannot be solved or the move does not complete
   */
  def follow(poses: NonEmptyChunk[Pose]): IO[ApplicationError, Arm.Reached]

  /**
   * Open or close the gripper.
   *
   * @param opening zero closed, one open
   * @return what the encoder reports; aborts when the gripper does not reach it
   */
  def grip(opening: Double): IO[ApplicationError, Arm.Grip]

  /**
   * Solve a path without commanding anything.
   *
   * Answers reachability and nothing else: it does not prove the path is clear, and it does not prove the
   * arm would track it.
   *
   * @param poses the path to solve
   * @return what the solver made of it; aborts when the solver itself fails
   */
  def check(poses: NonEmptyChunk[Pose]): IO[ApplicationError, Arm.Solved]


object Arm:

  /**
   * Where a move ended.
   *
   * @param pose the measured pose at the end of the move
   * @param residualM how far it is from the pose that was asked for, in metres
   */
  final case class Reached(pose: Pose, residualM: Double) derives Schema

  /**
   * What the gripper reports.
   *
   * @param opening zero closed, one open
   */
  final case class Grip(opening: Double) derives Schema

  /**
   * What the solver made of a path.
   *
   * @param reachable whether every pose solved
   * @param firstFailure the index of the first pose that did not, when one did not
   */
  final case class Solved(reachable: Boolean, firstFailure: Option[Int]) derives Schema


/**
 * What turns a pixel into geometry.
 *
 * Unimplemented, and the most interesting tool in the set: a model cannot read depth off one RGB frame, so
 * this is the capability gap answered with a tool rather than with an instruction. One observation gives a
 * ray; a second of the same feature, with enough parallax, gives metres.
 */
trait Localiser:

  /**
   * Resolve a pixel.
   *
   * @param camera which view the pixel is in
   * @param pixelXy the pixel, as x then y
   * @param against the same feature seen in an earlier step, when there is one
   * @return the direction, and a position when two views allow one; aborts when the camera is unknown
   */
  def locate(
    camera: String,
    pixelXy: List[Double],
    against: Option[Localiser.Reference],
  ): IO[ApplicationError, Localiser.Located]


object Localiser:

  /**
   * The same feature, seen earlier.
   *
   * @param step which step saw it
   * @param pixelXy where it was in that view
   */
  final case class Reference(step: Int, pixelXy: List[Double]) derives Schema

  /**
   * What a pixel resolved to.
   *
   * @param rayXyz a unit direction in the arm's base frame
   * @param positionXyz metres in the arm's base frame, when two views allowed it
   */
  final case class Located(rayXyz: List[Double], positionXyz: Option[List[Double]]) derives Schema
