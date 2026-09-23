package homelab.incubator.llm.v4


import homelab.common.error.ApplicationError
import zio.schema.Schema
import zio.{ Chunk, IO, NonEmptyChunk, ZIO }

import scala.collection.immutable.ListMap


/**
 * The tools an application offers, before any caller is known.
 *
 * A value, not a handle: [[add]] answers with a new registry and leaves this one alone, so what a session is
 * built from is what the code holding it put there. Registration is where a tool meets the wire — the schema
 * is derived once and the codecs are captured — and a type outside the describable subset is set aside
 * rather than registered, to be reported by [[rejections]] or by [[forSession]].
 *
 * @param entries the registered tools, by name, in registration order
 * @param rejected the tools that could not be described, in the order they were added
 * @tparam Ctx the caller context every tool here accepts
 */
final class Registry[Ctx] private (
  entries: ListMap[String, Registered[Ctx, ?, ?]],
  rejected: Chunk[Registry.Rejected],
) {

  /**
   * Register a tool, deriving its schema and capturing its codecs.
   *
   * One `zio.schema.Schema` per side is all it takes: the advertised JSON Schema, the decoder that reads
   * what the model wrote, and the encoder that writes the result back all come from it. A tool whose
   * arguments cannot be described is set aside under its own name, so every such tool is named when the
   * rejections are read, not just the first.
   *
   * @param tool the tool to register
   * @tparam In the arguments the model chooses
   * @tparam Out what it produces
   * @return a registry holding this tool, or holding one more rejection
   */
  def add[In: Schema, Out: Schema](tool: Tool[Ctx, In, Out]): Registry[Ctx] =
    Tool.validateInput[In] match
      case Right(arguments) => new Registry(entries.updated(tool.name, Registered(tool, arguments)), rejected)
      case Left(cause)      => new Registry(entries, rejected :+ Registry.Rejected(tool.name, cause))

  /**
   * Register one more tool, continuing a chain begun by [[Tool.+]].
   *
   * The same registration as [[add]], spelled for a run of tools written as one expression.
   *
   * @param tool the tool to register
   * @tparam In the arguments the model chooses
   * @tparam Out what it produces
   * @return a registry holding this tool, or holding one more rejection
   */
  def +[In: Schema, Out: Schema](tool: Tool[Ctx, In, Out]): Registry[Ctx] = add(tool)

  /**
   * The tools that could not be described, which is what a boot check reads.
   *
   * @return one rejection per tool that was set aside, in the order they were added
   */
  def rejections: Chunk[Registry.Rejected] = rejected

  /**
   * Bind the registry to one caller — the only thing that can run a tool, so nothing runs unscoped.
   *
   * Every tool is asked once, here, rather than at each dispatch: what a caller may use is decided when the
   * session is built, so the list advertised to the model and the list it may call are the same list.
   *
   * @param context the caller context every dispatch will carry
   * @return the tools this caller may use; aborts with [[Registry.Incomplete]] when any tool was set aside,
   *         and with the tool's own error when one cannot say whether it permits this caller
   */
  def forSession(context: Ctx): IO[ApplicationError, Session[Ctx]] =
    NonEmptyChunk.fromChunk(rejected) match
      case Some(problems) => ZIO.fail(Registry.Incomplete(problems))
      case None           =>
        ZIO
          .filter(entries.values)(permitting(context))
          .map(allowed => new Session(ListMap.from(allowed.map(byName)), context))

  /**
   * Whether one registered tool is available to a caller.
   *
   * @param context the caller context
   * @param registered the tool to ask
   * @return true when it is available; aborts when the tool cannot say
   */
  private def permitting(context: Ctx)(registered: Registered[Ctx, ?, ?]): IO[ApplicationError, Boolean] =
    registered.permits(context)

  /**
   * One tool as the entry a session looks it up by.
   *
   * @param registered the tool
   * @return its name paired with it
   */
  private def byName(registered: Registered[Ctx, ?, ?]): (String, Registered[Ctx, ?, ?]) =
    registered.name -> registered
}


object Registry {

  /**
   * A registry holding one tool.
   *
   * @param tool the tool to register
   * @tparam Ctx the caller context every tool here accepts
   * @tparam In the arguments the model chooses
   * @tparam Out what it produces
   * @return a registry holding it, or holding a rejection for it
   */
  def add[Ctx, In: Schema, Out: Schema](tool: Tool[Ctx, In, Out]): Registry[Ctx] =
    Registry[Ctx](
      ListMap.empty[String, Registered[Ctx, ?, ?]],
      Chunk.empty[Registry.Rejected],
    ).add(tool)

  /**
   * What the model is told when a tool aborts.
   *
   * An abort is an [[ApplicationError]] raised by the tool's own dependencies, and its message is written
   * for an operator: a query that failed, a path, an upstream payload. The model reads the conversation this
   * text lands in, so it gets the fact and the detail is logged.
   */
  val Withheld: String = "the tool could not complete"

  /**
   * An empty registry.
   *
   * @tparam Ctx the caller context every tool will accept
   * @return the registry
   */
  def empty[Ctx]: Registry[Ctx] = new Registry(ListMap.empty, Chunk.empty)

  /**
   * Why a tool could not be registered.
   *
   * An [[ApplicationError.ImplementationError]] even though its cause is an
   * [[ApplicationError.EncodingError]], and the change of category is the point. The cause is a fact about a
   * type, which a caller might answer by choosing another representation. This is a tool that cannot exist
   * as written: nothing recovers from it, so it is fixed rather than handled.
   *
   * @param tool the tool's name
   * @param cause what stopped it
   */
  final case class Rejected(tool: String, cause: Tool.InvalidInputSchema):

    /**
     * What went wrong, for whoever has to change the tool.
     *
     * @return the reason, naming the tool
     */
    def message: String = s"tool '$tool' cannot be registered: ${cause.message}"

  /**
   * A registry asked for a session while holding tools it could not describe.
   *
   * Every rejection is carried rather than the first, because they are construction failures found together
   * and fixed together.
   *
   * @param problems what was set aside, in the order the tools were added
   */
  final case class Incomplete(problems: NonEmptyChunk[Rejected]) extends ApplicationError.ImplementationError:

    /**
     * Every rejection, one per line.
     *
     * @return the reasons, joined
     */
    override def message: String = problems.map(_.message).mkString("; ")

}
