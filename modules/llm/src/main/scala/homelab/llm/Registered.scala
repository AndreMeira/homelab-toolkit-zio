package homelab.llm


import homelab.common.error.ApplicationError
import homelab.llm.schema.JsonSchema
import zio.schema.Schema
import zio.{ IO, UIO, ZIO }


/**
 * A tool with its wire concerns resolved: its arguments described, its codecs to hand.
 *
 * It holds the tool rather than closures over it, so every method here reads as a call to the thing that
 * owns the behaviour. The types stay as parameters and a registry holds `Registered[Ctx, ?, ?]`, which costs
 * nothing: an outcome is an outcome whatever the tool's types were, so there is never anything to recover.
 *
 * @param tool what was registered
 * @param jsonSchema its arguments as the model is told them
 * @tparam Ctx what the caller supplies — see [[Tool]]
 * @tparam In the arguments the model chooses
 * @tparam Out what the tool produces
 */
final class Registered[Ctx, In, Out: Schema] private[llm] (
  tool: Tool[Ctx, In, Out],
  jsonSchema: JsonSchema,
) {

  /** The name the model calls it by. */
  def name: String = tool.name

  /**
   * Whether this caller may use it.
   *
   * @param context the caller's context
   * @return true when it is available to this caller; aborts when the tool cannot say
   */
  def permits(context: Ctx): IO[ApplicationError, Boolean] = tool.permits(context)

  /**
   * Read a call's arguments without running anything.
   *
   * The same reading [[invoke]] does, offered on its own so a caller can see what the model asked for
   * before deciding what to do about it — branch on an argument, record it, or refuse a call on grounds the
   * tool itself knows nothing about. Nothing is permitted, nothing is handled, nothing is written; a caller
   * that reads and then invokes pays for the parse twice.
   *
   * The arguments come back as `In`, which a holder of this knows. One reaching it through a [[Registry]]
   * holds `Registered[Ctx, ?, ?]` and learns only whether the JSON parsed — so branching on an argument
   * means holding the [[Tool]], where [[Tool.decoded]] answers the same question without a registration in
   * the way.
   *
   * @param call the call, whose arguments are the JSON the model wrote
   * @return the arguments as the tool would receive them, or what the model would be told about its own JSON
   */
  def decoded(call: Tool.Call.Raw): Either[String, In] = tool.decoded(call.arguments)

  /**
   * Run this tool for one caller against a call a model made.
   *
   * Everything the model could react to becomes its text: arguments that did not parse, and an abort, which
   * reaches the model as [[Registry.Withheld]] and an operator as a log line.
   *
   * @param context the caller's context
   * @param call the call, whose arguments are the JSON the model wrote
   * @return the outcome; never fails
   */
  def invoke(context: Ctx, call: Tool.Call.Raw): UIO[Outcome] =
    tool.decoded(call.arguments) match
      case Left(reason) => ZIO.succeed(Outcome(call, Tool.Result.failure[Out](reason)))
      case Right(input) =>
        val read = Tool.Call.Decoded(call.id, call.name, input)
        tool.handle(context, input).tapError(report).fold(withheld(read), answered(read))

  /**
   * This tool as a provider is told about it.
   *
   * The parts rather than a rendering of them, because providers do not agree on the rendering and the one
   * that does the telling is the one that knows which.
   *
   * @return what to advertise
   */
  def advertised: Advertised = Advertised(tool.name, tool.description, jsonSchema)

  /**
   * The outcome for a call the tool answered.
   *
   * @param call the call, with its arguments read
   * @param result what the tool returned
   * @return the outcome
   */
  private def answered(call: Tool.Call[In])(result: Tool.Result[Out]): Outcome = Outcome(call, result)

  /**
   * The outcome for a call the tool aborted on, carrying what the model is told instead of the detail.
   *
   * @param call the call, with its arguments read
   * @param error what the tool aborted with, already logged
   * @return the outcome
   */
  private def withheld(call: Tool.Call[In])(error: ApplicationError): Outcome =
    Outcome(call, Tool.Result.failure[Out](Registry.Withheld))

  /**
   * Write an abort where an operator can read it, since the model is told only that it happened.
   *
   * @param error what the tool aborted with
   * @return noop once logged
   */
  private def report(error: ApplicationError): UIO[Unit] =
    ZIO.logError(s"tool '${tool.name}' aborted: ${error.message}")
}
