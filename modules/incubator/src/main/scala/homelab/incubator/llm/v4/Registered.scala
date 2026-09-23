package homelab.incubator.llm.v4


import homelab.common.error.ApplicationError
import homelab.incubator.llm.v4.schema.JsonSchema
import zio.json.ast.Json
import zio.schema.Schema
import zio.schema.codec.JsonCodec
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
 * @tparam Ctx the caller context
 * @tparam In the arguments the model chooses
 * @tparam Out what the tool produces
 */
final class Registered[Ctx, In: Schema, Out: Schema] private[v4] (
  tool: Tool[Ctx, In, Out],
  jsonSchema: JsonSchema,
) {

  private val arguments = summon[Schema[In]]

  /** The name the model calls it by. */
  def name: String = tool.name

  /**
   * Whether this caller may use it.
   *
   * @param context the caller context
   * @return true when it is available to this caller; aborts when the tool cannot say
   */
  def permits(context: Ctx): IO[ApplicationError, Boolean] = tool.permits(context)

  /**
   * Run this tool for one caller against a call a model made.
   *
   * Everything the model could react to becomes its text: arguments that did not parse, and an abort, which
   * reaches the model as [[Registry.Withheld]] and an operator as a log line.
   *
   * @param context the caller context
   * @param call the call, whose arguments are the JSON the model wrote
   * @return the outcome; never fails
   */
  def invoke(context: Ctx, call: Tool.Call): UIO[Outcome] =
    decode(call.arguments) match
      case Left(reason) => ZIO.succeed(Outcome(call.id, Tool.Result.failure[Out](reason)))
      case Right(input) =>
        tool.handle(context, input).tapError(report).fold(withheld(call.id), answered(call.id))

  /**
   * This tool as the provider expects to receive it.
   *
   * @return the `{"type":"function","function":{…}}` object for a request's `tools` array
   */
  def advertised: Json = Json.Obj(
    "type"     -> Json.Str("function"),
    "function" -> Json.Obj(
      "name"        -> Json.Str(tool.name),
      "description" -> Json.Str(tool.description),
      "parameters"  -> jsonSchema.json,
    ),
  )

  /**
   * Read the arguments a model wrote.
   *
   * @param written the JSON the model produced for this call
   * @return the decoded arguments, or what to tell the model about its own JSON
   */
  private def decode(written: String): Either[String, In] =
    JsonCodec.jsonDecoder(arguments).decodeJson(written).left.map(prefix)

  /**
   * Say that a decode failure was about the arguments.
   *
   * @param reason what the decoder reported
   * @return the same reason, placed
   */
  private def prefix(reason: String): String = s"arguments did not parse: $reason"

  /**
   * The outcome for a call the tool answered.
   *
   * @param callId the id the model gave this call
   * @param result what the tool returned
   * @return the outcome
   */
  private def answered(callId: Tool.Call.Id)(result: Tool.Result[Out]): Outcome = Outcome(callId, result)

  /**
   * The outcome for a call the tool aborted on, carrying what the model is told instead of the detail.
   *
   * @param callId the id the model gave this call
   * @param error what the tool aborted with, already logged
   * @return the outcome
   */
  private def withheld(callId: Tool.Call.Id)(error: ApplicationError): Outcome =
    Outcome(callId, Tool.Result.failure[Out](Registry.Withheld))

  /**
   * Write an abort where an operator can read it, since the model is told only that it happened.
   *
   * @param error what the tool aborted with
   * @return noop once logged
   */
  private def report(error: ApplicationError): UIO[Unit] =
    ZIO.logError(s"tool '${tool.name}' aborted: ${error.message}")
}
