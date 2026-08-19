package homelab.incubator.llm.v2


import homelab.common.error.ApplicationError
import zio.*
import zio.json.ast.Json
import zio.schema.Schema
import zio.schema.codec.JsonCodec

import scala.collection.immutable.ListMap


/**
 * A capability the model may invoke: what it is called, what it is for, and what it does. Nothing about the
 * wire — no schema, no codecs — because a tool does not know it is being described to anyone. Those arrive at
 * [[Tool.Registry.add]], where the types are still concrete.
 *
 * The trust boundary runs through the arguments. `Input` is what the *model* chooses, and is the only half
 * described to it; `Ctx` is what the *caller* supplies — the user, the tenant, the namespace this call must be
 * confined to — and never appears in a schema. A prompt injection cannot set what the model was never offered.
 *
 * @tparam Ctx the caller context, joined to the model's arguments in [[handle]]
 * @tparam Input the arguments the model chooses
 * @tparam Output the result, which reaches the model as text
 */
trait Tool[Ctx, Input, Output] {

  /** The name the model calls it by; unique within a [[Tool.Registry]]. */
  def name: String

  /** What it is for, in the words the model reads before deciding to call it. */
  def description: String

  /**
   * Whether this caller may use this tool at all. One it may not use is one it is never told about.
   *
   * @param context the caller context
   * @return true when the tool is available to this caller
   */
  def permits(context: Ctx): Boolean = true

  /**
   * Run the tool — where the untrusted and the trusted halves of the arguments meet.
   *
   * @param context the caller context, supplied by the session
   * @param input the arguments the model chose
   * @return the result; aborts only on failures the *model* cannot do anything about
   */
  def handle(context: Ctx, input: Input): IO[ApplicationError, Output]
}


object Tool {

  /** A tool call as the model emitted it — `arguments` is a JSON *string*, and a model wrote it. */
  final case class Call(id: String, name: String, arguments: String)

  /**
   * What a dispatch produced, as the conversation sees it.
   *
   * @param callId the id the model gave this call, echoed back so it can pair request with result
   * @param content the text appended to the conversation as the tool's reply
   */
  final case class Outcome(callId: String, content: String)

  /**
   * Why a tool could not be registered.
   *
   * Registration failures are *construction* failures — known at boot, one per bad tool, worth reporting
   * together. They are deliberately not the same type as anything dispatch produces, because dispatch
   * produces no failures at all: everything a model could react to goes back to it as text.
   *
   * An [[ApplicationError.ImplementationError]] even though its cause is an
   * [[ApplicationError.EncodingError]], and the change of category is the point. The cause is a fact about a
   * type, which a caller might answer by choosing another representation. This is a tool that cannot exist as
   * written: nothing recovers from it, so it should be fixed rather than handled.
   *
   * @param tool the tool's name
   * @param cause what stopped it
   */
  final case class Rejected(tool: String, cause: JsonSchema.Unsupported) extends ApplicationError.ImplementationError:
    override def message: String = s"tool '$tool' cannot be registered: ${cause.message}"

  /**
   * A tool with its wire concerns resolved: schema derived, codecs captured, types gone.
   *
   * Binding `Input` and `Output` at registration is what removes the existential — the registry holds no
   * `Tool[Ctx, ?, ?]` and needs no type-recovering helper at dispatch, only a function from the caller and the
   * model's raw arguments to the text that goes back.
   *
   * @param name the name the model calls it by
   * @param description what it is for
   * @param schema the description of its arguments, sent as `parameters`
   * @param permits whether a given caller may use it
   * @param invoke decode, run, encode — with every model-actionable failure already rendered as text
   * @tparam Ctx the caller context
   */
  final case class Registered[Ctx](
    name: String,
    description: String,
    schema: JsonSchema,
    permits: Ctx => Boolean,
  )(
    logic: (Ctx, String) => UIO[String]
  ) {

    /**
     * 
     * @param ctx
     * @param arg
     * @return
     */
    def invoke(ctx: Ctx, arg: String): UIO[String] = logic(ctx, arg)

    /**
     * This tool as the provider expects to receive it.
     *
     * @return the `{"type":"function","function":{…}}` object for a request's `tools` array
     */
    def advertised: Json = Json.Obj(
      "type"     -> Json.Str("function"),
      "function" -> Json.Obj(
        "name"        -> Json.Str(name),
        "description" -> Json.Str(description),
        "parameters"  -> schema.json,
      ),
    )
  }

  /**
   * The tools an application offers, before any caller is known.
   *
   * Registration is where a tool meets the wire: the schema is derived once, the codecs are captured, and a
   * type outside the describable subset is refused *here*, at boot, rather than when a model calls.
   *
   * @param entries the registered tools, by name, in registration order
   * @tparam Ctx the caller context every tool here accepts
   */
  final class Registry[Ctx] private (entries: Ref[ListMap[String, Registered[Ctx]]]) {

    /**
     * Register a tool, deriving its schema and capturing its codecs.
     *
     * One `zio.schema.Schema` per side is all it takes: the advertised JSON Schema, the decoder that reads
     * what the model wrote, and the encoder that writes the result back are all derived from it. That is the
     * point — a codec derived *alongside* the schema rather than *from* it can disagree with it, and does:
     * a map described as an association list is decoded as a JSON object by an independently derived reader.
     *
     * @param tool the tool to register
     * @tparam In the arguments the model chooses
     * @tparam Out the result
     * @return noop once registered; aborts with [[Rejected]] if the arguments cannot be described
     */
    def add[In: Schema, Out: Schema](tool: Tool[Ctx, In, Out]): IO[Rejected, Unit] =
      for
        described <- ZIO.fromEither(JsonSchema.Encoder[In].get).mapError(Rejected(tool.name, _))
        _         <- ZIO.fromEither(namesItsArguments(described)).mapError(Rejected(tool.name, _))
        _         <- entries.update(_.updated(tool.name, register(tool, described)))
      yield ()

    /**
     * Bind the registry to one caller — the only thing that can run a tool, so nothing runs unscoped.
     *
     * @param context the caller context every dispatch will carry
     * @return the tools this caller may use
     */
    def forSession(context: Ctx): UIO[Session[Ctx]] =
      entries.get.map(all => new Session(all.filter((_, tool) => tool.permits(context)), context))

    /**
     * Close over a tool's types, leaving a value that knows nothing about them.
     *
     * @param tool the tool being registered
     * @param described its derived schema
     * @tparam In the arguments the model chooses
     * @tparam Out the result
     * @return the monomorphic entry the registry stores
     */
    private def register[In: Schema, Out: Schema](
      tool: Tool[Ctx, In, Out],
      described: JsonSchema,
    ): Registered[Ctx] =
      Registered(tool.name, tool.description, described, tool.permits)((context, arguments) =>
        ZIO
          .fromEither(JsonCodec.jsonDecoder(summon[Schema[In]]).decodeJson(arguments))
          .mapError(reason => s"arguments did not parse: $reason")
          .flatMap(input => tool.handle(context, input).mapError(_.message))
          .fold(reason => s"error: $reason", output => JsonCodec.jsonEncoder(summon[Schema[Out]]).encodeJson(output).toString)
      )

    /**
     * Check the wire's one structural demand on `parameters`: it describes an object, because arguments are
     * named. A tool taking a bare string or array has nowhere to put it.
     *
     * @param described the derived schema
     * @return noop when the root is an object; the reason otherwise
     */
    private def namesItsArguments(described: JsonSchema): Either[JsonSchema.Unsupported, Unit] =
      described.root.shape match
        case JsonSchema.Shape.Obj(_)          => Right(())
        case JsonSchema.Shape.Reference(name) =>
          described.definitions.get(name).map(_.shape) match
            case Some(JsonSchema.Shape.Obj(_)) => Right(())
            case _                             => Left(JsonSchema.Unsupported(s"'$name' does not resolve to an object"))
        case other                            => Left(JsonSchema.Unsupported(s"arguments must be an object, not $other"))
  }

  object Registry:

    /**
     * An empty registry.
     *
     * @tparam Ctx the caller context every tool will accept
     * @return the registry; never fails
     */
    def make[Ctx]: UIO[Registry[Ctx]] = Ref.make(ListMap.empty[String, Registered[Ctx]]).map(new Registry(_))

  /**
   * A registry bound to one caller.
   *
   * @param permitted the tools this caller may use, already filtered
   * @param context the caller context handed to every dispatch
   * @tparam Ctx the caller context
   */
  final class Session[Ctx](permitted: ListMap[String, Registered[Ctx]], context: Ctx) {

    /**
     * The `tools` array for a request — only what this caller may use, so a forbidden tool is not refused, it
     * is never offered.
     *
     * @return one function object per available tool
     */
    def advertised: List[Json] = permitted.values.map(_.advertised).toList

    /**
     * Run one call, turning everything the model could react to into text it can read.
     *
     * The name is checked again here, not merely hidden from [[advertised]]: a model may name a tool it
     * guessed, or a resumed conversation may replay a call whose caller has since lost access.
     *
     * @param call the tool call the model asked for
     * @return the outcome to append to the conversation; never fails on the model's behalf
     */
    def dispatch(call: Call): UIO[Outcome] =
      permitted.get(call.name) match
        case None       => ZIO.succeed(Outcome(call.id, s"error: no tool '${call.name}' is available"))
        case Some(tool) => tool.invoke(context, call.arguments).map(Outcome(call.id, _))

    /**
     * Run several calls from one turn concurrently, keeping every outcome.
     *
     * The protocol needs an answer for every `tool_call_id` before the next model call, so this never
     * short-circuits: a tool that fails yields an error outcome like any other.
     *
     * @param calls the calls the model asked for, in the order it asked
     * @param parallelism how many tools may run at once
     * @return one outcome per call, in the same order
     */
    def dispatchAll(calls: List[Call], parallelism: Int = 4): UIO[List[Outcome]] =
      ZIO.foreachPar(calls)(dispatch).withParallelism(parallelism)
  }
}
