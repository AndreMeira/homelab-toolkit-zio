package homelab.incubator.llm.v3


import homelab.common.error.ApplicationError
import homelab.incubator.llm.v2.JsonSchema
import zio.*
import zio.json.ast.Json
import zio.schema.{ DeriveSchema, Schema }
import zio.schema.codec.JsonCodec
import zio.schema.validation.Validation

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
   * Effectful because the answer is usually a lookup — a role, a grant, a flag someone else owns — and a
   * pure signature would force every one of those to be resolved into `Ctx` before anyone knows which
   * tools will be asked about.
   *
   * @param context the caller context
   * @return true when the tool is available to this caller; aborts when the answer cannot be established,
   *         which refuses the session rather than assuming either way
   */
  def permits(context: Ctx): IO[ApplicationError, Boolean] = ZIO.succeed(true)

  /**
   * Run the tool — where the untrusted and the trusted halves of the arguments meet.
   *
   * The result carries what it leaves outstanding alongside the value itself, so a tool that starts work
   * outliving the call names it here. [[Tool.Result.success]] is the rest of them.
   *
   * @param context the caller context, supplied by the session
   * @param input the arguments the model chose
   * @return the value and what it owes; aborts only on failures the *model* cannot do anything about
   */
  def handle(context: Ctx, input: Input): IO[ApplicationError, Tool.Result[Output]]
}


object Tool {

  /**
   * Check the wire's one structural demand on a tool's `parameters`: it describes an object, because
   * arguments are named. A tool taking a bare string or array has nowhere to put it.
   *
   * @param described what a tool's arguments derived to
   * @return the same schema when its root is an object, or through a reference to one; the reason otherwise
   */
  def validateInputSchema(described: JsonSchema): Either[JsonSchema.Unsupported, JsonSchema] =
    described.root.shape match
      case JsonSchema.Shape.Obj(_)          => Right(described)
      case JsonSchema.Shape.Reference(name) =>
        described.definitions.get(name).map(_.shape) match
          case Some(JsonSchema.Shape.Obj(_)) => Right(described)
          case _                             => Left(JsonSchema.Unsupported(s"'$name' does not resolve to an object"))
      case other                            => Left(JsonSchema.Unsupported(s"arguments must be an object, not $other"))

  /**
   * A tool call as the model emitted it —
   * `arguments` is a JSON *string*, and a model wrote it.
   */
  final case class Call(id: String, name: String, arguments: String)

  /**
   * What a tool produced: a value and what it leaves outstanding, or the reason it could not.
   *
   * The two are not the same shape, which is the point. A encodeFailure has nothing outstanding — it started
   * nothing, so there is nowhere on it to say otherwise — while a encodeSuccess carries what it owes. Both reach
   * the model as text either way, and a reader that only wants the text asks for [[text]].
   *
   * The parameter is what a encodeSuccess holds: the tool's own type before registration, and the rendering of it
   * after. Registration maps one to the other and changes nothing else.
   *
   * @tparam A what a successful value is
   */
  enum Result[A: Schema]:

    /**
     * The tool ran.
     *
     * @param value what it produced
     * @param standing what that value opens or closes
     */
    case Succeeded(value: A, standing: Result.Standing)(using Schema[A])

    /**
     * The tool did not run, or ran and could not answer.
     *
     * @param reason what the model is told, and what a provider carrying an error flag marks
     */
    case Failed(reason: String)(using Schema[A])

    /**
     * What reaches the model, whichever this is.
     *
     * @return the rendered value, or the reason it is missing
     */
    def text(using ev: A <:< String): String = this match
      case Succeeded(value, _) => ev(value)
      case Failed(reason)      => reason

    /**
     * Whether a provider that can mark an errored result should mark this one.
     *
     * @return true when the tool could not answer
     */
    def failed: Boolean = this match
      case Succeeded(_, _) => false
      case Failed(_)       => true

    /**
     * This result with its value written as the text a conversation carries.
     *
     * @param schema what writes the value
     * @tparam B the value's type, which a encodeSuccess holds
     * @return the same result, rendered
     */
    def render: Result[String] = this match
      case Succeeded(value, standing) => Result.encodeSuccess[A](value, standing)
      case Failed(reason)             => Result.encodeFailure(reason)

  object Result {

    /** How a encodeFailure is written. Named, and built once rather than per failed call. */
    private val errorSchema: Schema[Error] = DeriveSchema.gen[Error]

    /**
     * A encodeFailure as the model reads it: a flag it can branch on, and what went wrong.
     *
     * @param isError always true; what a provider with an error flag would set, carried in the text for one
     *                that has none
     * @param reason what could not be done
     */
    final private case class Error(isError: Boolean, reason: String)

    /**
     * A encodeFailure written as the text a conversation carries.
     *
     * Only [[Result.render]] calls this, so the envelope is put on once, at the point where a result
     * becomes text. A [[Failed]] before that holds the plain reason, which is what a loop inspecting a
     * encodeFailure wants to read.
     *
     * @param reason what could not be done
     * @return the encodeFailure, rendered
     */
    private[v3] def encodeFailure(reason: String): Result[String] =
      Failed(JsonCodec.jsonEncoder(errorSchema).encodeJson(Error(true, reason)).toString)

    /**
     * A successful result, written as the text a conversation carries.
     *
     * @param value what the tool produced
     * @param standing what that value opens or closes, which rendering leaves alone
     * @param schema what writes the value
     * @tparam A the value's type
     * @return the result, rendered
     */
    private[v3] def encodeSuccess[A](value: A, standing: Standing)(using schema: Schema[A]): Result[String] =
      Succeeded(JsonCodec.jsonEncoder(schema).encodeJson(value).toString, standing)

    /**
     * 
     * @param reason
     * @tparam A
     * @return
     */
    private[v3] def failure[A: Schema](reason: String): Result[A] = Failed(reason)

    /**
     *
     * @param reason
     * @tparam A
     * @return
     */
    private[v3] def error(reason: String): Result[String] = Failed(reason)

    /**
     * A result that owes nothing — what most tools return.
     *
     * @param value what the tool produced
     * @tparam A what the value is
     * @return the result, standing [[Standing.Answered]]
     */
    def success[A: Schema](value: A): Result[A] = Result.Succeeded(value, Standing.Answered)

    /**
     * What a result owes, once it has been read.
     *
     * A handle names work that outlives the call that started it. `Promised` says the call started some and
     * its result is not here; `Delivered` says this call carries the result of work named earlier. Reading
     * both over a conversation gives what is still outstanding: promised, less delivered.
     */
    enum Standing:

      /** The text is the whole result. */
      case Answered

      /**
       * The result is a reference to work still running.
       *
       * @param handles what was started
       */
      case Promised(handles: NonEmptyChunk[String])

      /**
       * The result carries work promised by an earlier call.
       *
       * @param handles what is now settled
       */
      case Delivered(handles: NonEmptyChunk[String])
  }

  /**
   * What a dispatch produced, as the tool produced it.
   *
   * The result is unrendered and its type is gone, but it carries what writes it — so `render` still works
   * without anyone naming the type again, and a loop that knows what it is looking for can match the value
   * instead of reading it back out of text. What the model reads is `result.render`.
   *
   * @param callId the id the model gave this call, echoed back so it can pair request with result
   * @param result what the tool returned, whether it succeeded, and what it leaves outstanding
   */
  final case class Outcome(callId: String, result: Result[?])

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
   * model's raw arguments to what goes back.
   *
   * @param name the name the model calls it by
   * @param description what it is for
   * @param schema the description of its arguments, sent as `parameters`
   * @param permits whether a given caller may use it
   * @param logic arguments and run, with every model-actionable encodeFailure turned into a result the model reads
   * @tparam Ctx the caller context
   */
  final case class Registered[Ctx](
    name: String,
    description: String,
    schema: JsonSchema,
    permits: Ctx => IO[ApplicationError, Boolean],
  )(
    logic: (Ctx, Call) => UIO[Outcome]
  ) {

    /**
     * Run this tool for one caller against a call a model made.
     *
     * @param context the caller context
     * @param call the call, whose arguments are the JSON the model wrote
     * @return the outcome, with anything the model could react to rendered as its text; never fails
     */
    def invoke(context: Ctx, call: Call): UIO[Outcome] = logic(context, call)

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
    def add[In: Schema, Out <: Matchable: Schema](tool: Tool[Ctx, In, Out]): IO[Rejected, Unit] =
      for
        described <- ZIO.fromEither(JsonSchema.Encoder[In].get).mapError(Rejected(tool.name, _))
        arguments <- ZIO.fromEither(validateInputSchema(described)).mapError(Rejected(tool.name, _))
        _         <- entries.update(_.updated(tool.name, register(tool, arguments)))
      yield ()

    /**
     * Bind the registry to one caller — the only thing that can run a tool, so nothing runs unscoped.
     *
     * Every tool is asked once, here, rather than at each dispatch: what a caller may use is decided when
     * the session is built, so the list advertised to the model and the list it may call are the same list.
     *
     * @param context the caller context every dispatch will carry
     * @return the tools this caller may use; aborts when a tool cannot say whether it permits this caller
     */
    def forSession(context: Ctx): IO[ApplicationError, Session[Ctx]] =
      for
        all     <- entries.get
        allowed <- ZIO.filter(all.values)(_.permits(context))
      yield new Session(ListMap.from(allowed.map(tool => tool.name -> tool)), context)

    /**
     * Close over a tool's types, leaving a value that knows nothing about them.
     *
     * Rendering changes the value and keeps the standing: what a result leaves outstanding is the tool's to
     * say, and no codec has an opinion about it.
     *
     * @param tool the tool being registered
     * @param described its derived schema
     * @tparam In the arguments the model chooses
     * @tparam Out the result
     * @return the monomorphic entry the registry stores
     */
    private def register[In: Schema, Out <: Matchable: Schema](
      tool: Tool[Ctx, In, Out],
      described: JsonSchema,
    ): Registered[Ctx] =
      Registered(tool.name, tool.description, described, tool.permits) { (context, call) =>
        {
          for {
            decoded = JsonCodec.jsonDecoder(summon[Schema[In]]).decodeJson(call.arguments) match
                        case Right(value) => Right(value)
                        case Left(reason) => Left(s"arguments did not parse: $reason")
            input  <- decoded match
                        case Right(input) => ZIO.succeed(input)
                        case Left(reason) => ZIO.fail(Outcome(call.id, Result.error(reason)))
            result <- tool
                        .handle(context, input)
                        .tapError(reportCallError(tool.name))
                        .mapError(_ => Outcome(call.id, Result.error(Registry.Withheld)))
          } yield Outcome(call.id, result)
        }.merge
      }

    /**
     * Write an abort where an operator can read it, since the model is told nothing about it.
     *
     * @param name the tool that aborted
     * @return what logs one such encodeFailure
     */
    private def reportCallError(name: String): ApplicationError => UIO[Unit] =
      error => ZIO.logError(s"tool '$name' aborted: ${error.message}")

  }

  object Registry:

    /**
     * What the model is told when a tool aborts.
     *
     * An abort is an [[ApplicationError]] raised by the tool's own dependencies, and its message is written
     * for an operator: a query that failed, a path, an upstream payload. The model is a reader of the
     * conversation this text lands in, so it gets the fact and not the detail, and the detail is logged.
     */
    val Withheld: String = "the tool could not complete"

    /**
     * An empty registry.
     *
     * @tparam Ctx the caller context every tool will accept
     * @return the registry; never fails
     */
    def make[Ctx]: UIO[Registry[Ctx]] =
      Ref.make(ListMap.empty[String, Registered[Ctx]]).map(new Registry(_))

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
        case None       => ZIO.succeed(Outcome(call.id, Result.error(s"no tool '${call.name}' is available")))
        case Some(tool) => tool.invoke(context, call)

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
