package homelab.incubator.llm.v4


import homelab.common.error.ApplicationError
import homelab.incubator.llm.v4.Tool.Result.errorEncoder
import homelab.incubator.llm.v4.schema.{ Encoder, JsonSchema, Shape }
import zio.schema.codec.JsonCodec
import zio.schema.{ DeriveSchema, Schema }
import zio.{ IO, ZIO }


/**
 * A capability the model may invoke: what it is called, what it is for, and what it does. Both halves carry
 * the `zio.schema.Schema` that reads and writes them — the arguments a model fills in, and the value a
 * result is rendered from — so nothing downstream names those types again to describe or decode them.
 *
 * The trust boundary runs through the arguments. `Input` is what the *model* chooses, and is the only half
 * described to it; `Ctx` is what the *caller* supplies — the user, the tenant, the namespace this call must be
 * confined to — and never appears in a schema. A prompt injection cannot set what the model was never offered.
 *
 * @tparam Ctx the caller context, joined to the model's arguments in [[handle]]
 * @tparam Input the arguments the model chooses, described to it from its schema
 * @tparam Output the result, which reaches the model as text written by its schema
 */
trait Tool[Ctx, Input: Schema, Output: Schema] {
  self =>

  /** The name the model calls it by; unique within the registry it is added to. */
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
   * The same tool, asking a different caller context for what it needs.
   *
   * A registry is bound to one `Ctx`, so a tool written against a narrower one — an arm rather than a whole
   * rig, a tenant rather than a session — joins it by saying how to reach its own from the registry's. The
   * narrowing travels with the tool rather than being repeated at each call, and `permits` is narrowed with
   * it, so a tool cannot end up authorised against one context and run against another.
   *
   * @param fn how to reach this tool's context from the wider one
   * @tparam Ctx2 the context the result accepts
   * @return the same behaviour, over `Ctx2`
   */
  def contramapCtx[Ctx2](fn: Ctx2 => Ctx): Tool[Ctx2, Input, Output] = new Tool[Ctx2, Input, Output] {
    override def name: String        = self.name
    override def description: String = self.description

    override def permits(context: Ctx2): IO[ApplicationError, Boolean] =
      self.permits(fn(context))

    override def handle(context: Ctx2, input: Input): IO[ApplicationError, Tool.Result[Output]] =
      self.handle(fn(context), input)
  }

  /**
   * A registry holding this tool and one more, which is where a chain of tools begins.
   *
   * Both tools accept the same caller context, so what the chain yields is a registry one session can be
   * bound to. Registration happens here as it does in [[Registry.add]]: either tool whose arguments cannot
   * be described is set aside rather than held, and named when the rejections are read.
   *
   * @param other the tool to register alongside this one
   * @tparam In2 the arguments the model chooses for `other`
   * @tparam Out2 what `other` produces
   * @return a registry holding both, or holding a rejection for either
   */
  def +[In2: Schema, Out2: Schema](other: Tool[Ctx, In2, Out2]): Registry[Ctx] =
    Registry.add(self).add(other)

  /**
   * Read arguments a model wrote as the type this tool takes.
   *
   * Offered here because this is where the schema is: a caller holding this tool knows what `Input` is, so
   * it can see what was asked before anything happens — to branch on an argument, or to act on a call whose
   * answer is beside the point. Nothing is permitted and nothing is run; this only reads.
   *
   * @param arguments the JSON the model wrote for one call
   * @return the arguments as [[handle]] would receive them, or what the model is told about its own JSON
   */
  def decoded(arguments: String): Either[String, Input] =
    JsonCodec.jsonDecoder(summon[Schema[Input]]).decodeJson(arguments).left.map(Tool.unparsed)

  /**
   * Run the tool — where the untrusted and the trusted halves of the arguments meet.
   *
   * @param context the caller context, supplied by the session
   * @param input the arguments the model chose
   * @return what it produced; aborts only on failures the *model* cannot do anything about
   */
  def handle(context: Ctx, input: Input): IO[ApplicationError, Tool.Result[Output]]
}


object Tool:

  /**
   * Say that a decode failure was about the arguments.
   *
   * @param reason what the decoder reported
   * @return the same reason, placed
   */
  private[v4] def unparsed(reason: String): String = s"arguments did not parse: $reason"

  /**
   * A tool call as the model emitted it — `arguments` is a JSON *string*, and a model wrote it.
   *
   * @param id what the model called this call, echoed back so it can pair the result with the request
   * @param name which tool it is asking for, which may be one that does not exist
   * @param arguments the JSON it wrote, unparsed and unchecked
   */
  final case class Call(id: Call.Id, name: String, arguments: String)

  object Call:

    /**
     * What the model called one of its calls, echoed back to pair a result with the request that asked.
     *
     * A subtype of `String`, so it reads, compares and renders as the text the provider sent. It is named
     * because a call carries three pieces of text and only this one is an identity — nothing else may
     * stand where it does.
     */
    opaque type Id <: String = String

    object Id:

      /**
       * An id as the model wrote it.
       *
       * @param value the text the provider sent
       * @return the id
       */
      def apply(value: String): Id = value

  /**
   * A tool whose name and description are given rather than overridden.
   *
   * What a tool *is* — two strings — and what it *does* end up beside each other, instead of separated by
   * the ceremony of restating a signature the enclosing type already fixed.
   *
   * @param name the name the model calls it by
   * @param description what it is for, in the words the model reads
   * @tparam Ctx the caller context
   * @tparam Input the arguments the model chooses
   * @tparam Output what it produces
   */
  trait Definition[Ctx, Input: Schema, Output: Schema](
    override val name: String,
    override val description: String,
  ) extends Tool[Ctx, Input, Output]

  object Definition:

    /**
     * A tool from a name, a description and what it does.
     *
     * @param name the name the model calls it by
     * @param description what it is for, in the words the model reads
     * @param fn what it does, given the caller's context and the model's arguments
     * @tparam Ctx the caller context
     * @tparam Input the arguments the model chooses
     * @tparam Output what it produces
     * @return the tool
     */
    def apply[Ctx, Input: Schema, Output: Schema](
      name: String,
      description: String,
    )(
      fn: (ctx: Ctx) => (input: Input) => IO[ApplicationError, Tool.Result[Output]]
    ): Definition[Ctx, Input, Output] = new Definition[Ctx, Input, Output](name, description) {
      override def handle(context: Ctx, input: Input): IO[ApplicationError, Tool.Result[Output]] =
        fn(context)(input)
    }

  /**
   * What a tool produced: the value, or the reason it could not.
   *
   * Both reach the model as text, which [[render]] writes, and a loop looking for a particular value can
   * match the success rather than read it back out of that text.
   *
   * @tparam A what a successful value is
   */
  enum Result[+A: Schema as schema]:

    /**
     * The tool ran.
     *
     * @param value what it produced
     */
    case Succeeded(value: A)(using Schema[A])

    /**
     * The tool did not run, or ran and could not answer.
     *
     * @param reason what the model is told, and what a provider carrying an error flag marks
     */
    case Failed(reason: String)(using Schema[A])

    /**
     * Whether a provider that can mark an errored result should mark this one.
     *
     * @return true when the tool could not answer
     */
    def failed: Boolean = this match
      case Succeeded(_) => false
      case Failed(_)    => true

    /**
     * This result as the text a conversation carries.
     *
     * A success is its value, written by the schema it carries. A failure is the `{isError, reason}`
     * envelope — put on here, and only here, so a reason stays plain prose until the moment it becomes
     * something a model reads.
     *
     * @return the text to append as the tool's reply
     */
    def render: String = this match
      case result: Succeeded[A] => JsonCodec.jsonEncoder(schema).encodeJson(result.value).toString
      case result: Failed[A]    => errorEncoder.encodeJson(Result.Error(true, result.reason)).toString

  object Result {

    /** What writes a failure. Built once, like the schema it comes from. */
    /** What writes a failure. Built once, from a schema derived once. */
    private val errorEncoder = JsonCodec.jsonEncoder(DeriveSchema.gen[Error])

    /**
     * A failure as the model reads it: a flag it can branch on, and what went wrong.
     *
     * @param isError always true; what a provider with an error flag would set,
     *                carried in the text for one that has none
     *
     * @param reason  what could not be done
     */
    final private case class Error(isError: Boolean, reason: String)

    /**
     * A failure holding the plain reason, before anything renders it.
     *
     * The type parameter is the result the tool would have produced, so a failure sits where a success of
     * that type would have — including where nothing was produced at all, since a call that could not parse
     * its arguments still belongs to the tool it was aimed at. Nothing is enveloped here: [[Result.render]]
     * does that, once.
     *
     * @param reason what could not be done, in words the model can act on
     * @tparam A what the call would have produced
     * @return the failure, unrendered
     */
    private[v4] def failure[A: Schema](reason: String): Result[A] = Failed(reason)

    /**
     * The result of a tool that ran.
     *
     * @param value what the tool produced
     * @tparam A what the value is
     * @return the result
     */
    def success[A: Schema](value: A): Result[A] = Result.Succeeded(value)

  }

  /**
   * Why a tool's arguments cannot be advertised.
   *
   * An [[ApplicationError.EncodingError]]: the outbound direction failing, and nothing at runtime recovers
   * from it — a tool whose arguments cannot be described cannot exist as written, so the fix is to change
   * the type rather than to handle this.
   *
   * The two ways it happens are named on the companion, and a reader wants them apart: the type is outside
   * the describable subset, or it describes something that is not an object.
   *
   * @param reason what is wrong, in the words whoever has to change the type reads
   */
  final case class InvalidInputSchema(reason: String) extends ApplicationError.EncodingError:

    /**
     * What went wrong, for whoever has to change the type.
     *
     * @return the reason
     */
    override def message: String = reason

  object InvalidInputSchema:

    /**
     * The type is outside the describable subset, so there is no schema to check.
     *
     * @param reason what the derivation refused, and why
     * @return the failure
     */
    def notDescribable(reason: String): InvalidInputSchema =
      InvalidInputSchema(s"arguments cannot be described: $reason")

    /**
     * The type describes something a `parameters` object cannot be.
     *
     * @param shape what it described instead
     * @return the failure
     */
    def wrongShape(shape: Shape): InvalidInputSchema =
      InvalidInputSchema(s"arguments must describe an object, not ${label(shape)}")

    /**
     * The type describes a reference the document does not define, so its shape is unknown.
     *
     * @param name the definition the root points at
     * @return the failure
     */
    def danglingReference(name: String): InvalidInputSchema =
      InvalidInputSchema(s"arguments refer to '$name', which the schema does not define")

    /**
     * A shape as a reader of the message would name it.
     *
     * @param shape the shape met
     * @return its name, without the structure hanging off it
     */
    private def label(shape: Shape): String = shape match
      case Shape.Text(_)           => "a string"
      case Shape.Number            => "a number"
      case Shape.Integer           => "an integer"
      case Shape.Bool              => "a boolean"
      case Shape.Null              => "null"
      case Shape.Enumeration(_, _) => "an enumeration"
      case Shape.Obj(_)            => "an object"
      case Shape.Arr(_)            => "an array"
      case Shape.AnyOf(_, _, _)    => "a union"
      case Shape.Reference(name)   => s"a reference to '$name'"

  /**
   * Describe a tool's arguments and check they can be advertised, which is the whole of what registration
   * asks of a type.
   *
   * Goes through [[Encoder]] rather than the derivation directly, so a type carrying its own description is
   * described the way it says.
   *
   * @tparam A the arguments the model chooses
   * @return the schema to advertise, or why this type cannot be one
   */
  def validateInput[A: Encoder as encoder]: Either[InvalidInputSchema, JsonSchema] =
    encoder.get match
      case Right(described)  => validateInputSchema(described)
      case Left(unsupported) => Left(InvalidInputSchema.notDescribable(unsupported.message))

  /**
   * Check the wire's one structural demand on a tool's `parameters`: it describes an object, because
   * arguments are named. A tool taking a bare string or array has nowhere to put it.
   *
   * @param described what a tool's arguments derived to
   * @return the same schema when its root is an object, or through a reference to one; the reason otherwise
   */
  private def validateInputSchema(described: JsonSchema): Either[InvalidInputSchema, JsonSchema] =
    described.root.shape match
      case Shape.Obj(_)          => Right(described)
      case Shape.Reference(name) =>
        described.definitions.get(name).map(resolved => resolved.shape) match
          case Some(Shape.Obj(_)) => Right(described)
          case Some(other)        => Left(InvalidInputSchema.wrongShape(other))
          case None               => Left(InvalidInputSchema.danglingReference(name))
      case other                 => Left(InvalidInputSchema.wrongShape(other))
