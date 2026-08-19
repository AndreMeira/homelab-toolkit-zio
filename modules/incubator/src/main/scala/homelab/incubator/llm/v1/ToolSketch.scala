package homelab.incubator.llm.v1

import homelab.common.data.Codec.{ Decoder, Encoder }
import homelab.common.error.ApplicationError
import zio.*

import java.nio.charset.StandardCharsets.UTF_8


/**
 * The tool-calling half of an agent loop, reduced to what it actually needs.
 *
 * Two ideas, and everything else follows from them.
 *
 * '''Dispatch needs no GADT.''' A model asks for a tool by *name*, with arguments as an opaque JSON string,
 * and the registry answering holds tools of unrelated argument and result types. The result type never
 * escapes [[Registry.Session.invoke]] — a tool's output goes straight back into the conversation as text — so
 * binding the existential's type variables in one polymorphic helper is enough.
 *
 * '''A tool's arguments straddle a trust boundary.''' The model supplies `A`; the caller supplies `Ctx` — the
 * user, tenant, or namespace the call must be confined to. Only `A` is described by [[Tool.schema]], so the
 * trusted half is *unaskable* rather than validated: a prompt injection cannot set what the model was never
 * offered. The two are joined inside [[Tool.handle]].
 *
 * What this leaves out: schema derivation (`schema` is hand-written here; a `zio.schema.Schema[A]` would
 * derive both it and the decoder), the model client, and the loop itself (a `Workflow`).
 */
object ToolSketch {

  /**
   * One capability the model may invoke, scoped to a caller context.
   *
   * `Ctx` is contravariant, so a tool needing no context (`Tool[Any, A, B]` — a calculator) sits in the same
   * registry as one needing the full caller (`Tool[Caller, A, B]` — anything touching stored data).
   *
   * @tparam Ctx the caller context the tool runs under — never serialised, never advertised
   * @tparam A the arguments, decoded from the model's JSON
   * @tparam B the result, encoded back into the conversation
   */
  trait Tool[-Ctx, A, B]:

    /** The name the model calls it by; unique within a [[Registry]]. */
    def name: String

    /**
     * The JSON Schema advertised to the model — describing `A` and nothing else. Anything the caller supplies
     * belongs in `Ctx`, out of reach of the model.
     */
    def schema: String

    /** Reads the model's arguments. A failure is fed back to the model, not raised — see [[Outcome]]. */
    def decoder: Decoder[A]

    /** Writes the result back into the conversation. */
    def encoder: Encoder[B]

    /**
     * Whether this context may use this tool at all. A tool it may not use is one it is never *told* about
     * ([[Registry.Session.schemas]] filters on this), and dispatch checks again anyway.
     *
     * @param context the caller context
     * @return true when the tool is available to this caller
     */
    def permits(context: Ctx): Boolean = true

    /**
     * Run the tool — the one place the untrusted and trusted halves of the arguments meet.
     *
     * @param context the caller context, supplied by the session
     * @param args the arguments the model chose
     * @return the result; aborts only on failures the *model* cannot do anything about
     */
    def handle(context: Ctx, args: A): IO[ApplicationError, B]

  /** A tool call as the model emitted it. */
  final case class Call(id: String, name: String, arguments: String)

  /**
   * What a dispatch produced, from the conversation's point of view.
   *
   * Refusals, undecodable arguments and tool failures are all *data*: things the model should see and can
   * react to. Only failures it could do nothing about belong in an error channel.
   *
   * @param callId the id the model gave this call, echoed back so it can pair request with result
   * @param content the text appended to the conversation as the tool's reply
   */
  final case class Outcome(callId: String, content: String)

  /**
   * The tools an application offers, before any caller is known.
   *
   * @param tools the registered tools, keyed by [[Tool.name]]
   * @tparam Ctx the caller context every tool here accepts
   */
  final class Registry[Ctx](tools: Map[String, Tool[Ctx, ?, ?]]):

    /**
     * Bind the registry to one caller. Nothing can dispatch without doing this, so a tool cannot run unscoped.
     *
     * @param context the caller context every dispatch will carry
     * @return the tools this caller may use, ready to dispatch
     */
    def forSession(context: Ctx): Registry.Session[Ctx] =
      new Registry.Session(tools.filter((_, tool) => tool.permits(context)), context)

  object Registry:

    /**
     * Build a registry, keyed by each tool's own name.
     *
     * @param tools the tools to expose
     * @tparam Ctx the caller context every tool accepts
     * @return the registry
     */
    def of[Ctx](tools: Tool[Ctx, ?, ?]*): Registry[Ctx] =
      new Registry(tools.map(tool => tool.name -> tool).toMap)

    /**
     * A registry bound to one caller: the only thing that can run a tool.
     *
     * @param permitted the tools this caller may use — already filtered by [[Tool.permits]]
     * @param context the caller context handed to every [[Tool.handle]]
     * @tparam Ctx the caller context
     */
    final class Session[Ctx](permitted: Map[String, Tool[Ctx, ?, ?]], context: Ctx):

      /**
       * The schemas to advertise to the model — only of tools this caller may use, so a forbidden tool is not
       * refused, it is never offered.
       *
       * @return one JSON Schema per available tool
       */
      def schemas: List[String] = permitted.values.map(_.schema).toList

      /**
       * Run one call, turning everything the model could react to into text it can read.
       *
       * A name outside `permitted` is refused here as well as hidden from [[schemas]] — the model may still
       * name one it guessed, or a resumed conversation may replay a call the caller has since lost access to.
       * That second check is the actual control; hiding is only what keeps it from trying.
       *
       * @param call the tool call the model asked for
       * @return the outcome to append to the conversation; never fails on the model's behalf
       */
      def dispatch(call: Call): UIO[Outcome] =
        permitted.get(call.name) match
          case None       => ZIO.succeed(Outcome(call.id, s"error: no tool '${call.name}' is available"))
          case Some(tool) =>
            invoke(tool, call.arguments)
              .map(Outcome(call.id, _))
              .catchAll(error => ZIO.succeed(Outcome(call.id, s"error: ${error.message}")))

      /**
       * Run several calls from one turn concurrently, keeping every outcome.
       *
       * The protocol needs *all* results before the next model call, so this never short-circuits: a tool that
       * fails yields an error outcome like any other.
       *
       * @param calls the calls the model asked for, in the order it asked
       * @param parallelism how many tools may run at once
       * @return one outcome per call, in the same order
       */
      def dispatchAll(calls: List[Call], parallelism: Int = 4): UIO[List[Outcome]] =
        ZIO.foreachPar(calls)(dispatch).withParallelism(parallelism)

      /**
       * Decode, run, encode — the one place the existential's type variables are bound, and the one place the
       * caller context meets the model's arguments.
       *
       * @param tool the tool to run
       * @param arguments the raw JSON the model produced
       * @tparam A the tool's argument type, recovered from the wildcard
       * @tparam B the tool's result type, erased again on the way out
       * @return the encoded result; aborts with the decode failure or whatever `handle` aborts with
       */
      private def invoke[A, B](tool: Tool[Ctx, A, B], arguments: String): IO[ApplicationError, String] =
        ZIO
          .fromEither(tool.decoder.decode(arguments.getBytes(UTF_8)))
          .flatMap(args => tool.handle(context, args))
          .map(result => new String(tool.encoder.encode(result), UTF_8))
}
