package homelab.llm


import zio.json.ast.Json
import zio.{ Chunk, UIO, ZIO }

import scala.collection.immutable.ListMap


/**
 * A registry bound to one caller, and the only thing that runs a tool.
 *
 * @param permitted the tools this caller may use, already filtered
 * @param context the caller's context handed to every dispatch
 * @tparam Ctx what the caller supplies — see [[Tool]]
 */
final class Session[Ctx] private[llm] (permitted: ListMap[String, Registered[Ctx, ?, ?]], context: Ctx) {

  /**
   * The tools for a request — only what this caller may use, so a forbidden tool is not refused, it is
   * never offered.
   *
   * @return one per available tool, in the order they were registered
   */
  def advertised: Chunk[Advertised] = Chunk.fromIterable(permitted.values).map(advertise)

  /**
   * Run one call, turning everything the model could react to into text it can read.
   *
   * The name is checked again here, not merely hidden from [[advertised]]: a model may name a tool it
   * guessed, and a resumed conversation may replay a call whose caller has since lost access.
   *
   * @param call the tool call the model asked for
   * @return the outcome to append to the conversation; never fails on the model's behalf
   */
  def dispatch(call: Tool.Call.Raw): UIO[Outcome] =
    permitted.get(call.name) match
      case Some(tool) => tool.invoke(context, call)
      case None       => ZIO.succeed(Outcome(call, Tool.Result.failure[Unit](unavailable(call.name))))

  /**
   * Run several calls from one turn concurrently, keeping every outcome.
   *
   * The protocol needs an answer for every call id before the next model turn, so this never
   * short-circuits: a tool that fails yields an error outcome like any other.
   *
   * @param calls the calls the model asked for, in the order it asked
   * @param parallelism how many tools may run at once
   * @return one outcome per call, in the same order
   */
  def dispatchAll(calls: Chunk[Tool.Call.Raw], parallelism: Int = 4): UIO[Chunk[Outcome]] =
    ZIO.foreachPar(calls)(dispatch).withParallelism(parallelism)

  /**
   * One tool as a provider is told about it.
   *
   * @param registered the tool
   * @return what to advertise for it
   */
  private def advertise(registered: Registered[Ctx, ?, ?]): Advertised = registered.advertised

  /**
   * What the model is told when it names a tool this session does not offer.
   *
   * @param name the name the model wrote
   * @return the reason, naming it back
   */
  private def unavailable(name: String): String = s"no tool '$name' is available"
}


object Session:

  /**
   * A session over the tools one caller may use.
   *
   * Keying by name happens here rather than wherever the tools came from, so what a session looks a call up
   * by stays its own business and a caller hands it a plain list. The order is kept, so what
   * [[Session.advertised]] offers a model is the order the tools were given in.
   *
   * @param registered the tools this caller may use, already filtered
   * @param context the caller's context every dispatch will carry
   * @tparam Ctx what the caller supplies — see [[Tool]]
   * @return the session
   */
  def apply[Ctx](registered: Chunk[Registered[Ctx, ?, ?]], context: Ctx): Session[Ctx] =
    new Session(ListMap.from(registered.map(tool => tool.name -> tool)), context)
