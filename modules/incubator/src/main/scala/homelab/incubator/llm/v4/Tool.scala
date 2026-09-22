package homelab.incubator.llm.v4

import homelab.common.error.ApplicationError
import homelab.incubator.llm.v3.Tool as V3Tool
import zio.{ IO, ZIO }


/**
 * A capability the model may invoke: what it is called, what it is for, and what it does. Nothing about the
 * wire — no schema, no codecs — because a tool does not know it is being described to anyone. Those arrive
 * at registration, where the types are still concrete.
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
   * Run the tool — where the untrusted and the trusted halves of the arguments meet.
   *
   * The result carries what it leaves outstanding alongside the value itself, so a tool that starts work
   * outliving the call names it here. `Result.success` is the rest of them.
   *
   * @param context the caller context, supplied by the session
   * @param input the arguments the model chose
   * @return the value and what it owes; aborts only on failures the *model* cannot do anything about
   */
  def handle(context: Ctx, input: Input): IO[ApplicationError, V3Tool.Result[Output]]
}