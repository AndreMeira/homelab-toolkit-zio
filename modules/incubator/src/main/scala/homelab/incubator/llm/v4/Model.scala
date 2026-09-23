package homelab.incubator.llm.v4


import homelab.common.error.ApplicationError
import zio.json.ast.Json
import zio.{ Chunk, IO, UIO, ZIO }


/**
 * One call to a chat-completions model: messages in, the model's next move out.
 *
 * The shape follows the chat-completions API a gateway normalises, which is what makes a tool call a
 * structured request rather than prose. Absent is everything around the call — the loop, prompt
 * construction, memory, and running the tools the model asks for. One request in, one completion out.
 *
 * The failure type is a parameter so an adapter names its own refusals rather than squeezing them into a
 * set this port fixed. The bound keeps a shared vocabulary: a caller that knows nothing of `E` can still
 * ask whether a failure is an [[ApplicationError.TransientError]] and retry on that alone.
 *
 * @tparam E what this model's adapter fails with
 */
trait Model[+E <: ApplicationError.AdapterError] {
  self =>

  /**
   * Send a conversation and read what the model does next.
   *
   * @param request the conversation, the tools on offer, and the model to ask
   * @return what the model said, asked for, and cost; aborts with what the adapter refuses on
   */
  def complete(request: Model.Request): IO[E, Model.Completion]

  /**
   * This model, falling back to another when it refuses.
   *
   * The result fails only when both do, and its failure type is the union of theirs, so a chain across
   * providers keeps what each of them can say. The first refusal is logged before the second is asked,
   * since the value carries only the failure that ended the chain.
   *
   * @param other the model to ask when this one refuses
   * @tparam E2 what the other model's adapter fails with
   * @return a model asking this one first
   */
  def orElse[E2 <: ApplicationError.AdapterError](other: Model[E2]): Model[E | E2] =
    new Model[E | E2] {
      override def complete(request: Model.Request): IO[E | E2, Model.Completion] =
        self.complete(request).tapError(self.reportFallback).orElse(other.complete(request))
    }

  /**
   * Write a refusal that a fallback is about to hide.
   *
   * @param error what this model refused with
   * @return noop once logged
   */
  private def reportFallback(error: ApplicationError.AdapterError): UIO[Unit] =
    ZIO.logWarning(s"model refused, falling back: ${error.message}")
}


object Model {

  /**
   * Why the model stopped.
   *
   * [[Stop]] and [[ToolCalls]] are the two a loop acts on; the rest end a turn without an answer it can
   * carry forward. [[Other]] keeps a reason this type does not name, since a gateway fronts many providers
   * and the set is theirs rather than ours.
   */
  enum FinishReason:

    /** The model finished what it was saying. */
    case Stop

    /** The model asked for tools and is waiting on their results. */
    case ToolCalls

    /** The model ran out of room before it finished. */
    case Length

    /** The provider stopped the model on its own policy. */
    case ContentFilter

    /**
     * A reason this type does not name.
     *
     * @param reason what the provider called it
     */
    case Other(reason: String)

  /**
   * What a call consumed and what it cost.
   *
   * The cost is on the value rather than in a log because a caller that cannot read it cannot hold a budget
   * or attribute spend to a conversation. It is absent where the provider does not report one, which a
   * budget reads differently from a charge of nothing.
   *
   * @param promptTokens what was sent
   * @param completionTokens what came back
   * @param cost what the provider charged, in the account's currency, where it says
   */
  final case class Usage(promptTokens: Int, completionTokens: Int, cost: Option[BigDecimal])

  /**
   * One call's worth of conversation.
   *
   * The tools are carried as the objects a provider expects rather than as anything this type models: they
   * come from a session, which decides what a caller may use, and reach the wire unchanged. `extra` is the
   * same idea for the request body — temperature, routing preferences, a provider's own options — so a
   * caller is not held to what is modelled here.
   *
   * @param model which model to ask, as the gateway names it
   * @param messages the conversation so far, oldest first
   * @param tools the tool objects to advertise, or empty to offer none
   * @param extra fields merged into the request body by the adapter
   */
  final case class Request(
    model: String,
    messages: Chunk[Message],
    tools: List[Json] = Nil,
    extra: Json.Obj = Json.Obj(),
  )

  /**
   * What the model did with a request.
   *
   * @param content what it said
   * @param calls what it asked to have run, which is empty unless `finish` is [[FinishReason.ToolCalls]]
   * @param finish why it stopped
   * @param usage what the call consumed and cost
   */
  final case class Completion(
    content: Chunk[Message.Content],
    calls: Chunk[Tool.Call],
    finish: FinishReason,
    usage: Usage,
  )
}
