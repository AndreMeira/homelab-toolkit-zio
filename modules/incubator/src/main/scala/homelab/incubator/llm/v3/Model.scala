package homelab.incubator.llm.v3


import homelab.common.error.ApplicationError
import zio.*
import zio.json.ast.Json


/**
 * One call to a chat-completions model: messages in, the model's next move out.
 *
 * The shape follows the chat-completions API a gateway normalises, which is what makes a tool call a
 * structured request rather than prose. What is deliberately absent is everything around the call — the
 * loop, prompt construction, memory, and running the tools the model asks for. One request in, one
 * completion out.
 */
trait Model {

  /**
   * Send a conversation and read what the model does next.
   *
   * @param request the conversation, the tools on offer, and the model to ask
   * @return what the model said, asked for, and cost; aborts with an
   *         [[ApplicationError.TransientError]] where a retry is worth making, an
   *         [[ApplicationError.UnauthorisedError]] where the credentials are refused, an
   *         [[ApplicationError.DecodingError]] where the body does not parse, and an
   *         [[ApplicationError.AdapterError]] otherwise
   */
  def complete(request: Model.Request): IO[ApplicationError, Model.Completion]
}


object Model {

  /**
   * A piece of a message.
   *
   * Text is the part a loop reads. Anything else a provider accepts in a message — an image, a document, a
   * cache marker — travels as [[Raw]] and reaches the wire as it was written, so a caller can use what a
   * provider offers without waiting for it to be modelled here.
   */
  enum Content:

    /**
     * Words.
     *
     * @param text what they are
     */
    case Text(text: String)

    /**
     * A content part this type does not model, carried as the provider will receive it.
     *
     * @param json the part, as it goes on the wire
     */
    case Raw(json: Json)

  /**
   * A turn of the conversation, one-to-one with what a request carries.
   *
   * @see [[Model.Request.messages]]
   */
  enum Message:

    /**
     * Instructions that hold for the conversation.
     *
     * @param content what they are
     */
    case System(content: Chunk[Content])

    /**
     * What the caller said.
     *
     * @param content what they said
     */
    case User(content: Chunk[Content])

    /**
     * What the model said, and what it asked to have run.
     *
     * @param content what it said, which is empty when it only asked for tools
     * @param calls what it asked for, which is empty when it only answered
     */
    case Assistant(content: Chunk[Content], calls: Chunk[Tool.Call])

    /**
     * What a tool answered, paired to the call that asked.
     *
     * @param callId the id of the call this answers
     * @param content the answer, as the model will read it
     * @param standing what the answer leaves outstanding, which no provider has a field for and an adapter
     *                 drops on the way out
     */
    case ToolResult(
      callId: String,
      content: Chunk[Content],
      standing: Tool.Result.Standing = Tool.Result.Standing.Answered,
    )

  object Message:

    /**
     * What the model said, as the conversation carries it.
     *
     * A turn goes back as it came — words and calls both — so what the model reads next is what it wrote.
     * Why it stopped and what it cost are facts about the call rather than about the turn, and stay behind.
     *
     * @param completion what the model returned
     * @return the assistant turn to append
     */
    def said(completion: Completion): Assistant = Assistant(completion.content, completion.calls)

    /**
     * What a tool answered, as the conversation carries it.
     *
     * The text is whatever dispatch rendered, a encodeFailure included — everything a model can react to reaches
     * it as words. What the result leaves outstanding travels with it, for a reader of the conversation
     * rather than for a provider: see [[ToolResult.standing]].
     *
     * @param outcome what dispatch produced
     * @return the tool turn to append
     */
    def answered(outcome: Tool.Outcome): ToolResult =
      val rendered = outcome.result.render
      ToolResult(outcome.callId, Chunk(Content.Text(rendered.text)), standingOf(rendered))

    /**
     * What a dispatched result leaves outstanding.
     *
     * @param result what dispatch produced
     * @return its standing; a encodeFailure started nothing, so it owes nothing
     */
    private def standingOf(result: Tool.Result[String]): Tool.Result.Standing = result match
      case Tool.Result.Succeeded(_, standing) => standing
      case Tool.Result.Failed(_)              => Tool.Result.Standing.Answered

    /**
     * One whole turn: what the model said, and an answer to every call it made.
     *
     * The protocol needs a result for each `tool_call_id` before the next model call, so the pair is built
     * together rather than appended separately. The order is the model's: its turn first, then the outcomes
     * as dispatch returned them.
     *
     * @param completion what the model returned
     * @param outcomes one outcome per call the completion asked for
     * @return the messages to append, in order
     */
    def turn(completion: Completion, outcomes: Chunk[Tool.Outcome]): Chunk[Message] =
      said(completion) +: outcomes.map(answered)

  /**
   * Why the model stopped.
   *
   * [[Stop]] and [[ToolCalls]] are the two a loop acts on; the rest end a turn without an answer it can
   * carry forward. [[Other]] covers a reason this type does not name, since a gateway fronts many providers
   * and the set is theirs rather than ours.
   */
  enum FinishReason:
    case Stop, ToolCalls, Length, ContentFilter, Other

  /**
   * What a call consumed and what it cost.
   *
   * The cost is on the value rather than in a log because a caller that cannot read it cannot hold a budget
   * or attribute spend to a conversation.
   *
   * @param promptTokens what was sent
   * @param completionTokens what came back
   * @param cost what the provider charged, in the account's currency
   */
  final case class Usage(promptTokens: Int, completionTokens: Int, cost: BigDecimal)

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
   * @param extra fields merged into the request body; a key this type already models is refused rather than
   *              overwritten
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
    content: Chunk[Content],
    calls: Chunk[Tool.Call],
    finish: FinishReason,
    usage: Usage,
  )
}
