package homelab.llm.anthropic.response


import homelab.llm.anthropic.AnthropicError
import homelab.llm.{ Message, Model, Tool }
import zio.Chunk
import zio.json.*
import zio.json.ast.Json


/**
 * The Messages response, as Anthropic sends it.
 *
 * The content is a list of blocks, of which this adapter reads two kinds. The array is heterogeneous and
 * open — `text`, `tool_use`, `thinking`, and whatever is added next — and a block of a kind it does not
 * model has to reach the next turn exactly as it came, so [[CompletionResponse.Block]] keeps that one whole
 * rather than failing the whole body.
 *
 * Fields the API sends and nothing here uses — `id`, `type`, `role`, `model`, `stop_sequence` — are not
 * named, because a decoder that ignores them cannot be broken by one more arriving.
 *
 * @param content the blocks it produced, in order
 * @param stopReason why it stopped, absent while a stream is still open
 * @param usage what the call consumed
 */
@jsonMemberNames(SnakeCase)
final case class CompletionResponse(
  content: List[CompletionResponse.Block],
  stopReason: Option[String],
  usage: Option[CompletionResponse.Usage],
) derives JsonDecoder


object CompletionResponse:

  /**
   * One block of what the model produced.
   *
   * Two states of one block, named as [[Tool.Call]] names its two: [[Block.Raw]] is what arrived,
   * [[Block.Decoded]] is what it turned out to mean. The array is open, so a block of a kind this adapter
   * does not model stays raw and reaches the next turn as it came, rather than failing the whole body.
   */
  enum Block:

    /**
     * A block as it arrived, of a kind this adapter does not model.
     *
     * @param json the block
     */
    case Raw(json: Json)

    /**
     * A block read as one of the kinds this adapter acts on.
     *
     * @param kind what it turned out to be
     */
    case Decoded(kind: Block.Kind)

  object Block:

    /**
     * The kinds this adapter reads, told apart by the `type` every block carries.
     */
    @jsonDiscriminator("type")
    enum Kind derives JsonDecoder:

      /**
       * Words.
       *
       * @param text what they are
       */
      @jsonHint("text") case Text(text: String)

      /**
       * What the model asked to have run.
       *
       * @param id what the model named the call
       * @param name which tool it asked for
       * @param input the arguments it wrote, as an object
       */
      @jsonHint("tool_use") case ToolUse(id: String, name: String, input: Json)

    /**
     * How a block is read: as one of the kinds above, or kept whole when it is none of them.
     *
     * Composed rather than written — the derived decoder does the reading, and this only says what to do
     * when it does not recognise a block.
     */
    given JsonDecoder[Block] =
      JsonDecoder[Json].map(json => json.as[Kind].fold(_ => Raw(json), Decoded.apply))

  /**
   * What the call consumed.
   *
   * There is no cost: Anthropic reports tokens and leaves the pricing to whoever holds the table, which is
   * why [[Model.Usage.cost]] is optional and absent here.
   *
   * @param inputTokens what was sent
   * @param outputTokens what came back
   */
  @jsonMemberNames(SnakeCase)
  final case class Usage(inputTokens: Int, outputTokens: Int) derives JsonDecoder

  /**
   * One completion, read out of a decoded body.
   *
   * @param response what the API sent
   * @return the completion; refuses a body that carries neither content nor a reason for stopping
   */
  def completion(response: CompletionResponse): Either[AnthropicError, Model.Completion] =
    response.content -> response.stopReason match {
      case Nil -> None       => Left(AnthropicError.Malformed("the response carried no content and no reason for stopping"))
      case content -> reason => Right(completed(content, reason, response.usage))
    }

  /**
   * One answer, as the toolkit holds it.
   *
   * Content and a reason are both optional on their own: a model that ran out of room before it said
   * anything produces none of the first, and one still being streamed has none of the second. Only a body
   * with neither is refused, by [[completion]].
   *
   * @param content the blocks it produced, which may be none
   * @param stopReason why it stopped, which may be absent
   * @param usage what the call consumed, absent on some replies
   * @return the completion
   */
  private def completed(
    content: List[Block],
    stopReason: Option[String],
    usage: Option[Usage],
  ): Model.Completion =
    Model.Completion(
      content = said(content),
      calls = requested(content),
      finish = stopped(stopReason),
      usage = consumed(usage),
    )

  /**
   * What the model said.
   *
   * Everything but a call, since a call is read separately — and a block this adapter does not model is
   * kept as it came, which is what a conversation carrying reasoning back to the next turn needs.
   *
   * @param blocks what it produced
   * @return its content, as the conversation carries it
   */
  private def said(blocks: List[Block]): Chunk[Message.Content] =
    Chunk.fromIterable(blocks.map(part).flatten)

  /**
   * One block, as content.
   *
   * @param block what the API sent
   * @return the part, or nothing when the block is a call, which [[requested]] reads instead
   */
  private def part(block: Block): Option[Message.Content] = block match
    case Block.Decoded(Block.Kind.Text(said))       => Some(Message.Content.Text(said))
    case Block.Decoded(Block.Kind.ToolUse(_, _, _)) => None
    case Block.Raw(json)                            => Some(Message.Content.Raw(json))

  /**
   * What the model asked to have run.
   *
   * The arguments arrive as an object and the toolkit holds the string a model wrote, so they are written
   * back out — a round trip through text, which is what a call carries until something decodes it.
   *
   * @param blocks what it produced
   * @return the calls, raw
   */
  private def requested(blocks: List[Block]): Chunk[Tool.Call.Raw] =
    Chunk.fromIterable(blocks.collect(asked))

  /**
   * One call, as the toolkit carries it.
   *
   * @return the calls among the blocks that were read
   */
  private def asked: PartialFunction[Block, Tool.Call.Raw] = {
    case Block.Decoded(Block.Kind.ToolUse(id, name, input)) =>
      Tool.Call.Raw(Tool.Call.Id(id), name, input.toJson)
  }

  /**
   * Why the model stopped.
   *
   * The names differ from the chat-completions protocol's and mean the same things, so they are translated
   * rather than kept: `end_turn` is a stop, `tool_use` is a turn waiting on tools, `max_tokens` is running
   * out of room. A reason this type does not name is kept as the API spelled it, and a body without one is
   * one this adapter cannot explain — better said than resolved into a case.
   *
   * @param reason what the API called it, where it called it anything
   * @return the reason, named where it is one the toolkit knows
   */
  private def stopped(reason: Option[String]): Model.FinishReason = reason match
    case Some("end_turn")      => Model.FinishReason.Stop
    case Some("stop_sequence") => Model.FinishReason.Stop
    case Some("tool_use")      => Model.FinishReason.ToolCalls
    case Some("max_tokens")    => Model.FinishReason.Length
    case Some("refusal")       => Model.FinishReason.ContentFilter
    case Some(other)           => Model.FinishReason.Other(other)
    case None                  => Model.FinishReason.Other("none given")

  /**
   * What the call consumed.
   *
   * @param usage what the API sent
   * @return the usage, with no cost, which this API does not report
   */
  private def consumed(usage: Option[Usage]): Model.Usage = usage match
    case Some(reported) => Model.Usage(reported.inputTokens, reported.outputTokens, None)
    case None           => Model.Usage(0, 0, None)
