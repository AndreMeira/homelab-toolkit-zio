package homelab.llm.anthropic.response


import homelab.llm.anthropic.AnthropicError
import homelab.llm.{ Message, Model, Tool }
import zio.Chunk
import zio.json.*
import zio.json.ast.Json


/**
 * The Messages response, as Anthropic sends it.
 *
 * The content is left as JSON rather than decoded into a type per block. The array is heterogeneous and
 * open — `text`, `tool_use`, `thinking`, and whatever is added next — and a block this adapter does not
 * read has to reach the next turn exactly as it came, which a type that never decoded its fields could not
 * do.
 *
 * Fields the API sends and nothing here uses — `id`, `type`, `role`, `model`, `stop_sequence` — are not
 * named, because a decoder that ignores them cannot be broken by one more arriving.
 *
 * @param content the blocks it produced, in order
 * @param stopReason why it stopped, absent while a stream is still open
 * @param usage what the call consumed
 */
@jsonMemberNames(SnakeCase)
final case class MessagesResponse(
  content: List[Json],
  stopReason: Option[String],
  usage: Option[MessagesResponse.Usage],
) derives JsonDecoder


object MessagesResponse:

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
  def completion(response: MessagesResponse): Either[AnthropicError, Model.Completion] =
    if response.content.isEmpty && response.stopReason.isEmpty then
      Left(AnthropicError.Malformed("the response carried no content and no reason for stopping"))
    else
      Right(
        Model.Completion(
          content = said(response.content),
          calls = requested(response.content),
          finish = stopped(response.stopReason),
          usage = consumed(response.usage),
        )
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
  private def said(blocks: List[Json]): Chunk[Message.Content] =
    Chunk.fromIterable(blocks.filterNot(isCall).map(part))

  /**
   * One block, as content.
   *
   * @param block what the API sent
   * @return the part
   */
  private def part(block: Json): Message.Content =
    if kind(block).contains("text") then
      field(block, "text") match
        case Some(Json.Str(text)) => Message.Content.Text(text)
        case _                    => Message.Content.Raw(block)
    else Message.Content.Raw(block)

  /**
   * What the model asked to have run.
   *
   * The arguments arrive as an object and the toolkit holds the string a model wrote, so they are written
   * back out — a round trip through text, which is what a call carries until something decodes it.
   *
   * @param blocks what it produced
   * @return the calls, raw
   */
  private def requested(blocks: List[Json]): Chunk[Tool.Call.Raw] =
    Chunk.fromIterable(blocks.filter(isCall).flatMap(asked))

  /**
   * One call, as the toolkit carries it.
   *
   * @param block the block the API sent
   * @return the call, or nothing when the block does not carry what a call needs
   */
  private def asked(block: Json): Option[Tool.Call.Raw] =
    for
      id    <- field(block, "id").collect { case Json.Str(value) => value }
      name  <- field(block, "name").collect { case Json.Str(value) => value }
      input <- field(block, "input")
    yield Tool.Call.Raw(Tool.Call.Id(id), name, input.toJson)

  /**
   * Whether a block is the model asking for a tool.
   *
   * @param block the block
   * @return true when it is a call
   */
  private def isCall(block: Json): Boolean = kind(block).contains("tool_use")

  /**
   * What kind of block this is, as the API labels it.
   *
   * @param block the block
   * @return its label, or nothing when it has none
   */
  private def kind(block: Json): Option[String] = field(block, "type").collect { case Json.Str(value) => value }

  /**
   * One field of a block.
   *
   * @param block the block
   * @param name which field
   * @return its value, or nothing when the block is not an object or does not have it
   */
  private def field(block: Json, name: String): Option[Json] = block match
    case Json.Obj(fields) => fields.collectFirst { case (key, value) if key == name => value }
    case _                => None

  /**
   * Why the model stopped.
   *
   * The names differ from the chat-completions protocol's and mean the same things, so they are translated
   * rather than kept: `end_turn` is a stop, `tool_use` is a turn waiting on tools, `max_tokens` is running
   * out of room. A reason this type does not name is kept as the API spelled it.
   *
   * @param reason what the API called it
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
