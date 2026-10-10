package homelab.llm.mistral.response


import homelab.llm.mistral.error.MistralError
import homelab.llm.mistral.request.ContentChunk
import homelab.llm.{ Message, Model, Tool }
import zio.Chunk
import zio.json.*
import zio.json.ast.Json


/**
 * A chat-completions response, as Mistral sends it.
 *
 * Every choice, why each stopped, and what the call consumed. A message's content arrives as a string, or as
 * chunks when a model reasoned or cited, and is read as chunks either way: a string is one text chunk.
 *
 * @param id what Mistral calls this completion
 * @param model which model answered
 * @param created when, in seconds since the epoch
 * @param choices what the model produced, one per answer asked for
 * @param usage what the call consumed
 */
final case class CompletionResponse(
  id: String,
  model: String,
  created: Long,
  choices: Chunk[CompletionResponse.Choice],
  usage: Option[CompletionResponse.Usage],
) derives JsonDecoder


object CompletionResponse:

  /**
   * One of the model's answers.
   *
   * @param index where it sits among the answers
   * @param message what it said and asked for
   * @param finishReason why it stopped — `stop`, `length`, `model_length`, `error` or `tool_calls` — as the
   *                     API spelled it
   */
  @jsonMemberNames(SnakeCase)
  final case class Choice(index: Int, message: AssistantMessage, finishReason: Option[String]) derives JsonDecoder

  /**
   * What the model said and asked for.
   *
   * @param content what it said, reasoning included, as chunks; empty when it only asked for tools
   * @param toolCalls what it asked for, absent when it only answered
   */
  final case class AssistantMessage(content: Chunk[ContentChunk], toolCalls: Option[Chunk[Call]])

  object AssistantMessage:

    /**
     * A message as the API sends it, before its content is read as chunks.
     *
     * @param content a string, null, or an array of chunks
     * @param toolCalls what the model asked for
     */
    @jsonMemberNames(SnakeCase)
    final private case class Received(content: Option[Json] = None, toolCalls: Option[Chunk[Call]] = None) derives JsonDecoder

    /** A message is read in two steps: the fields, then its content as chunks. */
    given JsonDecoder[AssistantMessage] = JsonDecoder[Received].mapOrFail(read)

    /**
     * One message, out of what arrived.
     *
     * @param received the fields as the API sent them
     * @return the message; refuses content that is neither text nor chunks
     */
    private def read(received: Received): Either[String, AssistantMessage] =
      chunks(received.content).map(AssistantMessage(_, received.toolCalls))

    /**
     * What the message said, as chunks.
     *
     * An empty string is no chunk at all, which is what the API sends beside a message that only asked for
     * tools.
     *
     * @param content what arrived in the field
     * @return the chunks; refuses a value that is neither a string, null, nor an array
     */
    private def chunks(content: Option[Json]): Either[String, Chunk[ContentChunk]] = content match
      case None | Some(Json.Null) => Right(Chunk.empty)
      case Some(Json.Str(""))     => Right(Chunk.empty)
      case Some(Json.Str(text))   => Right(Chunk(ContentChunk.text(text)))
      case Some(list: Json.Arr)   => list.as[Chunk[ContentChunk]]
      case Some(other)            => Left(s"content is neither text nor chunks: ${other.toJson.take(80)}")

  /**
   * One tool call.
   *
   * @param id what the model named it, echoed back with the answer
   * @param function which tool, and with what
   * @param index where it sits among the message's calls
   */
  final case class Call(id: String, function: Function, index: Option[Int] = None) derives JsonDecoder

  /**
   * The called tool and its arguments.
   *
   * The API sends the arguments as a string and its schema allows an object; either is read as the JSON
   * text the model wrote.
   *
   * @param name which tool the model asked for
   * @param arguments the JSON it wrote, unparsed
   */
  final case class Function(name: String, arguments: String)

  object Function:

    /**
     * A function as the API sends it, before its arguments are read as text.
     *
     * @param name which tool
     * @param arguments a JSON string, or an object
     */
    final private case class Received(name: String, arguments: Json) derives JsonDecoder

    /** A function is read in two steps: the fields, then its arguments as text. */
    given JsonDecoder[Function] = JsonDecoder[Received].map(read)

    /**
     * One function, out of what arrived.
     *
     * @param received the fields as the API sent them
     * @return the function, its arguments as text
     */
    private def read(received: Received): Function = Function(received.name, text(received.arguments))

    /**
     * Arguments as the text of a JSON object.
     *
     * @param arguments a string holding the JSON, or the JSON itself
     * @return the text: the string as it came, or the object written out
     */
    private def text(arguments: Json): String = arguments match
      case Json.Str(written) => written
      case other             => other.toJson

  /**
   * What the call consumed.
   *
   * There is no cost: Mistral reports tokens and leaves the pricing to whoever holds the table.
   *
   * @param promptTokens what was sent
   * @param completionTokens what came back
   * @param totalTokens the two together
   * @param promptAudioSeconds how much audio was sent, where any was
   * @param promptTokensDetails how the prompt was served, where the API says
   * @param serviceTier which capacity served the call, where the API says
   */
  @jsonMemberNames(SnakeCase)
  final case class Usage(
    promptTokens: Int,
    completionTokens: Int,
    totalTokens: Int,
    promptAudioSeconds: Option[Int] = None,
    promptTokensDetails: Option[Usage.PromptTokensDetails] = None,
    serviceTier: Option[String] = None,
  ) derives JsonDecoder

  object Usage:

    /**
     * How a prompt was served.
     *
     * @param cachedTokens how much of it came from the prompt cache
     */
    @jsonMemberNames(SnakeCase)
    final case class PromptTokensDetails(cachedTokens: Option[Int] = None) derives JsonDecoder

  /**
   * One completion, read out of a decoded body.
   *
   * Only the first choice is read. There is more than one only when a caller asked for several through the
   * request's `n`, which is reached by holding the client: what the port carries back is one completion.
   *
   * @param response what the API sent
   * @return the completion; refuses a body that carries no choice to read
   */
  def completion(response: CompletionResponse): Either[MistralError, Model.Completion] =
    response.choices.headOption match
      case Some(choice) => Right(completed(choice, response.usage))
      case None         => Left(MistralError.Malformed("the response carried no choices"))

  /**
   * One answer, as the toolkit holds it.
   *
   * @param choice the answer to read, which is the first the API sent
   * @param usage what the call consumed, which a body reports once however many answers it carries
   * @return the completion
   */
  private def completed(choice: Choice, usage: Option[Usage]): Model.Completion =
    Model.Completion(
      content = choice.message.content.map(said),
      calls = choice.message.toolCalls.getOrElse(Chunk.empty).map(requested),
      finish = stopped(choice.finishReason),
      usage = consumed(usage),
    )

  /**
   * One chunk of what the model said, as the conversation carries it.
   *
   * Text is words. Every other chunk — reasoning, references — is kept as JSON, which is how it goes back
   * on the next turn: Mistral asks for a model's reasoning to be replayed as it came.
   *
   * @param chunk what the model produced
   * @return the content part
   */
  private def said(chunk: ContentChunk): Message.Content = chunk match
    case ContentChunk.Decoded(ContentChunk.Kind.Text(text)) => Message.Content.Text(text)
    case other                                              => Message.Content.Raw(ContentChunk.json(other))

  /**
   * One call the model made, raw, since its arguments are the text it wrote.
   *
   * @param call what the API sent
   * @return the call, under the id Mistral gave it
   */
  private def requested(call: Call): Tool.Call.Raw =
    Tool.Call.Raw(Tool.Call.Id(call.id), call.function.name, call.function.arguments)

  /**
   * Why the model stopped.
   *
   * `model_length` is read as running out of room: Mistral does not document it, and the name says the
   * answer stopped at the model's context length. `error` is kept as the API spelled it, so the answer still
   * reaches the caller with the reason beside it. Any other reason is kept the same way, and a body without
   * one is one this adapter cannot explain.
   *
   * @param reason what the API called it
   * @return the reason, named where it is one the toolkit knows
   */
  private def stopped(reason: Option[String]): Model.FinishReason = reason match
    case Some("stop")         => Model.FinishReason.Stop
    case Some("tool_calls")   => Model.FinishReason.ToolCalls
    case Some("length")       => Model.FinishReason.Length
    case Some("model_length") => Model.FinishReason.Length
    case Some(other)          => Model.FinishReason.Other(other)
    case None                 => Model.FinishReason.Other("none given")

  /**
   * What the call consumed.
   *
   * @param usage what the API sent
   * @return the usage, with no cost, which this API does not report
   */
  private def consumed(usage: Option[Usage]): Model.Usage = usage match
    case Some(reported) => Model.Usage(reported.promptTokens, reported.completionTokens, None)
    case None           => Model.Usage(0, 0, None)
