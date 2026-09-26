package homelab.llm.openai.response


import homelab.llm.openai.error.ChatCompletionError
import homelab.llm.{ Message, Model, Tool }
import zio.Chunk
import zio.json.*
import zio.json.ast.Json


/**
 * The chat-completions response, as OpenRouter sends it.
 *
 * Only what this adapter reads: the first choice, why it stopped, and what it cost. Fields the gateway
 * sends and nothing here uses — `id`, `created`, `provider` — are not named, because a decoder that
 * ignores them cannot be broken by one more arriving.
 *
 * @param choices what the model produced, of which the first is taken — see [[CompletionResponse.completion]]
 * @param usage what the call consumed, absent on some providers
 */
final case class CompletionResponse(
  choices: Chunk[CompletionResponse.Choice],
  usage: Option[CompletionResponse.Usage],
) derives JsonDecoder


object CompletionResponse:

  /**
   * One of the model's answers.
   *
   * @param message      what it said and asked for
   * @param finishReason why it stopped, absent while a stream is still open
   */
  @jsonMemberNames(SnakeCase)
  final case class Choice(
    message: CompletionResponse.Turn,
    finishReason: Option[String],
  ) derives JsonDecoder

  /**
   * An assistant turn.
   *
   * Named for what it is rather than for the field that carries it, since `Message` in this package is the
   * conversation's own and this is the wire's account of one turn of it.
   *
   * @param content   what it said, absent when it only asked for tools
   * @param toolCalls what it asked for, absent when it only answered
   */
  @jsonMemberNames(SnakeCase)
  final case class Turn(
    content: Option[String],
    toolCalls: Option[Chunk[CompletionResponse.Call]],
  ) derives JsonDecoder

  /**
   * One tool call, whose arguments are a JSON string the model wrote.
   *
   * @param id       what the model named it, echoed back with the answer
   * @param function which tool, and with what
   */
  final case class Call(
    id: String,
    function: CompletionResponse.Function,
  ) derives JsonDecoder

  /**
   * The called tool and its arguments.
   *
   * @param name      which tool the model asked for
   * @param arguments the JSON it wrote, unparsed
   */
  final case class Function(
    name: String,
    arguments: String,
  ) derives JsonDecoder

  /**
   * What the call consumed.
   *
   * The cost is OpenRouter's own field, in credits, and is the reason a gateway is worth fronting a
   * provider with: a direct call reports tokens and leaves the pricing to whoever holds the table.
   *
   * @param promptTokens     what was sent
   * @param completionTokens what came back
   * @param cost             what it was charged, where the gateway says
   */
  @jsonMemberNames(SnakeCase)
  final case class Usage(
    promptTokens: Int,
    completionTokens: Int,
    cost: Option[BigDecimal],
  ) derives JsonDecoder

  /**
   * One completion, read out of a decoded body.
   *
   * Every refusal is an [[ChatCompletionError.Malformed]]: the body parsed as JSON and still did not say what a
   * completion needs. A gateway that answers with no choices has not answered.
   *
   * Only the first choice is read, and the rest are dropped. There is more than one only when a caller
   * asked for several through [[request.CompletionRequest.n]], which is reached by holding the client —
   * what the port carries back is one completion, with nowhere to put an alternative. A caller that wants
   * several is asking for something [[Model]] does not model, and is served by the client instead.
   *
   * @param response what the gateway sent
   * @return the completion; refuses when the body carries no choice to read
   */
  def completion(response: CompletionResponse): Either[ChatCompletionError, Model.Completion] =
    response.choices.headOption match
      case Some(choice) => Right(completed(choice, response.usage))
      case None         => Left(ChatCompletionError.Malformed("the response carried no choices"))

  /**
   * One answer, as the toolkit holds it.
   *
   * The usage comes from the response rather than from the choice, because a body reports once what a call
   * consumed however many answers it carried.
   *
   * @param choice the answer to read, which is the first the gateway sent
   * @param usage what the call consumed, absent on some providers
   * @return the completion
   */
  private def completed(choice: Choice, usage: Option[CompletionResponse.Usage]): Model.Completion =
    Model.Completion(
      content = said(choice.message.content),
      calls = requested(choice.message.toolCalls),
      finish = stopped(choice.finishReason),
      usage = consumed(usage),
    )

  /**
   * What the model said, as content parts.
   *
   * @param content the text it sent, absent when it only asked for tools
   * @return one text part, or none
   */
  private def said(content: Option[String]): Chunk[Message.Content] =
    Chunk.fromIterable(content.filter(_.nonEmpty).map(Message.Content.Text.apply))

  /**
   * What the model asked to have run.
   *
   * @param calls what it sent, absent when it only answered
   * @return the calls, raw, since the arguments are a string the model wrote
   */
  private def requested(calls: Option[Chunk[CompletionResponse.Call]]): Chunk[Tool.Call.Raw] =
    calls.getOrElse(Chunk.empty).map { call =>
      Tool.Call.Raw(Tool.Call.Id(call.id), call.function.name, call.function.arguments)
    }

  /**
   * Why the model stopped.
   *
   * A reason this type does not name is kept rather than discarded, since the set is the gateway's and it
   * fronts many providers. An absent reason is one too: a body without it is one this adapter cannot
   * explain, and saying so is better than choosing a case.
   *
   * @param reason what the gateway called it
   * @return the reason, named where it is one this type knows
   */
  private def stopped(reason: Option[String]): Model.FinishReason = reason match
    case Some("stop")           => Model.FinishReason.Stop
    case Some("tool_calls")     => Model.FinishReason.ToolCalls
    case Some("length")         => Model.FinishReason.Length
    case Some("content_filter") => Model.FinishReason.ContentFilter
    case Some(other)            => Model.FinishReason.Other(other)
    case None                   => Model.FinishReason.Other("none given")

  /**
   * What the call consumed.
   *
   * A gateway that reports nothing gives zero tokens and no cost — which a budget reads differently from a
   * charge of nothing, which is why the cost is optional in the first place.
   *
   * @param usage what the gateway sent, absent on some providers
   * @return the usage
   */
  private def consumed(usage: Option[CompletionResponse.Usage]): Model.Usage = usage match
    case Some(reported) => Model.Usage(reported.promptTokens, reported.completionTokens, reported.cost)
    case None           => Model.Usage(0, 0, None)
