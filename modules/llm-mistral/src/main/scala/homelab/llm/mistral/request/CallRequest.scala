package homelab.llm.mistral.request


import homelab.llm.Tool
import zio.json.*


/**
 * One tool call, as an assistant message carries it back.
 *
 * Many models refuse an id that is not nine letters and digits, and every id the API mints is one. An id
 * another provider minted is refused unless it is rewritten into that form, the same way in the call and
 * in the tool message that answers it.
 *
 * @param id what the call was named, in the form the API takes, echoed back with the answer
 * @param function which tool, and with what
 * @param kind what kind of call it is, which the wire spells `type` and which is always a function
 * @param index where the call sat among the turn's calls
 */
final case class CallRequest(
  id: CallId,
  function: CallRequest.Function,
  @jsonField("type") kind: String = "function",
  index: Option[Int] = None,
) derives JsonEncoder


object CallRequest:

  /**
   * The called tool and its arguments.
   *
   * @param name which tool the model asked for
   * @param arguments the JSON it wrote, as a string, which is how the API sends it
   */
  final case class Function(name: String, arguments: String) derives JsonEncoder

  /**
   * One call the model made, as the API restates it.
   *
   * @param call what the model asked for
   * @return the call, under an id the API takes and with the arguments the model wrote
   */
  def from(call: Tool.Call.Raw): CallRequest = CallRequest(CallId.from(call.id), Function(call.name, call.arguments))
