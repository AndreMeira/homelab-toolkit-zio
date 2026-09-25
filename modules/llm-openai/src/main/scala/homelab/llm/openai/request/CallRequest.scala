package homelab.llm.openai.request


import homelab.llm.Tool
import zio.json.*


/**
 * One tool call, as an assistant turn carries it back.
 *
 * The model's turn goes back as it came, so a call is restated rather than reinterpreted — the arguments
 * are the JSON string the model wrote, unparsed, whatever this adapter made of them since.
 *
 * @param id what the model named it, echoed back with the answer
 * @param kind what kind of call it is, which the wire spells `type` and which is always a function
 * @param function which tool, and with what
 */
final case class CallRequest(
  id: String,
  @jsonField("type") kind: String,
  function: CallRequest.Function,
) derives JsonEncoder


object CallRequest:

  /**
   * The called tool and its arguments.
   *
   * @param name which tool the model asked for
   * @param arguments the JSON it wrote, unparsed
   */
  final case class Function(name: String, arguments: String) derives JsonEncoder

  /**
   * One call, as the wire carries it.
   *
   * @param call what the model asked for
   * @return what to send for it
   */
  def from(call: Tool.Call.Raw): CallRequest = CallRequest(call.id, "function", Function(call.name, call.arguments))
