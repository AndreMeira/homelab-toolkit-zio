package homelab.incubator.llm.v4


/**
 * What a dispatch produced, as the tool produced it.
 *
 * The result is unrendered and its type is gone, but it carries what writes it, so `render` works without
 * anyone naming the type again and a loop looking for a particular value can match it rather than read it
 * back out of text. What the model reads is `result.render`.
 *
 * @param callId the id the model gave this call, echoed back so it can pair request with result
 * @param result what the tool returned, and whether it succeeded
 */
final case class Outcome(callId: Tool.Call.Id, result: Tool.Result[?])
