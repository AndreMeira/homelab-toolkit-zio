package homelab.llm.mistral.request

import zio.json.*

/**
 * Text the answer is expected to contain, which the API uses to answer faster.
 *
 * Suited to an edit of a document or of code where little changes.
 *
 * @param content what the answer is expected to contain
 * @param kind what kind of prediction it is, which the wire spells `type` and which is always content
 * @see [[CompletionRequest]]
 */
final case class Prediction(content: String, @jsonField("type") kind: String = "content") derives JsonEncoder
