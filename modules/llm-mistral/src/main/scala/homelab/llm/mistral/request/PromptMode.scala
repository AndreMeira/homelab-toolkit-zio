package homelab.llm.mistral.request

import zio.json.*


/**
 * Whether Mistral puts a system prompt of its own in front of the conversation. Absent, it puts none.
 *
 * @see [[CompletionRequest]]
 */
enum PromptMode {

  /** The system prompt Mistral uses for its reasoning models. */
  case Reasoning
}


object PromptMode:

  /** A mode is written as the word the API uses. */
  given JsonEncoder[PromptMode] = JsonEncoder[String].contramap(encode)

  /**
   * The word the API uses for a mode.
   *
   * @param mode the mode
   * @return its word
   */
  private def encode(mode: PromptMode): String = mode match
    case Reasoning => "reasoning"
