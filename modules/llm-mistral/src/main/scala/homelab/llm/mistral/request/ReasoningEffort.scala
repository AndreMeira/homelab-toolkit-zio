package homelab.llm.mistral.request

import zio.json.*


/**
 * How much a model reasons before it answers.
 *
 * Each model takes only some of these, and the API refuses the rest by name — and refuses the field
 * altogether on a model that does not reason. When a model reasons, its answer's content comes back as a
 * thinking chunk followed by text.
 *
 * @see [[CompletionRequest]]
 */
enum ReasoningEffort {

  /** It answers without reasoning first. */
  case Disabled

  /** The least reasoning a model that reasons does. */
  case Minimal

  /** A little reasoning. */
  case Low

  /** A moderate amount. */
  case Medium

  /** A lot, which Mistral recommends for agentic and coding work. */
  case High

  /** The most reasoning the API names. */
  case ExtraHigh
}


object ReasoningEffort:

  /** An effort is written as the word the API uses. */
  given JsonEncoder[ReasoningEffort] = JsonEncoder[String].contramap(encode)

  /**
   * The word the API uses for an effort.
   *
   * @param effort the effort
   * @return its word
   */
  private def encode(effort: ReasoningEffort): String = effort match
    case Disabled  => "none"
    case Minimal   => "minimal"
    case Low       => "low"
    case Medium    => "medium"
    case High      => "high"
    case ExtraHigh => "xhigh"
