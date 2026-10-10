package homelab.llm.mistral.request


import zio.json.*
import zio.json.ast.Json


/**
 * Whether the model may call a tool, must call one, or must call a named one.
 *
 * @see [[CompletionRequest]]
 */
enum ToolChoice {

  /** It decides — the default when nothing is said. */
  case Auto

  /** It may not call one, whatever it was offered. */
  case Never

  /** It must call one, and picks which. */
  case Any

  /** It must call one, and picks which — what `Any` asks, under the other word the API takes. */
  case Required

  /**
   * It must call this one.
   *
   * @param name the tool it has to use
   */
  case Named(name: String)
}


object ToolChoice:

  /**
   * How a choice is written.
   *
   * The wire is a union of a string and an object here — four of the five are bare words and the fifth is
   * a function object — which no derivation expresses, so this says it directly.
   */
  given JsonEncoder[ToolChoice] = JsonEncoder[Json].contramap(encode)

  /**
   * One choice, as JSON.
   *
   * @param choice the choice
   * @return the word, or the object naming the function
   */
  private def encode(choice: ToolChoice): Json = choice match
    case Auto        => Json.Str("auto")
    case Never       => Json.Str("none")
    case Any         => Json.Str("any")
    case Required    => Json.Str("required")
    case Named(name) => Json.Obj("type" -> Json.Str("function"), "function" -> Json.Obj("name" -> Json.Str(name)))
