package homelab.llm.openai.request


import zio.json.*
import zio.json.ast.Json


/**
 * Whether the model may call a tool, must call one, or must call a named one.
 *
 * @see [[CompletionRequest.toolChoice]]
 */
enum ToolChoice {

  /** It decides — the default when nothing is said. */
  case Auto

  /** It may not call one, whatever it was offered. */
  case Never

  /** It must call one, and picks which. */
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
   * The wire is a union of a string and an object here — three of the four are bare words and the fourth
   * is a function object — which no derivation expresses, so this says it directly.
   */
  given JsonEncoder[ToolChoice] = JsonEncoder[Json].contramap {
    case Auto        => Json.Str("auto")
    case Never       => Json.Str("none")
    case Required    => Json.Str("required")
    case Named(name) => Json.Obj("type" -> Json.Str("function"), "function" -> Json.Obj("name" -> Json.Str(name)))
  }
