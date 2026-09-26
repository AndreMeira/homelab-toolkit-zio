package homelab.llm.anthropic.request

import zio.json.*


/**
 * Whether the model may call a tool, must call one, or must call a named one.
 *
 * Every case is an object carrying its own `type`, which is how this API says all four — unlike the
 * chat-completions protocol, where three of them are bare words.
 *
 * @see [[CompletionRequest.toolChoice]]
 */
@jsonDiscriminator("type")
enum ToolChoice derives JsonEncoder {

  /** It decides — the default when nothing is said. */
  @jsonHint("auto") case Auto

  /** It must call one, and picks which. */
  @jsonHint("any") case Any

  /** It may not call one, whatever it was offered. */
  @jsonHint("none") case Never

  /**
   * It must call this one.
   *
   * @param name the tool it has to use
   */
  @jsonHint("tool") case Named(name: String)
}
