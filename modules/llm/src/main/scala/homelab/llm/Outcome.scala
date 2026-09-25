package homelab.llm


/**
 * What a dispatch produced: the call it answers, and what came of it.
 *
 * The call says whether its arguments could be read — [[Tool.Call.Decoded]] when they could, which is also
 * the only way to learn what the model asked for, and [[Tool.Call.Raw]] when they could not, which is what
 * a call naming no tool at all comes back as.
 *
 * The result is unrendered and its type is gone, but it carries what writes it, so `render` works without
 * anyone naming the type again. What the model reads is `result.render`.
 *
 * @param call the call this answers, read when its arguments could be and raw when they could not
 * @param result what the tool returned, and whether it succeeded
 */
final case class Outcome(call: Tool.Call[?], result: Tool.Result[?])
