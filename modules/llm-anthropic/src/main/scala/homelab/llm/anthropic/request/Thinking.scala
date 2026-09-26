package homelab.llm.anthropic.request

import zio.json.*


/**
 * Whether the model reasons before it answers, and how much room it has to.
 *
 * What it produces while reasoning comes back as a block this adapter does not model, and has to be
 * handed back unaltered on the following turn — which [[homelab.llm.Message.Content.Raw]] is what carries.
 *
 * @see [[CompletionRequest.thinking]]
 */
@jsonDiscriminator("type")
enum Thinking derives JsonEncoder {

  /**
   * It reasons first, within a budget.
   *
   * @param budgetTokens the most it may spend reasoning, which comes out of what the answer may cost
   */
  @jsonHint("enabled") @jsonMemberNames(SnakeCase) case Enabled(budgetTokens: Int)

  /** It answers directly. */
  @jsonHint("disabled") case Disabled
}
