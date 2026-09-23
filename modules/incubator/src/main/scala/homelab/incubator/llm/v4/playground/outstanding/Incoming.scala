package homelab.incubator.llm.v4.playground.outstanding


/**
 * Something that has arrived for a conversation.
 *
 * Everything that moves a conversation on arrives as one of these, on a queue keyed by the conversation, so
 * that one consumer holds a conversation at a time. A question put straight to the agent would be outside
 * that, and could run beside a delivery on the same conversation — which is what the key is there to stop.
 *
 * The conversation itself is not in here: it is the key the message arrived under.
 */
enum Incoming {

  /**
   * A question from whoever is talking to the agent.
   *
   * @param question what they asked
   */
  case Asked(question: String)

  /**
   * Word that work started earlier has finished.
   *
   * Carries the id and nothing else, so two of these deliver the same findings rather than two versions of
   * them: what is delivered is read from the row, not from the message.
   *
   * @param investigation which piece of work finished
   */
  case Delivered(investigation: Investigation.Id)
}
