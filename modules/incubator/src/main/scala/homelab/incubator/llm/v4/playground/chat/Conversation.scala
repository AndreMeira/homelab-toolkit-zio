package homelab.incubator.llm.v4.playground.chat

/**
 * Which conversation a message belongs to.
 *
 * A subtype of `String`, so it is whatever a caller already names its conversations by — a chat id, a
 * ticket, a run. Named because it is the one string a repository looks a conversation up by.
 */
type Conversation = Conversation.Type


object Conversation:

  opaque type Type <: String = String

  /**
   * A conversation's id, as its caller spells it.
   *
   * @param value the text
   * @return the id
   */
  def apply(value: String): Type = value
