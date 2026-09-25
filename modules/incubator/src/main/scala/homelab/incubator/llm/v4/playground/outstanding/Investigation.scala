package homelab.incubator.llm.v4.playground.outstanding

import homelab.incubator.llm.v4.playground.chat.Conversation


/**
 * Work started by a tool call and finished long after it answered.
 *
 * The row the tool keeps for itself. It names the conversation to deliver back to, which the model never
 * wrote and cannot change: a call carries it because the session was bound to it, not because the model
 * asked for it.
 *
 * @param id what this piece of work is called, and what a delivery names
 * @param conversation where its answer belongs when it arrives
 * @param topic what was asked, in the model's words
 * @param answer what came back, absent while it is still running
 */
final case class Investigation(
  id: Investigation.Id,
  conversation: Conversation,
  topic: String,
  answer: Option[String],
)


object Investigation:

  /**
   * What an investigation is named by.
   *
   * A subtype of `String`, so it reaches the model as text it can quote back and a delivery as the key it
   * looks up. Minted when the work starts, since nothing before then has anything to name.
   */
  opaque type Id <: String = String

  object Id:

    /**
     * A name for one piece of work.
     *
     * @param value the text
     * @return the id
     */
    def apply(value: String): Id = value
