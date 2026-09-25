package homelab.incubator.llm.v4.playground.outstanding


import homelab.common.error.ApplicationError
import homelab.incubator.llm.v4.Message
import homelab.incubator.llm.v4.playground.chat.{ Chat, Conversation }
import zio.{ Chunk, IO, ZIO }


/**
 * What a nudge wakes: the answer to a finished investigation, put back into the conversation that asked.
 *
 * The delivery is an ordinary question to the agent, because that is what it is — something new to read
 * and act on. The conversation holds no record that anything was owed, so nothing has to be reconciled
 * here: the findings are appended, the agent runs, and it reads the whole conversation as it always does.
 *
 * A nudge carries only an id, so one that arrives twice delivers the same findings twice. What stops that
 * being a problem is not this: it is the queue giving one conversation to one consumer at a time, and a
 * second delivery reading the same row. Reaching this any way other than through [[Attendant]] is outside that
 * guarantee.
 *
 * @param store where the work was recorded
 * @param chat the agent whose conversation is being continued
 */
final class Delivery(store: InvestigationStore, chat: Chat) {

  /**
   * Put a finished investigation back into its conversation and let the agent carry on.
   *
   * The conversation is taken twice — from the key the nudge arrived under and from the row — and the two
   * are required to agree. The key is what gave this run the conversation exclusively, so a row naming a
   * different one would be written into a conversation nothing is holding.
   *
   * @param conversation the key the nudge arrived under
   * @param id which investigation finished, as the nudge names it
   * @return what the agent said once it had read the findings; aborts with [[Delivery.Unknown]] when no
   *         such work was started, [[Delivery.Misrouted]] when the nudge arrived under the wrong key,
   *         [[Delivery.Unfinished]] when it has no answer yet, and with whatever the agent aborts with
   */
  def deliver(conversation: Conversation, id: Investigation.Id): IO[ApplicationError, Message.Assistant] =
    for
      found         <- store.get(id)
      investigation <- ZIO.fromOption(found).orElseFail(Delivery.Unknown(id))
      _             <- if investigation.conversation == conversation then ZIO.unit
                       else ZIO.fail(Delivery.Misrouted(conversation, id))
      answer        <- ZIO.fromOption(investigation.answer).orElseFail(Delivery.Unfinished(id))
      said          <- chat.run(Chat.Ask(conversation, findings(investigation, answer)))
    yield said

  /**
   * The findings as the conversation reads them.
   *
   * Named with the investigation the model was given a receipt for, so a model that started several can
   * tell which one has come back.
   *
   * @param investigation what was asked, and of what
   * @param answer what it found
   * @return the text to put to the agent
   */
  private def findings(investigation: Investigation, answer: String): String =
    s"Findings for investigation ${investigation.id}, which asked about ${investigation.topic}: $answer"
}


object Delivery:

  /**
   * A nudge naming work nothing started.
   *
   * @param id what the nudge named
   */
  final case class Unknown(id: Investigation.Id) extends ApplicationError.NotFoundError:

    /**
     * What went wrong, for whoever is reading the queue.
     *
     * @return the reason, naming the investigation
     */
    override def message: String = s"no investigation '$id' was ever started"

  /**
   * A nudge whose key is not the conversation the work belongs to.
   *
   * An [[ApplicationError.InconsistentState]]: the work records the conversation when it starts and the
   * nudge is keyed by the same one, so the two disagreeing means one of them was written wrong.
   *
   * @param conversation the key it arrived under
   * @param id which investigation it named
   */
  final case class Misrouted(conversation: Conversation, id: Investigation.Id)
      extends ApplicationError.InconsistentState:

    /**
     * What went wrong, for whoever is reading the queue.
     *
     * @return the reason, naming both
     */
    override def message: String = s"investigation '$id' does not belong to conversation '$conversation'"

  /**
   * A nudge that arrived before the answer it announces.
   *
   * An [[ApplicationError.TransientError]]: the work records its answer before it signals, so a nudge that
   * gets here first is ahead of a write that is on its way, and the same nudge delivered again lands after
   * it.
   *
   * @param id which investigation has no answer yet
   */
  final case class Unfinished(id: Investigation.Id) extends ApplicationError.TransientError:

    /**
     * What went wrong, for whoever is reading the queue.
     *
     * @return the reason, naming the investigation
     */
    override def message: String = s"investigation '$id' has not answered yet"
