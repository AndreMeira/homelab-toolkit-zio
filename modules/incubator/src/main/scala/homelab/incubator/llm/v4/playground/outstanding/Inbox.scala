package homelab.incubator.llm.v4.playground.outstanding


import homelab.common.error.ApplicationError
import homelab.common.flow.KeyedQueue
import homelab.incubator.llm.v4.Message
import homelab.incubator.llm.v4.playground.chat.{ Chat, Conversation }
import zio.{ Chunk, IO, UIO, ZIO }


/**
 * The one way into a conversation: everything that moves one on is queued under it and taken one at a time.
 *
 * Both kinds of arrival come through here because both take the conversation exclusively, and a queue can
 * only give it exclusively to what it delivers. A question put straight to the agent would be outside that
 * and could run beside a delivery on the same conversation, which is what the key is there to stop. That
 * exclusivity is also why the agent needs no lock of its own.
 *
 * The queue here is [[KeyedQueue]], which holds its work in memory: a process that dies loses what it had
 * claimed. A distributed keyed queue has the same shape and adds the lease that makes a lost claim come
 * back, which is what a real deployment wants and what the arrangement is written for.
 *
 * @param queue where arrivals wait, keyed by the conversation they are for
 * @param chat the agent a question is put to
 * @param delivery what puts finished work back
 */
final class Inbox(queue: KeyedQueue[Conversation, Incoming], chat: Chat, delivery: Delivery) {

  /**
   * Queue a question for a conversation.
   *
   * @param conversation which conversation it is for
   * @param question what is being asked
   * @return noop once it is queued
   */
  def ask(conversation: Conversation, question: String): UIO[Unit] =
    queue.offer(conversation, Incoming.Asked(question))

  /**
   * Queue word that work started earlier has finished.
   *
   * What the [[Investigator]] calls once it has recorded its answer. The key is the conversation the work
   * belongs to, so a delivery waits for whatever else is holding that conversation.
   *
   * @param conversation which conversation the work belongs to
   * @param investigation which piece of work finished
   * @return noop once it is queued
   */
  def delivered(conversation: Conversation, investigation: Investigation.Id): UIO[Unit] =
    queue.offer(conversation, Incoming.Delivered(investigation))

  /**
   * Wait for one arrival and act on it, holding its conversation until it is done.
   *
   * @return what the agent said once it had read it; aborts with whatever the agent or the delivery does
   */
  def attend: IO[ApplicationError, Chunk[Message.Content]] = queue.takeWith(receive)

  /**
   * Attend to arrivals for as long as this runs.
   *
   * @return never returns normally; aborts with the first failure an arrival produces
   */
  def attending: IO[ApplicationError, Nothing] = attend.forever

  /**
   * Act on one arrival, which is the only place the two kinds part company.
   *
   * @param conversation the key it arrived under
   * @param incoming what arrived
   * @return what the agent said once it had read it; aborts with whatever the agent or the delivery does
   */
  private def receive(
    conversation: Conversation,
    incoming: Incoming,
  ): IO[ApplicationError, Chunk[Message.Content]] =
    incoming match
      case Incoming.Asked(question)          => chat.run(Chat.Ask(conversation, question))
      case Incoming.Delivered(investigation) => delivery.deliver(conversation, investigation)
}


object Inbox:

  /**
   * An inbox with nothing queued.
   *
   * @param backlog how many arrivals may wait before an offer suspends, or none for no bound
   * @param chat the agent a question is put to
   * @param delivery what puts finished work back
   * @return the inbox; aborts when the backlog bound is not a usable one
   */
  def make(backlog: Option[Int], chat: Chat, delivery: Delivery): IO[ApplicationError, Inbox] =
    KeyedQueue.make[Conversation, Incoming](backlog).map(queue => new Inbox(queue, chat, delivery))
