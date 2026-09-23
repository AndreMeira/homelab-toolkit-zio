package homelab.incubator.llm.v4.playground.outstanding


import homelab.common.error.ApplicationError
import homelab.common.flow.KeyedQueue
import homelab.common.messaging.Consumer
import homelab.common.processing.Processor
import homelab.incubator.llm.v4.playground.chat.{ Chat, Conversation }
import zio.{ IO, UIO }


/**
 * What attends to a conversation: everything that moves one on is queued under it and taken one at a time,
 * and this is what takes them.
 *
 * Both kinds of arrival come through here because both take the conversation exclusively, and a queue can
 * only give it exclusively to what it delivers. A question put straight to the agent would be outside that
 * and could run beside a delivery on the same conversation, which is what the key is there to stop. That
 * exclusivity is also why the agent needs no lock of its own.
 *
 * A [[Processor]], so what runs it is the standard loop and what it plugs into is a graph. Its `process`
 * answers nothing: the agent's words went into the conversation as it worked, and whoever wants them reads
 * them there — nobody is waiting on this.
 *
 * The queue here is [[KeyedQueue]], which holds its work in memory: a process that dies loses what it had
 * claimed. A distributed keyed queue has the same shape and adds the lease that makes a lost claim come
 * back, which is what a real deployment wants and what the arrangement is written for.
 *
 * @param queue where arrivals wait, keyed by the conversation they are for
 * @param chat the agent a question is put to
 * @param delivery what puts finished work back
 */
final class Attendant(queue: KeyedQueue[Conversation, Incoming], chat: Chat, delivery: Delivery)
    extends Processor[ApplicationError, (Conversation, Incoming)] {

  /**
   * Arrivals as they are claimed, each paired with the conversation it is for.
   *
   * @return the intake, which never fails of its own accord
   */
  override def input: Consumer[ApplicationError, (Conversation, Incoming)] =
    Consumer.fromKeyedQueueWithKeys(queue)

  /**
   * Act on one arrival, which is the only place the two kinds part company.
   *
   * @param arrival the conversation it arrived under, and what arrived
   * @return noop once the agent has finished with it; aborts with whatever the agent or the delivery does
   */
  override def process(arrival: (Conversation, Incoming)): IO[ApplicationError, Unit] = arrival match
    case (conversation, Incoming.Asked(question))          => chat.run(Chat.Ask(conversation, question)).unit
    case (conversation, Incoming.Delivered(investigation)) => delivery.deliver(conversation, investigation).unit

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
}


object Attendant:

  /**
   * An inbox with nothing queued.
   *
   * @param backlog how many arrivals may wait before an offer suspends, or none for no bound
   * @param chat the agent a question is put to
   * @param delivery what puts finished work back
   * @return the inbox; aborts when the backlog bound is not a usable one
   */
  def make(backlog: Option[Int], chat: Chat, delivery: Delivery): IO[ApplicationError, Attendant] =
    KeyedQueue.make[Conversation, Incoming](backlog).map(queue => new Attendant(queue, chat, delivery))
