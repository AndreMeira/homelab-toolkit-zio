package homelab.incubator.llm.v4.playground.chat

import homelab.common.error.ApplicationError
import homelab.incubator.llm.v4.Message
import zio.{ Chunk, IO }


/**
 * Where a conversation's messages are kept between runs.
 *
 * Unimplemented. A real one is a table keyed by conversation with an ordering column, or a list per key in
 * a key-value store; nothing here says which, and nothing above here cares.
 *
 * Messages are added a turn at a time rather than one at a time. A turn is an assistant message and one
 * answer per call it made, and a reader that meets the first without the rest sees a conversation waiting
 * on tools that have already run.
 */
trait MessageRepository {

  /**
   * Every message of a conversation, oldest first.
   *
   * @param conversation which conversation to read
   * @return its messages, empty when it has none yet; aborts when the store does
   */
  def get(conversation: Conversation): IO[ApplicationError.AdapterError, Chunk[Message]]

  /**
   * Add a turn to the end of a conversation.
   *
   * @param conversation which conversation to add to
   * @param messages the turn, in the order it happened
   * @return noop once they are stored; aborts when the store does
   */
  def add(conversation: Conversation, messages: Chunk[Message]): IO[ApplicationError.AdapterError, Unit]
}
