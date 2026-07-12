package homelab.inmemory.messaging


import homelab.common.messaging.Producer
import zio.*


/**
 * In-memory [[Producer]] that emits by offering to an unbounded [[Queue]]. Never fails.
 *
 * @param queue the backing queue
 */
final class QueueProducer[A](val queue: Queue[A]) extends Producer[Nothing, A] {

  override def emit(value: A): IO[Nothing, Unit] = queue.offer(value).unit
}


object QueueProducer {

  /**
   * A producer over an existing queue.
   *
   * @param queue the backing queue
   * @return a producer offering to `queue`
   */
  def fromQueue[A](queue: Queue[A]): QueueProducer[A] = new QueueProducer(queue)

  /**
   * A producer over a fresh unbounded queue.
   *
   * @return the new producer
   */
  def make[A]: UIO[QueueProducer[A]] = Queue.unbounded[A].map(fromQueue)
}
