package homelab.inmemory.messaging


import homelab.common.messaging.{ Consumer, Producer }
import zio.*


/**
 * An in-process channel: a [[QueueProducer]] and [[QueueConsumer]] over one queue — both ends of the
 * same in-memory transport. It is simultaneously a [[Producer]] and a [[Consumer]], the natural
 * in-memory stand-in for a broker topic. Never fails.
 *
 * @param producer the emit side
 * @param consumer the take side
 */
final case class Wire[A](producer: QueueProducer[A], consumer: QueueConsumer[A]) extends Producer[Nothing, A], Consumer[Nothing, A] {

  override def emit(value: A): IO[Nothing, Unit] = producer.emit(value)

  override def consume[E2 >: Nothing](logic: A => IO[E2, Unit]): IO[E2, Unit] = consumer.consume(logic)
}


object Wire {

  /**
   * A wire over a fresh unbounded queue.
   *
   * @return the new wire
   */
  def make[A]: UIO[Wire[A]] = Queue.unbounded[A].map(fromQueue)

  /**
   * A wire from an existing producer/consumer pair.
   *
   * @param producer the emit side
   * @param consumer the take side
   * @return the wire connecting them
   */
  def connect[A](producer: QueueProducer[A], consumer: QueueConsumer[A]): Wire[A] =
    Wire(producer, consumer)

  /**
   * A wire whose producer and consumer share one existing queue.
   *
   * @param queue the backing queue
   * @return the wire over `queue`
   */
  def fromQueue[A](queue: Queue[A]): Wire[A] =
    connect(QueueProducer.fromQueue(queue), QueueConsumer.fromQueue(queue))
}
