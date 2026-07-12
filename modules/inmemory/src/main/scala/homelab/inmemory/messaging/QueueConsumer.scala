package homelab.inmemory.messaging


import homelab.common.messaging.Consumer
import zio.*


/**
 * In-memory [[Consumer]] backed by a [[QueueSource]] — a raw queue (`Pure`), a mapped transform
 * (`Mapped`), or a fair merge of several (`Merged`); the same shape works for all. Never fails.
 *
 * @param source the take-side this consumer reads from
 */
final class QueueConsumer[A](val source: QueueSource[A]) extends Consumer[Nothing, A] {

  override def consume[E2 >: Nothing](logic: A => IO[E2, Unit]): IO[E2, Unit] =
    source.take.flatMap(logic)

  override def map[B](f: A => B): QueueConsumer[B] =
    new QueueConsumer(source.map(f))

  /**
   * A batched view over the same source, delivering up to `maxBatchSize` per `consume` call.
   *
   * @param maxBatchSize the maximum messages per batch
   * @return the batched consumer
   */
  def batched(maxBatchSize: Int): QueueConsumer.Batched[A] =
    QueueConsumer.Batched(source, maxBatchSize)

  /**
   * A consumer over the fair merge of this consumer's source and `that`'s.
   *
   * @param that the consumer to merge with
   * @return a consumer delivering from both, interleaved
   */
  def merge(that: QueueConsumer[A]): UIO[QueueConsumer[A]] =
    QueueSource.Merged.make(List(this.source, that.source)).map(QueueConsumer.fromSource)
}


object QueueConsumer {

  /**
   * A consumer over an arbitrary source (`Pure`, `Mapped`, or `Merged`).
   *
   * @param source the take-side to read from
   * @return the consumer
   */
  def fromSource[A](source: QueueSource[A]): QueueConsumer[A] = new QueueConsumer(source)

  /**
   * A consumer over an existing raw queue.
   *
   * @param queue the backing queue
   * @return the consumer
   */
  def fromQueue[A](queue: Queue[A]): QueueConsumer[A] = fromSource(QueueSource.Pure(queue))

  /**
   * A consumer over a fresh unbounded queue.
   *
   * @return the new consumer
   */
  def make[A]: UIO[QueueConsumer[A]] = Queue.unbounded[A].map(fromQueue)

  /**
   * A batched consumer over a fresh unbounded queue.
   *
   * @param maxBatchSize the maximum messages per batch
   * @return the new batched consumer
   */
  def makeBatched[A](maxBatchSize: Int): UIO[Batched[A]] =
    Queue.unbounded[A].map(queue => new Batched(QueueSource.Pure(queue), maxBatchSize))

  /**
   * In-memory batched consumer: each `consume` delivers up to `maxBatchSize` messages in a [[List]],
   * blocking until at least one is available.
   *
   * @param source       the take-side to read from
   * @param maxBatchSize the maximum messages per batch
   */
  final class Batched[A](val source: QueueSource[A], maxBatchSize: Int) extends Consumer.Batched[Nothing, A] {

    override def consume[E2 >: Nothing](logic: List[A] => IO[E2, Unit]): IO[E2, Unit] =
      source.takeUpTo(maxBatchSize).flatMap(logic)

    /**
     * A per-item view over the same source.
     *
     * @return a per-item consumer sharing this source
     */
    def perItem: QueueConsumer[A] = new QueueConsumer(source)
  }
}
