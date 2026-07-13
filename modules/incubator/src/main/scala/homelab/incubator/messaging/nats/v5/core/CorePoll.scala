package homelab.incubator.messaging.nats.v5.core


import homelab.incubator.messaging.nats.v5.{ NatsError, Poll, Serde }
import io.nats.client.Message
import zio.*


class CorePoll(
  subject: String,
  queue: Queue[Message],
  subscriber: CoreSubscriber,
  started: Ref[Boolean],
  subscribeLock: Semaphore,
  capturedScope: Scope,
) extends Poll {
  override def one: IO[NatsError, Message] =
    start *> queue.take

  override def many(maxMessages: Int): IO[NatsError, List[Message]] =
    start *> queue.takeBetween(1, maxMessages).map(_.toList)

  /**
   * Subscribe exactly once, on first demand. Double-checked: the hot path is a lock-free `started.get`;
   * only the cold first call takes `subscribeLock`, and `started` flips to `true` only after `subscribe`
   * succeeds — so a failed subscribe leaves the gate open for the next caller to retry, and concurrent
   * first-callers serialize on the permit instead of racing.
   *
   * @return unit once the subscription is (or already was) live; aborts with [[NatsError.Connect]] if
   *         subscribing fails
   */
  private def start: IO[NatsError, Unit] =
    started.get.flatMap:
      case true  => ZIO.unit
      case false =>
        subscribeLock.withPermit:
          started.get.flatMap:
            case true  => ZIO.unit
            case false => subscriber.subscribe(subject, queue, capturedScope) *> started.set(true)
}
