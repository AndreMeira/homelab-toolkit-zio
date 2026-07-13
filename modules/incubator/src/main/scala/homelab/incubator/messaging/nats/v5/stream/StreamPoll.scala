package homelab.incubator.messaging.nats.v5.stream


import homelab.incubator.messaging.nats.v5.{ NatsError, Poll }
import homelab.incubator.messaging.nats.v5.config.ContextConfig
import io.nats.client.Message
import zio.*


class StreamPoll(
  config: ContextConfig,
  queue: Queue[Message],
  subscriber: JetStreamSubscriber,
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
            case false => subscriber.subscribe(config, queue, capturedScope) *> started.set(true)
}


object StreamPoll:
  /**
   * Run `effect`, pinging `inProgress()` on `messages` every `interval` for as long as it runs.
   *
   * @param interval how often to ping, or `None` to disable (run `effect` unwrapped)
   * @param messages the in-flight messages to keep alive
   * @param effect   the handler to run
   * @tparam E2 the effect's error
   * @tparam A  the effect's result
   * @return the effect's result; ping failures are ignored (redelivery covers a missed keepalive)
   */
  def heartbeat[E2, A](interval: Option[Duration], messages: List[Message])(effect: IO[E2, A]): IO[E2, A] =
    interval.fold(effect): delay =>
      ZIO.scoped {
        ZIO
          .foreachDiscard(messages): message =>
            ZIO.attemptBlocking(message.inProgress()).ignore
          .delay(delay)
          .forever
          .forkScoped *> effect
      }
