package homelab.nats.core


import homelab.nats.{ NatsError, Poll }
import io.nats.client.Message
import zio.*


/**
 * A Core NATS [[Poll]]: drains a bridge queue that a [[CoreSubscriber]] fills from the shared dispatcher,
 * subscribing lazily on the first `one`/`many` (so an unconsumed poll never buffers). Lazy because Core is
 * fire-and-forget with no server-side backpressure — deferring the SUB until first demand avoids piling
 * undrained messages into an unbounded queue.
 *
 * @param subject       the subject to subscribe to on first demand
 * @param queue         the bridge queue the subscriber offers delivered messages into
 * @param subscriber    the shared dispatcher-backed subscriber to subscribe through
 * @param started       tracks whether the lazy subscription has been established
 * @param subscribeLock serialises the cold subscription so concurrent first-callers don't race
 * @param capturedScope the consumer's scope the delivery fiber is forked into
 */
final class CorePoll(
  subject: String,
  queue: Queue[Message],
  subscriber: CoreSubscriber,
  started: Ref[Boolean],
  subscribeLock: Semaphore,
  capturedScope: Scope,
) extends Poll:

  /**
   * Take the next delivered message, subscribing on first demand.
   *
   * @return the next message; aborts with [[NatsError.Connect]] if the lazy subscription can't be set up
   */
  override def one: IO[NatsError, Message] =
    start *> queue.take

  /**
   * Take up to `maxMessages` currently-buffered messages (at least one), subscribing on first demand.
   *
   * @param maxMessages the batch ceiling
   * @return the drained messages; aborts with [[NatsError.Connect]] if the lazy subscription can't be set up
   */
  override def many(maxMessages: Int): IO[NatsError, List[Message]] =
    start *> queue.takeBetween(1, maxMessages).map(_.toList)

  /**
   * Establish the subscription exactly once, on first demand. Double-checked: the hot path is a lock-free
   * `started.get`; only the cold first call takes `subscribeLock`, and `started` flips to `true` only after
   * `subscribe` succeeds — so a failed subscribe leaves the gate open for the next caller to retry, and
   * concurrent first-callers serialise on the permit instead of racing.
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


object CorePoll:

  /**
   * Build a core poll over `subscriber`, capturing the current scope so the lazy subscription (established on
   * the first `one`/`many`) forks into the consumer's lifetime rather than the caller's. Subscription is
   * deferred, so this step itself cannot fail.
   *
   * @param subscriber the shared dispatcher-backed subscriber to subscribe through on first demand
   * @param subject    the subject to subscribe to (may be a wildcard, e.g. `orders.*`)
   * @return the poll, not yet subscribed
   */
  def make(subscriber: CoreSubscriber, subject: String): ZIO[Scope, Nothing, CorePoll] =
    for
      scope   <- ZIO.scope
      queue   <- Queue.unbounded[Message]
      started <- Ref.make(false)
      lock    <- Semaphore.make(1)
    yield new CorePoll(subject, queue, subscriber, started, lock, scope)
