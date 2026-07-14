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
) extends Poll.WithQueue(queue) with Poll.WithInit(subscribeLock, started):

  /** Establish the shared-dispatcher subscription — the one-time [[Poll.WithInit.init]] effect for this poll. */
  override def init: IO[NatsError, Unit] =
    subscriber.subscribe(subject, queue, capturedScope)


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
      queue   <- Queue.sliding[Message](256) // bounded, drop-oldest under overload — Core is lossy by nature; @todo config the size
      started <- Ref.make(false)
      lock    <- Semaphore.make(1)
    yield new CorePoll(subject, queue, subscriber, started, lock, scope)
