package homelab.nats.stream


import homelab.nats.{ NatsError, Poll }
import io.nats.client.Message
import zio.*


/**
 * A JetStream [[Poll]]: drains a bridge queue that a [[JetStreamSubscriber]] fills from the durable
 * consumer's async `consume`, subscribing lazily on the first `one`/`many`. Lazy keeps an unconsumed poll
 * from attaching delivery early; durability means nothing is lost by subscribing late (the stream holds
 * messages until the consumer attaches).
 *
 * @param config        the stream / durable / subject identity and ack tuning
 * @param queue         the bridge queue the subscriber offers delivered messages into
 * @param subscriber    the subscriber to attach the durable consumer through
 * @param started       tracks whether the lazy subscription has been established
 * @param subscribeLock serialises the cold subscription so concurrent first-callers don't race
 * @param capturedScope the consumer's scope the delivery fiber is forked into
 */
final class StreamPoll(
  config: ContextConfig,
  queue: Queue[Message],
  subscriber: JetStreamSubscriber,
  started: Ref[Boolean],
  subscribeLock: Semaphore,
  capturedScope: Scope,
) extends Poll.WithQueue(queue) with Poll.WithInit(subscribeLock, started):

  /** Attach the durable consumer's delivery — the one-time [[Poll.WithInit.init]] effect for this poll. */
  override def init: IO[NatsError, Unit] =
    subscriber.subscribe(config, queue, capturedScope)


object StreamPoll:

  /**
   * Build a JetStream poll over `subscriber`, capturing the current scope so the lazy subscription
   * (established on the first `one`/`many`) forks into the consumer's lifetime rather than the caller's.
   * Subscription is deferred, so this step itself cannot fail.
   *
   * @param subscriber the subscriber to attach the durable consumer through on first demand
   * @param config     the stream / durable / subject identity and ack tuning
   * @return the poll, not yet subscribed
   */
  def make(subscriber: JetStreamSubscriber, config: ContextConfig): ZIO[Scope, Nothing, StreamPoll] =
    for
      scope   <- ZIO.scope
      queue   <- Queue.unbounded[Message]
      started <- Ref.make(false)
      lock    <- Semaphore.make(1)
    yield new StreamPoll(config, queue, subscriber, started, lock, scope)
