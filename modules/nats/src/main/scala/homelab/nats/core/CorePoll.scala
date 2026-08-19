package homelab.nats.core


import homelab.nats.NatsError
import io.nats.client.Message
import zio.*


/**
 * The receive side of Core NATS: a bridge queue that a [[CoreSubscriber]] fills from the shared dispatcher,
 * drained one message at a time or in bounded batches, subscribing lazily on the first demand.
 *
 * '''A buffer is unavoidable here, and lossy on purpose.''' Core delivery is push — the client hands us a
 * message on its own thread whether or not anyone is ready — and Core has no server-side flow control, so
 * something must absorb the difference. The queue is `sliding`: under sustained overload it drops the oldest
 * rather than growing without bound, which is the honest behaviour for a substrate that already loses
 * messages with no live subscriber. JetStream needs none of this and has none — it pulls (see
 * `stream.Consumer`).
 *
 * Subscription is deferred to the first `one`/`many` because an unconsumed poll would otherwise pile
 * undrained messages into that queue from the moment it was built.
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
) {

  /**
   * Take the next delivered message, subscribing on first demand and then suspending until one arrives.
   *
   * @return the next message; aborts with [[NatsError.Connect]] if the lazy subscription can't be set up
   */
  def one: IO[NatsError, Message] = subscribed *> queue.take

  /**
   * Take a batch: at least one message (suspending until one arrives) and up to `maxMessages`. Returns
   * whatever is already buffered rather than waiting to fill, so it favours latency over packing.
   *
   * @param maxMessages the batch ceiling
   * @return the drained messages (1..`maxMessages`); aborts with [[NatsError.Connect]] if the lazy
   *         subscription can't be set up
   */
  def many(maxMessages: Int): IO[NatsError, List[Message]] =
    subscribed *> queue.takeBetween(1, maxMessages).map(_.toList)

  /**
   * Establish the subscription exactly once, on first demand.
   *
   * Double-checked: the hot path is a lock-free `started.get`; only the cold first call takes the lock, and
   * `started` flips to `true` only after the subscription succeeds, so a failed attempt is retried by the
   * next caller rather than leaving the poll permanently silent.
   *
   * @return noop once subscribed; aborts with [[NatsError.Connect]] if subscribing fails
   */
  private def subscribed: IO[NatsError, Unit] =
    started.get.flatMap:
      case true  => ZIO.unit
      case false =>
        subscribeLock.withPermit:
          started.get.flatMap:
            case true  => ZIO.unit
            case false => subscriber.subscribe(subject, queue, capturedScope) *> started.set(true)
}


object CorePoll {

  /** How many delivered-but-undrained messages the bridge holds before dropping the oldest. */
  private val bufferSize = 256

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
      queue   <- Queue.sliding[Message](bufferSize)
      started <- Ref.make(false)
      lock    <- Semaphore.make(1)
    yield new CorePoll(subject, queue, subscriber, started, lock, scope)
}
