package homelab.nats


import io.nats.client.Message
import zio.*


/**
 * The receive seam shared by both substrates: a source of already-delivered NATS [[Message]]s, drained one
 * at a time or in bounded batches. An implementation owns a bridge queue that a subscriber fills
 * asynchronously (a Core dispatcher, or a JetStream `consume` callback) and subscribes lazily on first
 * demand; the consumers built on top add decoding and settlement. Named for what the *consumer* does — take
 * the next unit of work — not for any network poll: delivery is push-based underneath.
 */
trait Poll:

  /**
   * Take the next delivered message, subscribing on first demand and then blocking (as a fiber) until one
   * arrives.
   *
   * @return the next message; aborts with [[NatsError.Connect]] if the lazy subscription can't be set up
   */
  def one: IO[NatsError, Message]

  /**
   * Take a batch of currently-buffered messages — at least one (blocking as a fiber until one arrives) and up
   * to `maxMessages` — subscribing on first demand. Returns whatever is already buffered rather than waiting
   * to fill, so it favours latency over packing.
   *
   * @param maxMessages the batch ceiling
   * @return the drained messages (1..`maxMessages`); aborts with [[NatsError.Connect]] if the lazy
   *         subscription can't be set up
   */
  def many(maxMessages: Int): IO[NatsError, List[Message]]


/**
 * Building blocks for the two [[Poll]] implementations. [[WithQueue]] supplies `one`/`many` by draining a
 * bridge queue; [[WithInit]] stacks a once-only lazy-initialisation gate in front of them. A concrete poll
 * mixes both — `extends Poll.WithQueue(queue) with Poll.WithInit(lock, started)` — and provides only the
 * [[WithInit.init]] effect.
 */
object Poll:

  /**
   * Supplies `one`/`many` by draining a bridge `queue` that a subscriber fills asynchronously. Pure queue
   * mechanics — no subscription and no failure of its own; stack [[WithInit]] on top to subscribe lazily on
   * first demand.
   *
   * @param queue the bridge queue delivered messages are taken from
   */
  trait WithQueue(queue: Queue[Message]) extends Poll:

    /**
     * Take the next message already buffered in the queue, blocking (as a fiber) until one is available.
     *
     * @return the next buffered message
     */
    override def one: IO[NatsError, Message] = queue.take

    /**
     * Take up to `maxMessages` currently-buffered messages (at least one, blocking as a fiber until one is
     * available). Returns whatever is buffered rather than waiting to fill, so it favours latency over packing.
     *
     * @param maxMessages the batch ceiling
     * @return the drained messages (1..`maxMessages`)
     */
    override def many(maxMessages: Int): IO[NatsError, List[Message]] =
      queue.takeBetween(1, maxMessages).map(_.toList)

  /**
   * A once-only lazy-initialisation gate stacked in front of `one`/`many`: the first call runs [[init]]
   * (e.g. establishes the subscription) exactly once before delegating to the underlying poll. '''Mix in
   * after [[WithQueue]]''' (`... with Poll.WithInit(...)`, rightmost) so the `abstract override`s wrap
   * WithQueue's concrete `one`/`many`; the reverse order leaves the gate un-stacked and [[init]] never runs.
   *
   * @param initLock serialises the cold init so concurrent first-callers run [[init]] once, not N times
   * @param started  tracks whether [[init]] has completed — the gate flips it, so [[init]] must not
   */
  trait WithInit(initLock: Semaphore, started: Ref[Boolean]) extends Poll:

    /**
     * The one-time initialisation effect — typically establishing the lazy subscription. Run once, on the
     * first `one`/`many`, guarded by [[start]]; it performs the effect only — the gate owns `started`, so an
     * implementation must not set it.
     *
     * @return unit once initialised; aborts with [[NatsError.Connect]] if initialisation fails
     */
    def init: IO[NatsError, Unit]

    /**
     * Ensure initialised (once), then take the next message.
     *
     * @return the next message; aborts with [[NatsError.Connect]] if the lazy initialisation fails
     */
    abstract override def one: IO[NatsError, Message] =
      start *> super.one

    /**
     * Ensure initialised (once), then take a batch.
     *
     * @param maxMessages the batch ceiling
     * @return the drained messages; aborts with [[NatsError.Connect]] if the lazy initialisation fails
     */
    abstract override def many(maxMessages: Int): IO[NatsError, List[Message]] =
      start *> super.many(maxMessages)

    /**
     * Run [[init]] exactly once, on first demand. Double-checked: the hot path is a lock-free `started.get`;
     * only the cold first call takes `initLock`, and `started` flips to `true` only after [[init]] succeeds —
     * so a failed init leaves the gate open for the next caller to retry, and concurrent first-callers
     * serialise on the permit instead of racing.
     *
     * @return unit once initialised (or already was); aborts with [[NatsError.Connect]] if [[init]] fails
     */
    private def start: IO[NatsError, Unit] =
      started.get.flatMap:
        case true  => ZIO.unit
        case false =>
          initLock.withPermit:
            started.get.flatMap:
              case true  => ZIO.unit
              case false => init *> started.set(true)
