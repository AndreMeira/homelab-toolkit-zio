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
