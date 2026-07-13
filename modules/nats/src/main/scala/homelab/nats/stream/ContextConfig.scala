package homelab.nats.stream


import io.nats.client.api.{ AckPolicy, ConsumerConfiguration }
import zio.Duration


/**
 * The identity and ack tuning of a durable JetStream consumer in one place: which stream/subject it filters
 * and how the server treats un-acked delivery. Adapter-internal — callers configure via a consumer's
 * `Config`, and the factory folds it into this before handing it to [[JetStreamSubscriber]].
 *
 * @param stream        the (existing) stream name to attach to
 * @param durable       the durable consumer name (shared progress across restarts)
 * @param subject       the subject filter (e.g. `orders.>`)
 * @param ackWait       how long the server waits for an ack before redelivering
 * @param maxAckPending the backpressure bound on un-acked in-flight messages
 */
private[nats] final case class ContextConfig(
  stream: String,
  durable: String,
  subject: String,
  ackWait: Duration,
  maxAckPending: Int,
):

  /**
   * Render this as a jnats explicit-ack durable pull [[ConsumerConfiguration]].
   *
   * @return the equivalent consumer configuration
   */
  def toConsumerConfiguration: ConsumerConfiguration =
    ConsumerConfiguration
      .builder()
      .durable(durable)
      .filterSubject(subject)
      .ackPolicy(AckPolicy.Explicit)
      .ackWait(ackWait)
      .maxAckPending(maxAckPending.toLong)
      .build()
