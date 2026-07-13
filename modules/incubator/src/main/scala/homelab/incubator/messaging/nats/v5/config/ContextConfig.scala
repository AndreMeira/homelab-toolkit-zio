package homelab.incubator.messaging.nats.v5.config


import io.nats.client.api.{ AckPolicy, ConsumerConfiguration }
import zio.Duration


case class ContextConfig(
  stream: String,
  durable: String,
  subject: String,
  ackWait: Duration,
  maxAckPending: Int,
) {
  def toConsumerConfiguration: ConsumerConfiguration =
    ConsumerConfiguration
      .builder()
      .durable(durable)
      .filterSubject(subject)
      .ackPolicy(AckPolicy.Explicit)
      .ackWait(ackWait)
      .maxAckPending(maxAckPending.toLong)
      .build()
}
