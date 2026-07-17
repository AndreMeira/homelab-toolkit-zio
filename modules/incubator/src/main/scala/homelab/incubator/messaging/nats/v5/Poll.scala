package homelab.incubator.messaging.nats.v5


import io.nats.client.Message
import zio.*


trait Poll:
  def one: IO[NatsError, Message]
  def many(maxMessages: Int): IO[NatsError, List[Message]]
