package homelab.incubator.messaging.nats.v4


import homelab.common.messaging.Consumer
import io.nats.client.{ Connection, Message }
import zio.*


/**
 * A Core NATS [[Consumer]] — ephemeral, fire-and-forget. It is a plain queue drain: a [[NatsSubscriber]]
 * feeds this queue from a dispatcher, and `consume` takes and decodes as fibers. There is '''no ack''':
 * a message is delivered once; an undecodable payload is handled per `onDecodeFailure`
 * ([[DecodeFailurePolicy.Surface]] fails `consume`, [[DecodeFailurePolicy.Discard]] skips it and delivers
 * the next), and a handler failure surfaces to the caller (the message is already gone — nothing to retry).
 *
 * The class is deliberately standalone (it only knows about a queue); who fills the queue — and whether a
 * dispatcher is shared across many consumers — is entirely [[NatsSubscriber]]'s concern.
 *
 * @param queue           the queue a [[NatsSubscriber]] offers received messages into
 * @param onDecodeFailure what to do when a payload can't be decoded
 * @tparam A the value consumed
 */
final private[v4] class CoreConsumer[A](
  queue: Queue[Message],
  onDecodeFailure: DecodeFailurePolicy,
)(using
  serde: Serde[A]
) extends Consumer[NatsError, A]:

  override def consume[E2 >: NatsError](logic: A => IO[E2, Unit]): IO[E2, Unit] =
    queue.take.flatMap: message =>
      serde.decode(message.getData) match
        case Right(value) => logic(value)
        case Left(reason) =>
          onDecodeFailure match
            case DecodeFailurePolicy.Surface => ZIO.fail(NatsError.Decode(reason))
            case DecodeFailurePolicy.Discard => consume(logic) // skip it and deliver the next


object CoreConsumer:

  /**
   * Convenience: a single ephemeral consumer on its own dispatcher. For many consumers sharing one
   * dispatcher (O(1) delivery threads), mint them from a [[NatsSubscriber]] instead.
   *
   * @param connection      the live make
   * @param subject         the subject to subscribe to (may be a wildcard, e.g. `orders.*`)
   * @param onDecodeFailure what to do when a payload can't be decoded (default [[DecodeFailurePolicy.Surface]])
   * @tparam A the value consumed
   * @return the consumer; aborts with [[NatsError.Connect]] if subscribing fails
   */
  def make[A](
    connection: Connection,
    subject: String,
    onDecodeFailure: DecodeFailurePolicy = DecodeFailurePolicy.Surface,
  )(using Serde[A]
  ): ZIO[Scope, NatsError, Consumer[NatsError, A]] =
    NatsSubscriber.make(connection).flatMap(_.consumer(subject, onDecodeFailure))
