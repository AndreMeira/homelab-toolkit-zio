package homelab.nats.jetstream


import homelab.nats.*
import homelab.common.messaging.Consumer
import io.nats.client.Message
import zio.*


/**
 * Shared settle machinery for the JetStream consumers: decode, run the handler, then settle each message
 * per the configured failure policy. The variants ([[JetStreamPollingConsumer]],
 * [[JetStreamBridgedConsumer]]) differ only in how they *receive* a message; this base owns what happens
 * *after*. Core NATS is deliberately not part of this hierarchy — it has no ack, so no settlement.
 *
 * @param onDecodeFailure  what to do when a payload can't be decoded
 * @param onHandlerFailure what to do when the handler fails on a decoded message
 * @param heartbeat        `inProgress()` keepalive interval while the handler runs, or `None` to disable
 * @tparam A the value consumed
 */
trait JetStreamConsumer[A: Serde](
  onDecodeFailure: DecodeFailurePolicy,
  onHandlerFailure: HandlerFailurePolicy,
  heartbeat: Option[Duration],
) extends Consumer[NatsError, A] {

  /**
   * Decode, run `logic`, then settle per policy: `ack` on success; on a handler failure apply
   * `onHandlerFailure` (`nak` / `term` / surface the error); on an undecodable payload apply
   * `onDecodeFailure` (surface / `term`).
   *
   * @param message the received message
   * @param logic   the handler to run on the decoded value
   * @tparam E2 the widened error of `logic`
   * @return unit once settled; may abort with [[NatsError.Decode]], the handler's error, or [[NatsError.Ack]]
   */
  protected def settle[E2 >: NatsError](message: Message, logic: A => IO[E2, Unit]): IO[E2, Unit] =
    Serde[A].decode(message.getData) match
      case Left(reason) =>
        onDecodeFailure match
          case DecodeFailurePolicy.Surface => ZIO.fail(NatsError.Decode(reason))
          case DecodeFailurePolicy.Discard => ack(message.term())
      case Right(value) =>
        Heartbeat
          .wrap(heartbeat, List(message))(logic(value))
          .foldZIO(
            error =>
              onHandlerFailure match
                case HandlerFailurePolicy.Redeliver => ack(message.nak())
                case HandlerFailurePolicy.Discard   => ack(message.term())
                case HandlerFailurePolicy.Surface   => ZIO.fail(error),
            _ => ack(message.ack()),
          )

  /**
   * Run a blocking ack/nak/term call, tagging a failure as [[NatsError.Ack]].
   *
   * @param acknowledge the by-name ack side effect
   * @return unit once acknowledged; aborts with [[NatsError.Ack]] on failure
   */
  private def ack(acknowledge: => Unit): IO[NatsError, Unit] =
    ZIO.attemptBlocking(acknowledge).mapError(NatsError.Ack(_))
}
