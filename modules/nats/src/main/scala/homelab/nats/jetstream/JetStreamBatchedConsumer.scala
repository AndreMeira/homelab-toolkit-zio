package homelab.nats.jetstream


import homelab.nats.*
import homelab.common.messaging.Consumer
import io.nats.client.Message
import zio.*


/**
 * Shared batch-settle machinery for the JetStream batched consumers: decode the whole batch, handle any
 * undecodable messages per policy, run the handler on the decodable values, then ack/nak/term the batch.
 * The variants ([[JetStreamPollingBatchedConsumer]], [[JetStreamBridgedBatchedConsumer]]) differ only in
 * how they *receive* a batch; this base owns what happens *after*.
 *
 * '''Batch blast-radius:''' at batch granularity, `Surface` (decode or handler) fails the *whole* consume
 * and leaves every message in the batch unsettled — so all of them redeliver. Per-message isolation for
 * poison is available via `DecodeFailurePolicy.Discard` (each undecodable message is `term`ed and the
 * rest still processed).
 *
 * '''All-or-nothing handler:''' the handler receives the whole decodable batch and succeeds or fails as a
 * unit — on failure every message in it is settled the same way (nak / term / surface), so successful
 * items are redelivered too (idempotency covers them). And the whole batch must be processed and acked
 * within the consumer's `ackWait`, or it redelivers mid-processing — size batches accordingly.
 *
 * @param onDecodeFailure  what to do when a message in the batch can't be decoded
 * @param onHandlerFailure what to do when the handler fails on the batch
 * @param heartbeat        `inProgress()` keepalive interval while the handler runs, or `None` to disable
 * @tparam A the value consumed
 */
trait JetStreamBatchedConsumer[A: Serde](
  onDecodeFailure: DecodeFailurePolicy,
  onHandlerFailure: HandlerFailurePolicy,
  heartbeat: Option[Duration],
) extends Consumer.Batched[NatsError, A]:

  /**
   * Settle a received batch: decode all, apply `onDecodeFailure` to the undecodable ones, run `logic` on
   * the decodable values, then settle those per outcome (`ack` on success, `onHandlerFailure` on failure).
   *
   * @param messages the received batch
   * @param logic    the handler to run on the decoded values
   * @tparam E2 the widened error of `logic`
   * @return unit once settled; may abort with [[NatsError.Decode]], the handler's error, or [[NatsError.Ack]]
   */
  protected def settleBatch[E2 >: NatsError](messages: List[Message], logic: List[A] => IO[E2, Unit]): IO[E2, Unit] =
    val (undecodable, decodable) = messages.partitionMap: message =>
      Serde[A].decode(message.getData) match
        case Left(reason) => Left((message, reason))
        case Right(value) => Right((message, value))

    onDecodeFailure match
      case DecodeFailurePolicy.Surface if undecodable.nonEmpty =>
        // fail the whole batch → all unsettled → redeliver
        ZIO.fail(NatsError.Decode(undecodable.map((_, reason) => reason).mkString(", ")))

      case DecodeFailurePolicy.Discard =>
        settleAll(undecodable.map((message, _) => message))(_.term())
          *> ZIO.when(decodable.nonEmpty)(runBatch(decodable, logic)).unit

      case _ =>
        ZIO.when(decodable.nonEmpty)(runBatch(decodable, logic)).unit

  /**
   * Run `logic` on the decodable values and settle their messages per outcome.
   *
   * @param decodable the `(message, value)` pairs that decoded
   * @param logic     the handler
   * @tparam E2 the widened error of `logic`
   * @return unit; aborts with the handler's error (on `Surface`) or [[NatsError.Ack]]
   */
  private def runBatch[E2 >: NatsError](decodable: List[(Message, A)], logic: List[A] => IO[E2, Unit]): IO[E2, Unit] =
    val (messages, decoded) = decodable.unzip
    Heartbeat.wrap(heartbeat, messages)(logic(decoded)).foldZIO(
      error =>
        onHandlerFailure match
          case HandlerFailurePolicy.Redeliver => settleAll(messages)(_.nak())
          case HandlerFailurePolicy.Discard   => settleAll(messages)(_.term())
          case HandlerFailurePolicy.Surface   => ZIO.fail(error),
      _ => settleAll(messages)(_.ack()),
    )

  /**
   * Apply a blocking ack/nak/term to every message, tagging a failure as [[NatsError.Ack]].
   *
   * @param messages    the messages to settle
   * @param acknowledge the per-message ack side effect (`_.ack()` / `_.nak()` / `_.term()`)
   * @return unit once all are settled; aborts with [[NatsError.Ack]] on the first failure
   */
  private def settleAll(messages: List[Message])(acknowledge: Message => Unit): IO[NatsError, Unit] =
    ZIO.foreachDiscard(messages)(message => ZIO.attemptBlocking(acknowledge(message)).mapError(NatsError.Ack(_)))
