package homelab.nats.jetstream


import homelab.nats.*
import homelab.common.messaging.Consumer
import io.nats.client.*
import zio.*

import scala.util.chaining.scalaUtilChainingOps


/**
 * A JetStream batched [[Consumer.Batched]] that '''fetches''': each `consume` pulls up to `batchSize`
 * messages via JetStream's native `fetch`, then settles the batch with explicit ack. The idiomatic
 * batched pull. '''Handlers must be idempotent''' (redelivery is real).
 *
 * '''Latency:''' a fetch waits up to `maxWait` to *fill* the batch, so under sparse traffic a message can
 * sit up to `maxWait` before delivery — trading latency for packing. Contrast
 * [[JetStreamBridgedBatchedConsumer]], which returns immediately with whatever is buffered.
 *
 * @param context   the durable pull consumer
 * @param batchSize the maximum messages per fetch
 * @param maxWait   how long a fetch waits to fill the batch before returning what it has (≥ 1s)
 * @tparam A the value consumed
 */
final private[nats] class JetStreamPollingBatchedConsumer[A: Serde](
  context: ConsumerContext,
  batchSize: Int,
  maxWait: Duration,
  onDecodeFailure: DecodeFailurePolicy,
  onHandlerFailure: HandlerFailurePolicy,
  heartbeat: Option[Duration],
) extends JetStreamBatchedConsumer[A](onDecodeFailure, onHandlerFailure, heartbeat):

  override def consume[E2 >: NatsError](logic: List[A] => IO[E2, Unit]): IO[E2, Unit] =
    fetch.flatMap {
      case Nil   => consume(logic) // empty fetch (nothing within maxWait) — retry
      case batch => settleBatch(batch, logic)
    }

  /**
   * Fetch up to `batchSize` messages (blocking, interruptible), draining the fetch consumer to a list.
   *
   * @return the fetched batch (possibly empty); aborts with [[NatsError.Receive]] on failure
   */
  private def fetch: IO[NatsError, List[Message]] =
    ZIO
      .attemptBlockingInterrupt:
        context.fetch(fetchOptions).pipe(drainFetcher)
      .mapError(NatsError.Receive(_))

  /**
   * Build the JetStream fetch options used by [[fetch]], constrained to this consumer's batch size and
   * maximum wait time.
   *
   * @return a configured fetch request that asks for at most `batchSize` messages and waits up to `maxWait`
   */
  private def fetchOptions: FetchConsumeOptions =
    FetchConsumeOptions
      .builder()
      .maxMessages(batchSize)
      .expiresIn(maxWait.toMillis)
      .build()

  /**
   * Drain `fetcher` into a strict `List`, pulling messages until `nextMessage()` returns `null`.
   *
   * @param fetcher the JetStream fetch consumer to drain
   * @return all fetched messages, in fetch order
   */
  private def drainFetcher(fetcher: FetchConsumer): List[Message] =
    List.unfold(fetcher): fetcher =>
      Option(fetcher.nextMessage()) match
        case Some(message) => Some(message -> fetcher)
        case None          => None


object JetStreamPollingBatchedConsumer:

  /**
   * Tuning for a batched polling consumer.
   *
   * @param batchSize        the maximum messages per fetch
   * @param maxWait          how long a fetch waits to fill the batch before returning what it has (≥ 1s)
   * @param ackWait          how long the server waits for an ack before redelivering
   * @param maxAckPending    the backpressure bound on un-acked in-flight messages
   * @param onDecodeFailure  what to do when a message can't be decoded
   * @param onHandlerFailure what to do when the handler fails on the batch
   * @param heartbeat        `inProgress()` keepalive interval while the handler runs, or `None` to disable
   */
  final case class Config(
    batchSize: Int = 100,
    maxWait: Duration = 1.second,
    ackWait: Duration = 30.seconds,
    maxAckPending: Int = 256,
    onDecodeFailure: DecodeFailurePolicy = DecodeFailurePolicy.Surface,
    onHandlerFailure: HandlerFailurePolicy = HandlerFailurePolicy.Redeliver,
    heartbeat: Option[Duration] = None,
  )

  /**
   * Attach a durable pull consumer to an existing `stream` and expose it as a [[Consumer.Batched]].
   *
   * @param connection the live connection
   * @param stream     the (existing) stream name
   * @param durable    the durable consumer name
   * @param subject    the subject filter
   * @param config     batch / ack / backpressure tuning
   * @tparam A the value consumed
   * @return the consumer; aborts with [[NatsError.Connect]] if the consumer can't be created
   */
  def make[A](
    connection: Connection,
    stream: String,
    durable: String,
    subject: String,
    config: Config = Config(),
  )(using Serde[A]
  ): IO[NatsError, Consumer.Batched[NatsError, A]] =
    NatsConnection
      .durableConsumer(connection, stream, durable, subject, config.ackWait, config.maxAckPending)
      .map(context =>
        new JetStreamPollingBatchedConsumer(
          context,
          config.batchSize,
          config.maxWait,
          config.onDecodeFailure,
          config.onHandlerFailure,
          config.heartbeat,
        )
      )
