package homelab.incubator.messaging.nats.v3


import homelab.common.messaging.Consumer
import io.nats.client.api.{ AckPolicy, ConsumerConfiguration }
import io.nats.client.{ Connection, ConsumerContext, Message, StreamContext }
import zio.*
import zio.stream.ZStream


/**
 * Shared JetStream consume machinery for both delivery models. The public surface is the
 * `Consumer[NatsError, A]` port; the two models are chosen by smart constructor:
 *
 *   - [[NatsConsumer.polling]] — a blocking pull (`consumerContext.next`) loop. Simple, demand-driven,
 *     one thread per consumer. Prefer for '''few''' subscriptions where simplicity wins.
 *   - [[NatsConsumer.bridged]] — the async `consume` callback bridged into a ZIO [[Queue]] via `ZStream`
 *     (the v2 pattern). Fiber-based, O(1) threads. Prefer for '''many''' subscriptions / high fan-out.
 *
 * Both settle each message with '''explicit ack''': `ack` after the handler succeeds, `nak` (redeliver)
 * if it fails, `term` (dead-letter) if the payload can't be decoded. Per-message logic failures are
 * therefore retried via redelivery and never surface from `consume` — so a run loop keeps going, and
 * '''handlers must be idempotent'''. `consume` aborts only on infrastructure failure (a failed
 * receive/ack). Backpressure is the consumer's `maxAckPending` — server-enforced, so the bridge queue
 * can stay unbounded and still be bounded overall.
 */
object NatsConsumer:

  /** Default backpressure: the server holds delivery once this many messages are un-acked. */
  private val defaultMaxAckPending = 256

  /**
   * Tuning for a [[polling]] consumer — everything beyond the stream/durable/subject identity.
   *
   * @param ackWait       how long the server waits for an ack before redelivering
   * @param maxAckPending the backpressure bound on un-acked in-flight messages
   * @param pollTimeout   how long each blocking `next` waits before retrying
   */
  final case class PollingConfig(
    ackWait: Duration = 30.seconds,
    maxAckPending: Int = defaultMaxAckPending,
    pollTimeout: Duration = 1.second,
  )

  /**
   * Tuning for a [[bridged]] consumer — everything beyond the stream/durable/subject identity.
   *
   * @param ackWait       how long the server waits for an ack before redelivering
   * @param maxAckPending the backpressure bound on un-acked in-flight messages
   */
  final case class BridgedConfig(
    ackWait: Duration = 30.seconds,
    maxAckPending: Int = defaultMaxAckPending,
  )

  /**
   * A polling consumer over `streamContext`: a durable pull consumer drained one message at a time by a
   * blocking `next`. One thread is parked per concurrent `consume`.
   *
   * @param streamContext the stream to consume from
   * @param durable       the durable consumer name (shared progress across restarts)
   * @param subject       the subject filter (e.g. `orders.>`)
   * @param config        ack / backpressure / poll tuning (see [[PollingConfig]])
   * @tparam A the value consumed
   * @return the consumer; aborts with [[NatsError.Connect]] if the consumer can't be created
   */
  def polling[A](
    streamContext: StreamContext,
    durable: String,
    subject: String,
    config: PollingConfig,
  )(using Serde[A]): IO[NatsError, Consumer[NatsError, A]] =
    consumerContext(streamContext, durable, subject, config.ackWait, config.maxAckPending)
      .map(context => new Polling(context, config.pollTimeout))

  /**
   * As [[polling]], but resolves the stream by name off `make` — the convenience form for callers
   * that don't already hold a [[StreamContext]]. The stream must already exist (create it via
   * [[NatsConnection.stream]]); sharing one `StreamContext` across many consumers instead saves a
   * per-consumer lookup.
   *
   * @param connection the live make
   * @param stream     the (existing) stream name
   * @param durable    the durable consumer name
   * @param subject    the subject filter
   * @param config     ack / backpressure / poll tuning (see [[PollingConfig]])
   * @tparam A the value consumed
   * @return the consumer; aborts with [[NatsError.Connect]] if the stream lookup or consumer setup fails
   */
  def polling[A](
    connection: Connection,
    stream: String,
    durable: String,
    subject: String,
    config: PollingConfig = PollingConfig(),
  )(using Serde[A]): IO[NatsError, Consumer[NatsError, A]] =
    streamContextOf(connection, stream).flatMap(context => polling(context, durable, subject, config))

  /**
   * A bridged consumer over `streamContext`: the async `consume` callback pushes messages into a ZIO
   * [[Queue]] (via `ZStream`, an adapter-internal detail), which `consume` drains as fibers. Many
   * consumers share the make's threads.
   *
   * @param streamContext the stream to consume from
   * @param durable       the durable consumer name
   * @param subject       the subject filter
   * @param config        ack / backpressure tuning (see [[BridgedConfig]])
   * @tparam A the value consumed
   * @return the consumer; aborts with [[NatsError.Connect]] if the consumer can't be set up
   */
  def bridged[A](
    streamContext: StreamContext,
    durable: String,
    subject: String,
    config: BridgedConfig,
  )(using Serde[A]): ZIO[Scope, NatsError, Consumer[NatsError, A]] =
    for
      context <- consumerContext(streamContext, durable, subject, config.ackWait, config.maxAckPending)
      queue   <- Queue.unbounded[Message] // bounded overall by config.maxAckPending
      started <- Promise.make[NatsError, Unit]
      _       <- deliveries(context, started).runForeach(queue.offer).forkScoped
      _       <- started.await // don't return until delivery is wired up
    yield new Bridged(queue)

  /**
   * As [[bridged]], but resolves the stream by name off `make` — the convenience form for callers
   * that don't already hold a [[StreamContext]]. The stream must already exist (create it via
   * [[NatsConnection.stream]]); sharing one `StreamContext` across many consumers instead saves a
   * per-consumer lookup.
   *
   * @param connection the live make
   * @param stream     the (existing) stream name
   * @param durable    the durable consumer name
   * @param subject    the subject filter
   * @param config     ack / backpressure tuning (see [[BridgedConfig]])
   * @tparam A the value consumed
   * @return the consumer; aborts with [[NatsError.Connect]] if the stream lookup or consumer setup fails
   */
  def bridged[A](
    connection: Connection,
    stream: String,
    durable: String,
    subject: String,
    config: BridgedConfig = BridgedConfig(),
  )(using Serde[A]): ZIO[Scope, NatsError, Consumer[NatsError, A]] =
    streamContextOf(connection, stream).flatMap(context => bridged(context, durable, subject, config))

  /**
   * Create (or attach to) an explicit-ack durable pull consumer.
   *
   * @param streamContext the stream to attach to
   * @param durable       the durable consumer name
   * @param subject       the subject filter
   * @param ackWait       redelivery timeout
   * @param maxAckPending un-acked in-flight bound
   * @return its context; aborts with [[NatsError.Connect]] on failure
   */
  /**
   * Look up an existing stream's [[StreamContext]] by name.
   *
   * @param connection the live make
   * @param stream     the stream name (must already exist)
   * @return its context; aborts with [[NatsError.Connect]] if the stream is missing or the lookup fails
   */
  private def streamContextOf(connection: Connection, stream: String): IO[NatsError, StreamContext] =
    ZIO.attemptBlocking(connection.getStreamContext(stream)).mapError(NatsError.Connect(_))

  private def consumerContext(
    streamContext: StreamContext,
    durable: String,
    subject: String,
    ackWait: Duration,
    maxAckPending: Int,
  ): IO[NatsError, ConsumerContext] =
    ZIO
      .attemptBlocking {
        val configuration = ConsumerConfiguration
          .builder()
          .durable(durable)
          .filterSubject(subject)
          .ackPolicy(AckPolicy.Explicit)
          .ackWait(ackWait)
          .maxAckPending(maxAckPending.toLong)
          .build()
        streamContext.createOrUpdateConsumer(configuration)
      }
      .mapError(NatsError.Connect(_))

  /**
   * The consumer's messages as a stream: `context.consume` delivers to the callback, which pushes into a
   * `ZStream.asyncScoped`; the [[io.nats.client.MessageConsumer]] is stopped when the stream finalizes,
   * and `started` completes once delivery is live (so the caller may publish safely).
   *
   * @param context the consumer context to attach delivery to
   * @param started completed once the async delivery is wired up
   * @return the delivered messages (adapter-internal; never surfaced)
   */
  private def deliveries(
    context: ConsumerContext,
    started: Promise[NatsError, Unit],
  ): ZStream[Any, NatsError, Message] =
    ZStream.asyncScoped[Any, NatsError, Message] { emit =>
      ZIO
        .acquireRelease(
          ZIO
            .attemptBlocking(context.consume(message => emit(ZIO.succeed(Chunk.single(message)))))
            .mapError(NatsError.Connect(_))
        )(consumer => ZIO.attemptBlocking(consumer.stop()).ignore)
        .tapError(started.fail(_))
        .zipRight(started.succeed(()))
    }

  /**
   * Settle a received message against `logic`: decode, run, then ack on success / nak on failure / term
   * on an undecodable payload. Never surfaces a per-message logic failure — redelivery handles retries.
   *
   * @param message the received message
   * @param logic   the handler to run on the decoded value
   * @param serde   decodes its payload
   * @tparam E2 the widened error of `logic`
   * @tparam A  the decoded value type
   * @return unit once settled; aborts with [[NatsError.Ack]] only if the ack/nak/term call fails
   */
  private def settle[E2 >: NatsError, A](message: Message, logic: A => IO[E2, Unit])(using serde: Serde[A]): IO[E2, Unit] =
    serde.decode(message.getData) match
      case Left(_)      => ack(message.term()) // poison — dead-letter, don't redeliver
      case Right(value) => logic(value).foldZIO(_ => ack(message.nak()), _ => ack(message.ack()))

  /**
   * Run a blocking ack/nak/term call, tagging a failure as [[NatsError.Ack]].
   *
   * @param acknowledge the by-name ack side effect
   * @return unit once acknowledged; aborts with [[NatsError.Ack]] on failure
   */
  private def ack(acknowledge: => Unit): IO[NatsError, Unit] =
    ZIO.attemptBlocking(acknowledge).mapError(NatsError.Ack(_))

  /**
   * The polling model: block on `next` (retrying across timeouts), then settle.
   *
   * @param context     the durable pull consumer
   * @param pollTimeout each blocking `next`'s wait before retrying
   * @param serde       decodes wire bytes into a value
   * @tparam A the value consumed
   */
  private final class Polling[A](context: ConsumerContext, pollTimeout: Duration)(using Serde[A])
      extends Consumer[NatsError, A]:

    override def consume[E2 >: NatsError](logic: A => IO[E2, Unit]): IO[E2, Unit] =
      receive.flatMap(message => settle(message, logic))

    /**
     * Block for the next message, retrying across `pollTimeout` expiries.
     *
     * @return the next message; aborts with [[NatsError.Connect]] on receive failure
     */
    private def receive: IO[NatsError, Message] =
      ZIO
        .attemptBlockingInterrupt(Option(context.next(pollTimeout)))
        .mapError(NatsError.Connect(_))
        .flatMap {
          case Some(message) => ZIO.succeed(message)
          case None          => receive
        }

  /**
   * The bridged model: pull the next delivered message off the bridge queue, then settle. Fiber-based.
   *
   * @param queue the bridge queue fed by the async delivery callback
   * @param serde decodes wire bytes into a value
   * @tparam A the value consumed
   */
  private final class Bridged[A](queue: Queue[Message])(using Serde[A]) extends Consumer[NatsError, A]:

    override def consume[E2 >: NatsError](logic: A => IO[E2, Unit]): IO[E2, Unit] =
      queue.take.flatMap(message => settle(message, logic))
