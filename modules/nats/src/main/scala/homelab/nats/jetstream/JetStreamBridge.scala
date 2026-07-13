package homelab.nats.jetstream


import homelab.nats.*
import io.nats.client.{ ConsumerContext, Message }
import zio.*
import zio.stream.ZStream


/**
 * The shared JetStream push→queue bridge: attaches an async `consume` delivery to a durable consumer and
 * feeds it into a ZIO [[Queue]]. Used by both [[JetStreamBridgedConsumer]] and
 * [[JetStreamBridgedBatchedConsumer]] — they differ only in how they *drain* the queue (per item vs in
 * batches), so the delivery setup lives here once.
 */
private[nats] object JetStreamBridge:

  /**
   * Bridge `context`'s async delivery into a fresh unbounded queue, torn down on scope close. The
   * returned effect completes only once delivery is live (so the caller may publish safely).
   *
   * @param context the durable consumer to attach delivery to
   * @return the fed queue; aborts with [[NatsError.Connect]] if delivery can't be set up
   */
  def deliveryQueue(context: ConsumerContext): ZIO[Scope, NatsError, Queue[Message]] =
    for
      queue   <- Queue.unbounded[Message]
      started <- Promise.make[NatsError, Unit]
      _       <- deliveries(context, started).runForeach(queue.offer).forkScoped
      _       <- started.await
    yield queue

  /**
   * The consumer's messages as a stream: `context.consume` delivers to a `ZStream.asyncScoped`; the
   * `MessageConsumer` is stopped when the stream finalizes, and `started` completes once delivery is live.
   *
   * @param context the consumer context to attach delivery to
   * @param started completed once the async delivery is wired up
   * @return the delivered messages (adapter-internal; never surfaced)
   */
  private def deliveries(
    context: ConsumerContext,
    started: Promise[NatsError, Unit],
  ): ZStream[Any, NatsError, Message] =
    ZStream.asyncScoped[Any, NatsError, Message]: emit =>
      ZIO
        .acquireRelease(
          ZIO
            .attemptBlocking(context.consume(message => emit(ZIO.succeed(Chunk.single(message)))))
            .mapError(NatsError.Connect(_))
        )(consumer => ZIO.attemptBlocking(consumer.stop()).ignore)
        .tapError(started.fail(_))
        .zipRight(started.succeed(()))
