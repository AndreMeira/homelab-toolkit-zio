package homelab.incubator.messaging.nats.v2


import io.nats.client.{ Connection, Dispatcher, Message, MessageHandler }
import zio.*
import zio.stream.ZStream


/**
 * Owns one shared NATS `Dispatcher` and mints [[NatsConsumer]]s onto it — the push→queue bridge.
 *
 * Each subscription is wrapped as a `ZStream.asyncInterrupt`: the dispatcher callback (running on a
 * NATS-managed thread) pushes bytes into the stream via its thread-safe `emit`, and a scoped fiber drains
 * the stream into a per-consumer ZIO [[Queue]] that [[NatsConsumer.consume]] pulls from as fibers. So N
 * consumers cost O(1) dispatcher threads, and the consume side never parks a thread.
 *
 * '''Why `ZStream` here.''' Bridging a *multi-shot* callback (a subscription fires per message, forever)
 * into ZIO needs a buffer, and feeding it from a non-fiber thread needs a runtime escape. `ZIO.async`
 * doesn't fit — it is *single-shot* (one value, then done), so it would drop every message after the
 * first. `ZStream.asyncInterrupt` is purpose-built for multi-shot callbacks and encapsulates the escape
 * (thread-safety, backpressure, and cleanup via its canceler) correctly, so we don't hand-roll
 * `runtime.unsafe.run`. '''`ZStream` is an adapter-internal detail only''' — the public surface stays the
 * `Consumer` port; no stream is ever exposed. (`ZIO.async` *does* fit the single-shot Mailbox
 * request/reply path — a different capability.)
 *
 * @param dispatcher the shared dispatcher subscriptions are registered on
 */
final class NatsSubscriber(dispatcher: Dispatcher):

  /**
   * Subscribe to `subject` on the shared dispatcher and expose it as a [[NatsConsumer]]. The subscription
   * and its drain fiber are torn down when the scope closes.
   *
   * @param subject the subject to subscribe to (may be a wildcard, e.g. `orders.*`)
   * @param serde   decodes wire bytes into a value
   * @tparam A the value consumed
   * @return the consumer
   */
  def consumer[A](subject: String, serde: Serde[A]): ZIO[Scope, NatsError, NatsConsumer[A]] =
    for
      queue      <- Queue.unbounded[Array[Byte]]
      subscribed <- Promise.make[NatsError, Unit]
      _          <- subscribe(subject, subscribed).runForeach(queue.offer).forkScoped
      _          <- subscribed.await // don't return until the SUB is on the wire (else an early publish is lost)
    yield new NatsConsumer(queue, serde)

  /**
   * The subject's messages as a stream: a `ZStream.asyncScoped` whose registration subscribes on the
   * shared dispatcher (unsubscribing when the stream finalizes) and completes `subscribed` once the
   * subscription is established — the ordering guarantee that lets the caller safely publish afterwards.
   *
   * @param subject    the subject to subscribe to
   * @param subscribed completed when the subscription is live, or failed if subscribing fails
   * @return the received payloads (adapter-internal; never surfaced)
   */
  private def subscribe(subject: String, subscribed: Promise[NatsError, Unit]): ZStream[Any, NatsError, Array[Byte]] =
    ZStream.asyncScoped[Any, NatsError, Array[Byte]] { emit =>
      ZIO
        .acquireRelease(
          ZIO
            .attemptBlocking(dispatcher.subscribe(subject, (message: Message) => emit(ZIO.succeed(Chunk.single(message.getData)))))
            .mapError(NatsError.Connect(_))
        )(subscription => ZIO.attemptBlocking(dispatcher.unsubscribe(subscription)).ignore)
        .tapError(subscribed.fail(_))
        .zipRight(subscribed.succeed(()))
    }


object NatsSubscriber:

  /**
   * Create a subscriber over `connection`, backed by a fresh shared dispatcher closed when the scope
   * closes.
   *
   * @param connection the live NATS connection
   * @return the subscriber; aborts with [[NatsError.Connect]] if the dispatcher can't be created
   */
  def make(connection: Connection): ZIO[Scope, NatsError, NatsSubscriber] =
    ZIO
      .acquireRelease(
        ZIO.attemptBlocking(connection.createDispatcher(noop)).mapError(NatsError.Connect(_))
      )(dispatcher => ZIO.attemptBlocking(connection.closeDispatcher(dispatcher)).ignore)
      .map(dispatcher => new NatsSubscriber(dispatcher))

  /** Unused default handler — every subscription supplies its own, but `createDispatcher` requires one. */
  private val noop: MessageHandler = _ => ()
