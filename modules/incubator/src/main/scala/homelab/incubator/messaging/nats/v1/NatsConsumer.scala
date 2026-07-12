package homelab.incubator.messaging.nats.v1


import homelab.common.messaging.Consumer
import io.nats.client.{ Connection, Subscription }
import zio.*


/**
 * A [[Consumer]] backed by a synchronous NATS subscription: each `consume` blocks for the next message,
 * decodes it, and runs `logic`. Blocking receive is interruptible, so a scope close cancels a parked
 * consumer.
 *
 * v1 is Core NATS: there is '''no ack''' — a message is delivered once and gone. If `logic` fails, the
 * message is not redelivered (contrast the port's general contract, where redelivery is possible and
 * handlers must be idempotent — that arrives with JetStream in v3).
 *
 * '''Scaling limitation.''' `nextMessage` parks a blocking (platform) thread for the whole time a
 * consumer waits. ZIO's blocking pool is unbounded, so this won't deadlock — but it is one thread per
 * parked consumer, so consumer count per instance is bounded by the thread/memory budget (thousands),
 * not by NATS. v2 removes this by bridging the async `Dispatcher` callback into a ZIO `Queue`, making
 * `consume` fiber-based (see `homelab.incubator.messaging.nats.v2`).
 *
 * @param subscription the live subscription this consumer reads from
 * @param serde        decodes wire bytes into a value
 * @param pollTimeout  how long each `nextMessage` blocks before retrying (transparent to callers)
 * @tparam A the value consumed
 */
final class NatsConsumer[A](
  subscription: Subscription,
  serde: Serde[A],
  pollTimeout: Duration,
) extends Consumer[NatsError, A]:

  override def consume[E2 >: NatsError](logic: A => IO[E2, Unit]): IO[E2, Unit] =
    receive.flatMap(logic)

  /**
   * Block for the next message, retrying across `pollTimeout` expiries, then decode it.
   *
   * @return the next decoded value; aborts with [[NatsError.Receive]] on I/O failure or
   *         [[NatsError.Decode]] on a malformed payload
   */
  private def receive: IO[NatsError, A] =
    ZIO
      .attemptBlockingInterrupt(Option(subscription.nextMessage(pollTimeout)))
      .mapError(NatsError.Receive(_))
      .flatMap {
        case Some(message) =>
          serde.decode(message.getData) match
            case Right(value)  => ZIO.succeed(value)
            case Left(reason)  => ZIO.fail(NatsError.Decode(reason))
        case None => receive // timeout with no message — keep waiting
      }


object NatsConsumer:

  /**
   * Subscribe to `subject`, unsubscribing when the scope closes.
   *
   * @param connection  the live NATS connection
   * @param subject     the subject to subscribe to (may be a wildcard, e.g. `orders.*`)
   * @param serde       decodes wire bytes into a value
   * @param pollTimeout how long each internal `nextMessage` blocks before retrying
   * @tparam A the value consumed
   * @return the consumer; aborts with [[NatsError.Connect]] if subscribing fails
   */
  def make[A](
    connection: Connection,
    subject: String,
    serde: Serde[A],
    pollTimeout: Duration = 1.second,
  ): ZIO[Scope, NatsError, NatsConsumer[A]] =
    ZIO
      .acquireRelease(
        ZIO.attemptBlocking(connection.subscribe(subject)).mapError(NatsError.Connect(_))
      )(subscription => ZIO.attemptBlocking(subscription.unsubscribe()).ignore)
      .map(subscription => new NatsConsumer(subscription, serde, pollTimeout))
