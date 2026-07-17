package homelab.incubator.messaging.nats.v2


import homelab.common.messaging.Consumer
import zio.*


/**
 * A [[Consumer]] fed by a ZIO [[Queue]] that a NATS `Dispatcher` callback offers into (see
 * [[NatsSubscriber]]). `consume` is a plain `queue.take` — fully fiber-based, parking no thread — so an
 * instance runs as many consumers as it can run fibers, independent of the (shared) dispatcher threads.
 * This is the v2 answer to v1's thread-per-consumer blocking receive; structurally it is the same shape
 * as the in-memory `QueueConsumer`, only fed by NATS instead of a local queue.
 *
 * @param queue the bridge queue the dispatcher offers received payloads into
 * @param serde decodes wire bytes into a value
 * @tparam A the value consumed
 */
final class NatsConsumer[A](queue: Queue[Array[Byte]], serde: Serde[A]) extends Consumer[NatsError, A]:

  override def consume[E2 >: NatsError](logic: A => IO[E2, Unit]): IO[E2, Unit] =
    queue.take.flatMap: bytes =>
      serde.decode(bytes) match
        case Right(value) => logic(value)
        case Left(reason) => ZIO.fail(NatsError.Decode(reason))
