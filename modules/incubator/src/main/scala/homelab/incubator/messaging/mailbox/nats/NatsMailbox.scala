package homelab.incubator.messaging.mailbox.nats


import homelab.incubator.messaging.mailbox.{ Address, Mailbox, Serde }
import homelab.incubator.messaging.nats.v5.NatsError
import io.nats.client.{ Connection, Dispatcher, MessageHandler }
import zio.*

import java.time.Duration as JavaDuration
import java.util.concurrent.CompletableFuture


/**
 * A NATS Core [[Mailbox]] — request-reply is native to NATS, so the mapping is almost free: [[expect]] mints a
 * fresh `_INBOX` subject and subscribes to it on a shared dispatcher; [[deliver]] is a plain publish to the
 * inbox subject (the [[Address]] is that subject verbatim). The reply bridges the dispatcher callback (a
 * non-fiber thread) into ZIO via a `CompletableFuture` — the '''single-shot''' escape that fits request-reply
 * (one value, then done), where the multi-shot Pub/Sub bridge used a `ZStream`.
 *
 * Delivery is fire-and-forget: a reply to a departed inbox is dropped. The inbox unsubscribes itself once the
 * reply lands (one-shot); if the timeout elapses first, `await` unsubscribes it and yields `None`.
 *
 * @param connection the live connection
 * @param dispatcher the shared dispatcher inbox subscriptions are registered on
 */
final class NatsMailbox(connection: Connection, dispatcher: Dispatcher) extends Mailbox[NatsError]:

  override def expect[B: Serde](timeout: Duration): IO[NatsError, Mailbox.Receipt[NatsError, B]] =
    ZIO
      .attemptBlocking {
        val inbox                   = connection.createInbox()
        val reply                   = new CompletableFuture[Array[Byte]]()
        val handler: MessageHandler = message => {
          reply.complete(message.getData)
          dispatcher.unsubscribe(inbox) // one-shot: drop the inbox once the reply lands
        }
        dispatcher.subscribe(inbox, handler)
        connection.flush(JavaDuration.ofSeconds(5)) // the SUB must be live before the address is handed out
        (inbox, reply)
      }
      .mapError(NatsError.Connect(_))
      .map: (inbox, reply) =>
        new Mailbox.Receipt[NatsError, B]:
          def address: Address = Address(inbox)

          def await: IO[NatsError, Option[B]] =
            ZIO
              .fromCompletableFuture(reply)
              .mapError(NatsError.Receive(_))
              .timeout(timeout)
              .flatMap:
                case None        => ZIO.attemptBlocking(dispatcher.unsubscribe(inbox)).ignore.as(None) // timed out — reap the sub
                case Some(bytes) =>
                  Serde[B].decode(bytes) match
                    case Right(value) => ZIO.succeed(Some(value))
                    case Left(reason) => ZIO.fail(NatsError.Decode(reason))

  override def deliver[B: Serde](address: Address, message: B): IO[NatsError, Unit] =
    ZIO
      .attemptBlocking(connection.publish(address, Serde[B].encode(message)))
      .mapError(NatsError.Publish(_))


object NatsMailbox:

  /** Unused default handler — each inbox supplies its own, but `createDispatcher` requires one. */
  private val noop: MessageHandler = _ => ()

  /**
   * Build a NATS mailbox over `connection`, backed by a fresh shared dispatcher closed with the scope.
   *
   * @param connection the live connection
   * @return the mailbox; aborts with [[NatsError.Connect]] if the dispatcher can't be created
   */
  def make(connection: Connection): ZIO[Scope, NatsError, Mailbox[NatsError]] =
    ZIO
      .acquireRelease(
        ZIO.attemptBlocking(connection.createDispatcher(noop)).mapError(NatsError.Connect(_))
      )(dispatcher => ZIO.attemptBlocking(connection.closeDispatcher(dispatcher)).ignore)
      .map(dispatcher => new NatsMailbox(connection, dispatcher))
