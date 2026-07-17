package homelab.incubator.messaging.mailbox.nats


import homelab.incubator.messaging.mailbox.{ Address, Mailbox, Pending, Serde }
import homelab.incubator.messaging.nats.v5.NatsError
import homelab.incubator.messaging.nats.v5.core.CoreSubscriber
import io.nats.client.{ Connection, Message }
import zio.*

import java.util.UUID
import java.util.concurrent.TimeUnit

import scala.util.Try


/**
 * A NATS Core [[Mailbox]] using a '''single, long-lived reply subscription''' plus subject-suffix
 * correlation — the alternative to [[NatsMailbox]]'s subscribe-per-reply.
 *
 * At `make` it subscribes '''once''' to a per-mailbox wildcard subject (`_MAILBOX.<node>.*`, drained by a
 * forked reply loop — the "Processor started at boot"), so:
 *   - `expect` is '''network-free''': it just mints a fresh reply subject under the prefix and registers a
 *     local promise — no per-request `subscribe`/`flush`/`unsubscribe` churn.
 *   - `deliver` stays a plain publish to the reply subject.
 *   - correlation rides in the '''subject''' (the trailing UUID token), so the generic reply loop routes each
 *     reply to its awaiting expectation without decoding the body or reading a header.
 *
 * Timeouts need no fiber: `await` bounds on the entry's remaining time (`deadline - now`), and every
 * `sweepEvery`-th `expect` drops entries already past their deadline, reaping abandoned expectations. The
 * cost, versus [[NatsMailbox]]: a background reply fiber and a per-node reply namespace (so a reply reaches
 * the node holding the promise).
 *
 * @param connection   the live connection (for publishing replies)
 * @param prefix       this mailbox's reply-subject prefix (`_MAILBOX.<node>`); reply subjects are `prefix.<uuid>`
 * @param pending      the correlation map — reply UUID → the awaiting expectation
 * @param sweepCounter counts `expect` calls, to gate the opportunistic sweep
 * @param sweepEvery   sweep expired entries once every this many `expect`s
 */
final class SharedInboxNatsMailbox(
  connection: Connection,
  prefix: String,
  pending: Ref[Map[UUID, Pending[NatsError]]],
  sweepCounter: Ref[Int],
  sweepEvery: Int,
) extends Mailbox[NatsError]:

  override def expect[B: Serde](timeout: Duration): UIO[Mailbox.Receipt[NatsError, B]] =
    for
      now     <- Clock.currentTime(TimeUnit.MILLISECONDS)
      replyId <- Random.nextUUID
      promise <- Promise.make[NatsError, Array[Byte]]
      count   <- sweepCounter.updateAndGet(_ + 1)
      deadline = now + timeout.toMillis
      _       <- pending.update: map =>
                   val kept = if count % sweepEvery == 0 then map.filter { case (_, entry) => entry.deadline > now } else map
                   kept + (replyId -> Pending(promise, deadline))
    yield new Mailbox.Receipt[NatsError, B]:
      def address: Address = Address(s"$prefix.$replyId")

      def await: IO[NatsError, Option[B]] =
        Clock
          .currentTime(TimeUnit.MILLISECONDS)
          .flatMap: awaitNow =>
            promise.await
              .timeout(Duration.fromMillis((deadline - awaitNow).max(0L)))
              .flatMap:
                case None        => pending.update(_ - replyId).as(None) // timed out — reap the expectation
                case Some(bytes) =>
                  Serde[B].decode(bytes) match
                    case Right(value) => ZIO.succeed(Some(value))
                    case Left(reason) => ZIO.fail(NatsError.Decode(reason))

  override def deliver[B: Serde](address: Address, message: B): IO[NatsError, Unit] =
    ZIO
      .attemptBlocking(connection.publish(address, Serde[B].encode(message)))
      .mapError(NatsError.Publish(_))

  /**
   * Route one reply to its awaiting expectation by the UUID in its subject, removing the entry (one-shot). An
   * unknown UUID (a late reply after timeout) or an unparseable subject is dropped.
   *
   * @param message the received reply
   * @return unit once routed or dropped
   */
  private[nats] def dispatch(message: Message): UIO[Unit] =
    replyIdOf(message.getSubject) match
      case None     => ZIO.unit
      case Some(id) =>
        pending
          .modify(map => (map.get(id), map - id))
          .flatMap:
            case Some(entry) => entry.promise.succeed(message.getData).unit
            case None        => ZIO.unit

  /**
   * Extract the reply UUID from a reply subject (`prefix.<uuid>`).
   *
   * @param subject the received message's subject
   * @return the reply UUID, or `None` if the subject isn't a well-formed reply for this mailbox
   */
  private def replyIdOf(subject: String): Option[UUID] =
    Option(subject)
      .filter(_.startsWith(s"$prefix."))
      .flatMap(s => Try(UUID.fromString(s.substring(prefix.length + 1))).toOption)


object SharedInboxNatsMailbox:

  /** Sweep expired expectations once every this many `expect`s (the abandoned-entry backstop). */
  private val defaultSweepEvery = 64

  /**
   * Build a shared-inbox NATS mailbox over `connection`: a fresh per-node reply namespace, one wildcard
   * subscription (via [[CoreSubscriber]], awaited live), and a forked reply loop — all torn down with the
   * scope.
   *
   * @param connection the live connection
   * @return the mailbox; aborts with [[NatsError.Connect]] if the reply subscription can't be established
   */
  def make(connection: Connection): ZIO[Scope, NatsError, Mailbox[NatsError]] =
    for
      nodeId     <- Random.nextUUID
      prefix      = s"_MAILBOX.$nodeId"
      pending    <- Ref.make(Map.empty[UUID, Pending[NatsError]])
      counter    <- Ref.make(0)
      mailbox     = new SharedInboxNatsMailbox(connection, prefix, pending, counter, defaultSweepEvery)
      queue      <- Queue.unbounded[Message]
      subscriber <- CoreSubscriber.make(connection)
      scope      <- ZIO.scope
      _          <- subscriber.subscribe(s"$prefix.*", queue, scope)           // one wildcard SUB, awaited live
      _          <- queue.take.flatMap(mailbox.dispatch).forever.forkIn(scope) // the reply Processor
    yield mailbox
