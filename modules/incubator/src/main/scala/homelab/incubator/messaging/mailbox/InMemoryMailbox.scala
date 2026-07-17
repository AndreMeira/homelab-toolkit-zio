package homelab.incubator.messaging.mailbox


import zio.*

import java.util.UUID
import java.util.concurrent.TimeUnit

import scala.util.Try


/**
 * In-memory [[Mailbox]] — the test/dev backend and design mirror. Outstanding expectations are a `Ref`-held
 * map from a UUID to a [[Pending]] (promise + deadline); the [[Address]] is the UUID's string form. `deliver`
 * parses the address back, then atomically removes and completes the matching promise (one-shot; a malformed
 * or unknown address is a harmless no-op).
 *
 * Timeouts need no fiber: `await` bounds on the entry's remaining time (`deadline - now`), and every
 * `sweepEvery`-th `expect` drops entries already past their deadline — so an abandoned expectation (expected
 * but never awaited) is reaped by later traffic rather than a background timer.
 *
 * Round-tripping through [[Serde]] mirrors a real wire — and lets a homogeneous `Array[Byte]` map stand in for
 * the prior art's `Map[_, Any]` + `Tag` runtime type-check: serialization removes the need for a type tag.
 *
 * @param pending      the UUID → expectation map
 * @param sweepCounter counts `expect` calls, to gate the opportunistic sweep
 * @param sweepEvery   sweep expired entries once every this many `expect`s
 */
final class InMemoryMailbox(
  pending: Ref[Map[UUID, Pending[MailboxError]]],
  sweepCounter: Ref[Int],
  sweepEvery: Int,
) extends Mailbox[MailboxError]:

  override def expect[B: Serde](timeout: Duration): UIO[Mailbox.Receipt[MailboxError, B]] =
    for
      now     <- Clock.currentTime(TimeUnit.MILLISECONDS)
      id      <- Random.nextUUID
      promise <- Promise.make[MailboxError, Array[Byte]]
      count   <- sweepCounter.updateAndGet(_ + 1)
      deadline = now + timeout.toMillis
      _       <- pending.update: map =>
                   val kept = if count % sweepEvery == 0 then map.filter { case (_, entry) => entry.deadline > now } else map
                   kept + (id -> Pending(promise, deadline))
    yield new Mailbox.Receipt[MailboxError, B]:
      def address: Address = Address(id.toString)

      def await: IO[MailboxError, Option[B]] =
        Clock
          .currentTime(TimeUnit.MILLISECONDS)
          .flatMap: awaitNow =>
            promise.await
              .timeout(Duration.fromMillis((deadline - awaitNow).max(0L)))
              .flatMap:
                case None        => pending.update(_ - id).as(None) // timed out — reap the expectation
                case Some(bytes) =>
                  Serde[B].decode(bytes) match
                    case Right(value) => ZIO.succeed(Some(value))
                    case Left(reason) => ZIO.fail(MailboxError.Decode(reason))

  override def deliver[B: Serde](address: Address, message: B): UIO[Unit] =
    Try(UUID.fromString(address)).toOption match
      case None     => ZIO.unit // malformed address — drop
      case Some(id) =>
        pending
          .modify(map => (map.get(id), map - id))
          .flatMap:
            case Some(entry) => entry.promise.succeed(Serde[B].encode(message)).unit
            case None        => ZIO.unit // unknown / departed / already reaped — drop


object InMemoryMailbox:

  /** Sweep expired expectations once every this many `expect`s (the abandoned-entry backstop). */
  private val defaultSweepEvery = 64

  /**
   * Build a fresh in-memory mailbox with no outstanding expectations.
   *
   * @return the mailbox, as the [[Mailbox]] port
   */
  def make: UIO[Mailbox[MailboxError]] =
    for
      pending <- Ref.make(Map.empty[UUID, Pending[MailboxError]])
      counter <- Ref.make(0)
    yield new InMemoryMailbox(pending, counter, defaultSweepEvery)
