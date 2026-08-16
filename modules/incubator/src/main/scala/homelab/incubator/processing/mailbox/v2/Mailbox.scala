package homelab.incubator.processing.mailbox.v2


import homelab.common.error.ApplicationError
import homelab.common.messaging.{ Consumer, Producer }
import homelab.common.processing.Processor
import zio.*

import java.time.Instant


object Mailbox {
  opaque type Address <: String = String
  def Address(value: String): Address = value

  case class Message(address: Address, payload: Array[Byte])

  trait Location[E]:
    def get: IO[E, Address]
    def droppoff[A]: IO[E, Address]

  trait Outgoing[E <: ApplicationError]:
    def producer: Producer[E, Message]
    def send[A: Encoder](address: Address, message: A): IO[E, Unit] =
      producer.emit(Message(address, Encoder[A].encode(message)))

  trait Inbox[E]:
    private type Error = E | ApplicationError.DecodingError
    def expect[B: Decoder](timeout: Duration): IO[E, Receipt[Error, B]]

  trait Incoming[E <: ApplicationError] extends Inbox[E] with Processor[E, Message]:
    self =>

    private type Error = E | ApplicationError.DecodingError

    def location: Location[E]
    // A `Ref[Map]`, not a `KeyValueStore`: these hold `Promise`s — local, unserialisable, meaningful only in
    // this process — so there is no other backend a store could stand in for. It also gives `cleanup`
    // something to iterate, which the store port cannot offer.
    def pending: Ref[Map[Address, Incoming.Pending[E]]]

    // Where a message goes when no expectation claims it: a reply whose caller has already timed out or
    // left, or something that was never a reply — an inbound request. Dropped by default, so a process that
    // only asks needs no wiring; give it a destination to grow a request side without this trait knowing.
    def forward: Producer[E, Message] = Incoming.dropped

    /**
     * This inbox again, handing what it cannot match to `producer` instead of dropping it.
     *
     * The copy shares this inbox's `pending` table and `input`, so it is a replacement rather than an
     * addition: give a [[Graph]] the copy, not both, or two loops will drain the same intake.
     *
     * @param producer where a message goes when no expectation claims it
     * @return an inbox equivalent to this one, forwarding instead of dropping
     */
    def forwardTo(producer: Producer[E, Message]): Incoming[E] =
      new Incoming[E]:
        override def input: Consumer[E, Message]                     = self.input
        override def location: Location[E]                           = self.location
        override def pending: Ref[Map[Address, Incoming.Pending[E]]] = self.pending
        override def forward: Producer[E, Message]                   = producer

    def expect[B: Decoder](timeout: Duration): IO[E, Receipt[Error, B]] =
      for {
        now     <- Clock.instant
        promise <- Promise.make[E, Message]
        addr    <- location.droppoff[B]
        _       <- pending.update(_.updated(addr, Incoming.Pending(promise, now.plus(timeout))))
        gc      <- ZIO.random.flatMap(_.nextIntBetween(0, 100))
        _       <- cleanup(now).when(gc < 10) // 10% chance to sweep the store on each expectation
        rct     <- receipt(addr, promise, timeout)
      } yield rct

    override def process(value: Message): IO[E, Unit] =
      // Take the expectation out in the same step that finds it, so a second delivery to the same address
      // cannot complete a promise the first one already claimed.
      pending.modify(waiting => waiting.get(value.address) -> (waiting - value.address)).flatMap {
        case None          => forward.emit(value)
        case Some(waiting) => waiting.promise.succeed(value).unit
      }

    // Drop expectations nobody can still be waiting on: their caller's own timeout has fired, or it was
    // interrupted and left. A janitor only — the caller's `.timeout` is what decides an expectation has failed.
    private def cleanup(now: Instant): UIO[Unit] =
      pending.update(_.filterNot((_, waiting) => waiting.deadline.isBefore(now)))

    private def receipt[B: Decoder](
      addr: Address,
      promise: Promise[E, Message],
      timeout: Duration,
    ): UIO[Receipt[Error, B]] = Receipt.make(addr) {
      for {
        _      <- pending.update(_ - addr)
        maybe  <- promise.await.timeout(timeout)
        result <- maybe match
                    case None      => ZIO.succeed(None)
                    case Some(msg) => ZIO.fromEither(Decoder[B].decode(msg.payload)).map(Some(_))
      } yield result
    }

  object Incoming:
    case class Pending[E](promise: Promise[E, Message], deadline: Instant)

    /** Where unclaimed messages go unless an inbox is given somewhere to send them. */
    private val dropped: Producer[Nothing, Message] = Producer.noop.contramap(_ => ())

    /**
     * An inbox over a consumer, with a fresh expectation table of its own.
     *
     * @param location mints this inbox's own address and the droppoff addresses it expects replies at
     * @param consumer the intake this inbox drains
     * @param forward where a message goes when no expectation claims it; dropped when absent
     * @tparam E the error the underlying transport aborts with
     * @return an inbox ready to be run by a [[Graph]]; never fails
     */
    def make[E <: ApplicationError](
      location: Location[E],
      consumer: Consumer[E, Message],
      forward: Option[Producer[E, Message]] = None,
    ): UIO[Incoming[E]] =
      val (where, onward) = (location, forward.getOrElse(dropped))
      Ref
        .make(Map.empty[Address, Pending[E]])
        .map: table =>
          new Incoming[E]:
            override def location: Location[E]                           = where
            override def input: Consumer[E, Message]                     = consumer
            override def pending: Ref[Map[Address, Incoming.Pending[E]]] = table
            override def forward: Producer[E, Message]                   = onward

  trait Receipt[+E, +B]:
    def address: Address
    def await: IO[E, Option[B]]

  object Receipt:
    def make[E, B](addr: Address)(effect: IO[E, Option[B]]): UIO[Receipt[E, B]] =
      Ref.Synchronized.make[(Boolean, Option[B])](false -> None).map { ref =>
        new Receipt[E, B] {
          override def address: Address        = addr
          override def await: IO[E, Option[B]] = ref.modifyZIO:
            case true -> result => ZIO.succeed(result -> (true, result))
            case false -> _     => effect.map(result => result -> (true, result))
        }
      }

  trait Encoder[A]:
    def encode(value: A): Array[Byte]

  object Encoder:
    def apply[A: Encoder]: Encoder[A] = summon

  trait Decoder[A]:
    def decode(value: Array[Byte]): Either[ApplicationError.DecodingError, A]

  object Decoder:
    def apply[A: Decoder]: Decoder[A] = summon

}
