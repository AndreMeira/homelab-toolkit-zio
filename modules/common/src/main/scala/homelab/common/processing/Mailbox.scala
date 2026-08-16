package homelab.common.processing


import homelab.common.data.Codec.{ Decoder, Encoder }
import homelab.common.error.ApplicationError
import homelab.common.messaging.{ Consumer, Producer }
import zio.*

import java.time.Instant


/**
 * A serialisable promise.
 *
 * An [[Mailbox.Inbox]] mints an expectation and hands back a [[Mailbox.Receipt]] carrying an
 * [[Mailbox.Address]]. That address is a *value*: attach it to a message going anywhere, to as many parties
 * as the protocol calls for, or pass it along a chain — whoever ends up holding it resolves the expectation
 * by sending a message to it, and the holder of the receipt gets that message back, decoded. It is the
 * `self` of an actor system, made small enough to put in a payload.
 *
 * Two consequences follow, and they are what make this cheap:
 *
 *   - '''Correlation is routing.''' The reply's destination *is* the correlation, so there is no correlation
 *     header to agree on, no matching table on the responder's side, and no envelope field for a reply-to —
 *     [[Mailbox.Message.address]] on the reply leg is already the droppoff. A responder needs to know how to
 *     send, and nothing else.
 *   - '''This is not a bidirectional channel.''' Nothing here asks anybody anything; there is no `ask`,
 *     because the message that carries an address need not go to the party that resolves it. A request side
 *     — inbound calls this process should answer — is a separate concern, reachable through
 *     [[Mailbox.Incoming.forward]].
 *
 * An expectation resolves once. The first message to reach its address completes it; anything later finds
 * nothing and is forwarded, as is anything that was never a reply at all.
 */
object Mailbox {

  /**
   * Where a message can be sent. Opaque over `String` and constructed without validation, so an adapter can
   * lift a subject, topic, or queue name it already trusts; whether a given string means anything is the
   * transport's business. See [[Location.droppoff]] for the one property the mailbox does rely on.
   */
  opaque type Address <: String = String

  /**
   * Lift a transport-supplied string into an [[Address]]. No validation: the caller is the transport.
   *
   * @param value the transport's name for a destination
   * @return that name as an address
   */
  def Address(value: String): Address = value

  /**
   * The envelope: a payload and where it is going. On a reply leg the address is the droppoff the sender was
   * handed, which is why nothing here names a sender — see the note on correlation in [[Mailbox]].
   *
   * @param address where this message is bound for
   * @param payload the encoded body
   */
  case class Message(address: Address, payload: Array[Byte])

  /**
   * The naming half of a transport: where this process can be reached, and how to mint somewhere for a
   * single reply to land.
   *
   * @tparam E the error naming aborts with
   */
  trait Location[E]:

    /**
     * This process's own address — the one to publish so peers can send to it, and the root a directory
     * hands out.
     *
     * @return this inbox's address; aborts with `E` if the transport cannot say
     */
    def get: IO[E, Address]

    /**
     * Mint an address for one expectation.
     *
     * '''The implementor owes two things.''' Every address minted must route back to the [[Incoming]] that
     * asked for it, or replies arrive nowhere and every expectation times out. And an address must never
     * repeat for the life of the process: a reused address lets a late reply to a dead expectation resolve a
     * live one, silently and with the wrong value.
     *
     * `A` is phantom here on purpose. Nothing in this file reads it, but it is what an implementor needs to
     * derive a per-type channel — a typed subject, a schema-tagged queue — through a macro or a type-level
     * lookup, and it costs nothing to leave open.
     *
     * @tparam A the type expected back at this address
     * @return a fresh address routing to this process; aborts with `E` if the transport cannot mint one
     */
    def droppoff[A]: IO[E, Address]

  /**
   * The sending half of a mailbox: somewhere to put a message, and the encoding to put it there with.
   *
   * @tparam E the error sending aborts with
   */
  trait Outgoing[E <: ApplicationError]:

    /**
     * The transport this sends through.
     *
     * @return the underlying producer
     */
    def producer: Producer[E, Message]

    /**
     * Encode a value and send it to an address — someone's inbox, or a droppoff whose receipt is waiting.
     *
     * @param address where to send it
     * @param message the value to send
     * @tparam A the value sent, with an [[Encoder]] in scope
     * @return noop once handed to the transport; aborts with `E` if it refuses
     */
    def send[A: Encoder](address: Address, message: A): IO[E, Unit] =
      producer.emit(Message(address, Encoder[A].encode(message)))

  /**
   * The receiving half of a mailbox, as its users see it: the ability to mint an expectation. Callers should
   * depend on this rather than on [[Incoming]], which additionally exposes the machinery that runs the
   * intake and could be used to inject replies.
   *
   * @tparam E the error minting aborts with
   */
  trait Inbox[E]:
    private type Error = E | ApplicationError.DecodingError

    /**
     * Mint an expectation, live for `timeout` from this moment.
     *
     * @param timeout how long from now the expectation stands
     * @tparam B the value expected back, with a [[Decoder]] in scope
     * @return a receipt over a fresh address; aborts with `E` if no address can be minted
     */
    def expect[B: Decoder](timeout: Duration): IO[E, Receipt[Error, B]]

  /**
   * The partial implementation of an [[Inbox]] for transports that expose a [[Consumer]]: it holds the
   * expectation table, resolves what arrives, and sweeps what expired, leaving an adapter to supply only
   * its [[Location]] and its intake. It is not itself an adapter, which is why it is not named after one.
   *
   * As a [[Processor]] it belongs to a [[Graph]], which drives its intake.
   *
   * @tparam E the error the underlying transport aborts with
   */
  trait Incoming[E <: ApplicationError] extends Inbox[E] with Processor[E, Message]:
    self =>

    private type Error = E | ApplicationError.DecodingError

    /**
     * The naming half of this inbox's transport.
     *
     * @return the location minting this inbox's addresses
     */
    def location: Location[E]

    /**
     * Expectations minted and not yet resolved, by the address each is waiting at.
     *
     * A `Ref[Map]`, not a `KeyValueStore`: these hold `Promise`s — local, unserialisable, meaningful only in
     * this process — so there is no other backend a store could stand in for. It also gives [[sweepInterval]]
     * something to iterate, which the store port cannot offer.
     *
     * @return the expectation table
     */
    def pending: Ref[Map[Address, Incoming.Pending[E]]]

    /**
     * When the table was last swept. Held apart from the table so a sweep costs one compare-and-set on the
     * quiet path, and so only one caller of many concurrent ones does the scan.
     *
     * @return the instant of the last sweep
     */
    def swept: Ref[Instant]

    /**
     * How long to let pass between sweeps. Bounds the janitor's cost per unit *time* rather than per call:
     * under load the table is scanned no more often than this, however many expectations are minted.
     *
     * @return the shortest interval between two sweeps
     */
    def sweepInterval: Duration = Incoming.defaultSweepInterval

    /**
     * Where a message goes when no expectation claims it: a reply whose holder has already given up or left,
     * a second reply to an address already resolved, or something that was never a reply at all — an inbound
     * request. Dropped by default, so a process that only expects needs no wiring; give it a destination to
     * grow a request side without this trait knowing anything about it.
     *
     * @return the sink for unclaimed messages
     */
    def forward: Producer[E, Message] = Incoming.dropped

    /**
     * This inbox again, handing what it cannot match to `producer` instead of dropping it.
     *
     * The copy shares this inbox's table and intake, so it is a replacement rather than an addition: give a
     * [[Graph]] the copy, not both, or two loops will drain the same intake. It also has no effect once a
     * graph is running this one — the copy is a different [[Processor]], and nobody is draining it.
     *
     * @param producer where a message goes when no expectation claims it
     * @return an inbox equivalent to this one, forwarding instead of dropping
     */
    def forwardTo(producer: Producer[E, Message]): Incoming[E] =
      new Incoming[E]:
        override def input: Consumer[E, Message]                     = self.input
        override def location: Location[E]                           = self.location
        override def pending: Ref[Map[Address, Incoming.Pending[E]]] = self.pending
        override def swept: Ref[Instant]                             = self.swept
        override def sweepInterval: Duration                         = self.sweepInterval
        override def forward: Producer[E, Message]                   = producer

    /**
     * Mint an expectation and register it, so that a reply arriving from this moment on can resolve it.
     * Registration happens before the address exists anywhere else, so no reply can outrun its expectation.
     *
     * @param timeout how long from now the expectation stands
     * @tparam B the value expected back, with a [[Decoder]] in scope
     * @return a receipt over a fresh address; aborts with `E` if no address can be minted
     */
    def expect[B: Decoder](timeout: Duration): IO[E, Receipt[Error, B]] =
      for {
        now     <- Clock.instant
        promise <- Promise.make[E, Message]
        addr    <- location.droppoff[B]
        // The deadline, not the duration, is the authority: the holder may await long after minting this,
        // and both it and the sweeper must agree on the one instant this expectation dies.
        deadline = now.plus(timeout)
        _       <- pending.update(_.updated(addr, Incoming.Pending(promise, deadline)))
        _       <- sweep(now)
        rct     <- receipt(addr, promise, deadline)
      } yield rct

    /**
     * Resolve the expectation waiting at this message's address, or hand the message to [[forward]].
     *
     * @param value the message that arrived
     * @return noop once resolved or forwarded; aborts with `E` if forwarding fails
     */
    override def process(value: Message): IO[E, Unit] =
      // Take the expectation out in the same step that finds it, so a second delivery to the same address
      // cannot complete a promise the first one already claimed.
      pending.modify(waiting => waiting.get(value.address) -> (waiting - value.address)).flatMap {
        case None          => forward.emit(value)
        case Some(waiting) => waiting.promise.succeed(value).unit
      }

    /**
     * Sweep, but no more often than [[sweepInterval]]. Claiming the slot with a `modify` means that of many
     * concurrent callers exactly one scans and the rest pay a compare-and-set.
     *
     * @param now the instant to judge the interval and the deadlines by
     * @return noop, whether or not a sweep was due
     */
    private def sweep(now: Instant): UIO[Unit] =
      swept
        .modify(last => if now.isAfter(last.plus(sweepInterval)) then (true, now) else (false, last))
        .flatMap(due => cleanup(now).when(due))
        .unit

    /**
     * Drop expectations past their deadline, whose holder has therefore either given up already or is about
     * to. A janitor: it never completes a promise, it forgets it.
     *
     * This is what reclaims the expectations no receipt will ever retire — one whose send failed after it was
     * minted, or that nobody awaited. A receipt retires its own entry however its wait ends.
     *
     * @param now the instant to judge deadlines by
     * @return noop once the table is swept
     */
    private def cleanup(now: Instant): UIO[Unit] =
      pending.update(_.filterNot((_, waiting) => waiting.deadline.isBefore(now)))

    /**
     * The receipt for a registered expectation.
     *
     * @param addr the address this expectation waits at
     * @param promise completed by [[process]] with the message that resolves it
     * @param deadline the instant the expectation dies, fixed when it was minted
     * @tparam B the value expected back, with a [[Decoder]] in scope
     * @return a receipt whose await yields the decoded value, or nothing if the deadline passes first
     */
    private def receipt[B: Decoder](
      addr: Address,
      promise: Promise[E, Message],
      deadline: Instant,
    ): UIO[Receipt[Error, B]] = Receipt.make(addr) {
      // The budget is measured from here to the deadline set at `expect`, so waiting cannot outlive the
      // entry the sweeper is entitled to reclaim. `ensuring` retires the entry however this ends —
      // delivered, expired, failed, or interrupted out of a race — leaving the sweeper only the
      // expectations nobody ever awaited.
      for {
        now    <- Clock.instant
        timeout = Duration.fromInterval(now, deadline)
        maybe  <- promise.await.timeout(timeout).ensuring(pending.update(_ - addr))
        result <- maybe match
                    case None      => ZIO.succeed(None)
                    case Some(msg) => ZIO.fromEither(Decoder[B].decode(msg.payload)).map(Some(_))
      } yield result
    }

  object Incoming:

    /**
     * A registered expectation: whom to hand the message to, and when it stops being anyone's.
     *
     * @param promise completed with the message that resolves this expectation
     * @param deadline the instant after which the sweeper may reclaim it
     * @tparam E the error the underlying transport aborts with
     */
    case class Pending[E](promise: Promise[E, Message], deadline: Instant)

    /** Where unclaimed messages go unless an inbox is given somewhere to send them. */
    private val dropped: Producer[Nothing, Message] = Producer.noop.contramap(_ => ())

    /** How long an inbox lets pass between sweeps of its table, unless told otherwise. */
    val defaultSweepInterval: Duration = 10.seconds

    /**
     * An inbox over a consumer, with an expectation table of its own.
     *
     * @param location mints this inbox's own address and the droppoff addresses it expects replies at
     * @param consumer the intake this inbox drains
     * @param forward where a message goes when no expectation claims it; dropped when absent
     * @param sweepInterval the shortest interval between two sweeps of the expectation table
     * @tparam E the error the underlying transport aborts with
     * @return an inbox ready to be run by a [[Graph]]; never fails
     */
    def make[E <: ApplicationError](
      location: Location[E],
      consumer: Consumer[E, Message],
      forward: Option[Producer[E, Message]] = None,
      sweepInterval: Duration = defaultSweepInterval,
    ): UIO[Incoming[E]] =
      val (where, onward, every) = (location, forward.getOrElse(dropped), sweepInterval)
      for
        table <- Ref.make(Map.empty[Address, Pending[E]])
        now   <- Clock.instant
        last  <- Ref.make(now)
      yield new Incoming[E]:
        override def location: Location[E]                           = where
        override def input: Consumer[E, Message]                     = consumer
        override def pending: Ref[Map[Address, Incoming.Pending[E]]] = table
        override def swept: Ref[Instant]                             = last
        override def sweepInterval: Duration                         = every
        override def forward: Producer[E, Message]                   = onward

  /**
   * A claim on one expectation: the address it waits at, and the wait itself.
   *
   * The address is the part to give away — put it in a message, hand it to whoever should resolve it. The
   * wait is the part to keep.
   *
   * @tparam E the error awaiting aborts with
   * @tparam B the value expected back
   */
  trait Receipt[+E, +B]:

    /**
     * Where this expectation is waiting. Attach it to whatever message should carry it.
     *
     * @return the expectation's address
     */
    def address: Address

    /**
     * Wait for the expectation to resolve, up to the deadline fixed when it was minted.
     *
     * Awaited more than once, it answers from the first outcome rather than waiting again — so a second
     * caller sees what the first saw, and a caller that already timed out is told so at once. The one
     * exception is a wait that was *interrupted*: it leaves no outcome to remember, and a later await runs
     * out the remaining budget against an expectation no longer in the table.
     *
     * @return the decoded value, or nothing if the deadline passed first; aborts with `E` if the transport
     *         failed or the reply could not be decoded
     */
    def await: IO[E, Option[B]]

  object Receipt:

    /**
     * A receipt whose wait runs `effect` at most once.
     *
     * @param addr the address the expectation waits at
     * @param effect the wait, run on the first await and remembered thereafter
     * @tparam E the error awaiting aborts with
     * @tparam B the value expected back
     * @return the receipt; never fails
     */
    def make[E, B](addr: Address)(effect: IO[E, Option[B]]): UIO[Receipt[E, B]] =
      Ref.Synchronized.make[(Boolean, Option[B])](false -> None).map { ref =>
        new Receipt[E, B] {
          override def address: Address        = addr
          override def await: IO[E, Option[B]] = ref.modifyZIO:
            case true -> result => ZIO.succeed(result -> (true, result))
            case false -> _     => effect.map(result => result -> (true, result))
        }
      }
}
