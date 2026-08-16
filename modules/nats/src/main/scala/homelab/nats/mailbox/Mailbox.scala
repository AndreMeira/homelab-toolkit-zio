package homelab.nats.mailbox


import homelab.common.messaging.Producer as ProducerContract
import homelab.common.processing.Mailbox as MailboxContract
import homelab.common.processing.Mailbox.{ Address, Incoming }
import homelab.nats.NatsError
import homelab.nats.core.{ Consumer as CoreConsumer, Producer as CoreProducer }
import homelab.nats.mailbox.MessageCodec.given
import io.nats.client.{ Connection, NUID }
import zio.*


/**
 * A [[MailboxContract]] over Core NATS.
 *
 * The whole adapter is a codec pair plus a [[Location]]: a NATS subject already *is* an address, and Core
 * NATS delivery to a subject already is point-to-point to whoever subscribed. Nothing here re-implements
 * request-reply — an expectation is resolved by publishing to the droppoff subject it was minted at, which
 * is an ordinary publish.
 *
 * '''One subscription, many droppoffs.''' Every address lives under `prefix`, and the inbox subscribes once
 * to `prefix.>`. That is what discharges the [[MailboxContract.Location.droppoff]] contract — a minted
 * address routes back to this `Incoming` because the wildcard already covers it — and it avoids a
 * subscribe-per-request race, since we were listening before the address existed. It is the same muxed-inbox
 * trick the NATS client uses internally for `request`.
 *
 * '''Uniqueness''' comes from [[NUID]], the client's own globally-unique id generator: process-restart-safe,
 * so a late reply to a dead expectation can never resolve a live one.
 *
 * '''Requests''' — anything arriving under `prefix.>` that no expectation claims — leave through the inbox's
 * `forward`. [[Location.get]] is the address to publish for peers to send those to.
 *
 * The wire mapping lives in [[MessageCodec]], deliberately outside this object — see the note there.
 */
object Mailbox {

  /**
   * The subject root everything under this mailbox lives beneath, when none is given.
   *
   * Deliberately unlovely. The inbox subscribes to `prefix.>`, so *everything* published beneath the root is
   * treated as mailbox traffic — a reply to claim, or an unclaimed message to forward. A prefix an
   * application might plausibly use for its own subjects (`mailbox.orders`) would quietly capture that
   * traffic. Underscored names are legal NATS tokens and are the convention for client-internal subjects,
   * `_INBOX` being the canonical one.
   *
   * Not `_INBOX` itself: the NATS client publishes its own `request()` replies to `_INBOX.<nuid>`, so a
   * mailbox rooted there would intercept them (and be intercepted). A secured deployment needs this root in
   * the connection's publish/subscribe permissions, same as `_INBOX.>`.
   */
  val defaultPrefix: String = "__MAILBOX__"

  /**
   * Names addresses under one subject root.
   *
   * @param prefix the subject root; the inbox subscribes to `prefix.>`
   */
  final class Location(prefix: String) extends MailboxContract.Location[NatsError]:

    /**
     * The address peers publish requests to — matched by the same wildcard the inbox listens on, so a
     * request arrives here and, claimed by no expectation, leaves through `forward`.
     *
     * @return this mailbox's public address
     */
    override def get: IO[NatsError, Address] = ZIO.succeed(Address(s"$prefix.inbox"))

    /**
     * A fresh reply subject under the same root. Unique via [[NUID]] and covered by the inbox's wildcard,
     * which is both obligations [[MailboxContract.Location.droppoff]] states.
     *
     * @tparam A the type expected back, unused here — NATS subjects carry no type
     * @return a subject no expectation has used before
     */
    override def droppoff[A]: IO[NatsError, Address] =
      ZIO.succeed(Address(s"$prefix.${NUID.nextGlobal()}"))

  /**
   * The sending half: publishes each message to the subject its address names.
   *
   * @param connection the live connection
   */
  final class Outgoing(connection: Connection) extends MailboxContract.Outgoing[NatsError]:

    /**
     * The underlying transport.
     *
     * @return a Core NATS producer of mailbox messages
     */
    override val producer: ProducerContract[NatsError, MailboxContract.Message] =
      CoreProducer.make[MailboxContract.Message](connection)

  /**
   * Both halves of a mailbox over one connection, sharing a subject root.
   *
   * @param outgoing where to send
   * @param incoming where replies land — hand this to a `Graph`, it is a `Processor`
   * @param location the naming this mailbox uses, exposed for the Directory case
   */
  final case class Endpoints(
    outgoing: MailboxContract.Outgoing[NatsError],
    incoming: Incoming[NatsError],
    location: MailboxContract.Location[NatsError],
  )

  /**
   * Build a mailbox over `connection`.
   *
   * '''Start the inbox before minting anything.''' The returned `incoming` is a
   * [[homelab.common.processing.Processor]], and its subscription is established on the first `consume` —
   * Core NATS subscribes lazily, having no server-side backpressure. Core delivery is fire-and-forget, so a
   * reply published to a droppoff before that first `consume` is dropped by the broker rather than buffered.
   * A `Graph` must therefore be running this `Incoming` before an expectation is minted; this is the same
   * hazard the Core tests in `NatsSpec` work around by forking the consumer first.
   *
   * @param connection the live connection
   * @param prefix     the subject root for this mailbox's addresses
   * @param forward    where messages no expectation claims go — inbound requests, late replies
   * @return both halves, scoped to the subscription; aborts with [[NatsError.Connect]] if the dispatcher
   *         cannot be created
   */
  def make(
    connection: Connection,
    prefix: String = defaultPrefix,
    forward: Option[ProducerContract[NatsError, MailboxContract.Message]] = None,
  ): ZIO[Scope, NatsError, Endpoints] =
    val location = new Location(prefix)
    for
      consumer <- CoreConsumer.make[MailboxContract.Message](connection, s"$prefix.>")
      incoming <- Incoming.make[NatsError](location, consumer, forward)
    yield Endpoints(new Outgoing(connection), incoming, location)
}
