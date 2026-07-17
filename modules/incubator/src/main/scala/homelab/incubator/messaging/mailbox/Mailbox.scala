package homelab.incubator.messaging.mailbox

import zio.*


/**
 * Point-to-point, node-addressable messaging — the request-reply half of the toolkit's messaging story
 * (companion to the Pub/Sub `Producer`/`Consumer`). A caller `expect`s a reply of type `B` and gets back a
 * [[Mailbox.Receipt]] whose serializable [[Address]] it hands out (embedded in a request); a holder of that
 * address `deliver`s the reply, completing the receipt's local `await`.
 *
 * `expect` allocates only in-process book-keeping — no substrate artifact by default. Delivery to an
 * unknown/departed address is dropped, and if no reply arrives within the expectation's timeout the mailbox
 * reaps the expectation and `await` yields `None` — so a lost reply leaks nothing.
 *
 * The reply address is a substrate-neutral [[Address]] token, not a broker-specific type — so a request DTO
 * that carries a `replyTo: Address` stays unchanged across substrates (the adapter serializes its internal
 * address into the token, and back on `deliver`).
 *
 * @tparam E the error a mailbox operation aborts with
 */
trait Mailbox[+E]:

  /**
   * Register an expectation for a reply of type `B`, returning a receipt over a fresh address. If no reply is
   * delivered within `timeout`, the mailbox reaps the expectation and the receipt's `await` yields `None`.
   *
   * @param timeout how long the receipt's `await` waits before giving up (and reaping the expectation)
   * @tparam B the expected reply type
   * @return the receipt — its `address` to hand out, its `await` to block on; aborts with `E` if the
   *         underlying inbox can't be established
   */
  def expect[B: Serde](timeout: Duration): IO[E, Mailbox.Receipt[E, B]]

  /**
   * Deliver `message` to a pending expectation at `address`. An unknown address is a no-op — the recipient is
   * gone (or already timed out), and the awaiter sees `None`.
   *
   * @param address the recipient address, taken from a request
   * @param message the value to deliver
   * @tparam B the delivered type (must match what the awaiter expected)
   * @return unit once delivered (or dropped); aborts with `E` on an encode / transport failure
   */
  def deliver[B: Serde](address: Address, message: B): IO[E, Unit]


object Mailbox:

  /**
   * The handle returned by [[Mailbox.expect]]: a serializable [[Address]] that travels on the wire, and a
   * local `await` (a promise) that the matching [[Mailbox.deliver]] completes. This asymmetry — address out,
   * promise local — is what makes the distributed case work.
   *
   * @tparam E the error `await` aborts with
   * @tparam B the awaited reply type
   */
  trait Receipt[+E, +B]:

    /** The serializable address to embed in an outgoing request. */
    def address: Address

    /**
     * Block (as a fiber) until the reply is delivered or the receipt's timeout elapses.
     *
     * @return `Some(reply)` if it arrived in time, or `None` if the timeout elapsed first (the expectation is
     *         then reaped); aborts with `E` if a delivered payload can't be decoded
     */
    def await: IO[E, Option[B]]
