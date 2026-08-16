package homelab.incubator.messaging.mailbox


import homelab.common.error.ApplicationError
import homelab.common.messaging.{ Consumer, Producer }
import homelab.common.processing.{ Processor, Worker }
import zio.*


/**
 * Sketch: putting a [[homelab.common.processing.Worker]] behind a mailbox, to check the claim that a remote
 * request/reply edge needs nothing new in `processing`.
 *
 * The shape it argues for: **a bridge is a [[Processor]], not a pipe.** A worker's mailbox is a pipe of
 * `(message, Promise)` — one channel, whose write side is "give this worker work" and whose read side is the
 * worker's own intake. A transport edge is two channels going opposite ways: requests arrive from the network,
 * replies leave to an address. Forcing that into one `Pipe` would leave `emit` meaning something unrelated to
 * `consume`, and would make the pipe depend on its consumer settling the promise before returning — a
 * contract pipes do not otherwise impose.
 *
 * As a processor it composes with everything already there: a [[homelab.common.processing.Graph]] starts it,
 * a `Node` can own it as a child of the worker it fronts, and `Processor.Parallel`'s limit *is* the bound on
 * concurrent in-flight remote requests. The worker itself is untouched and stays purely local.
 */
object MailboxBridge {

  /**
   * A request as it arrives from a transport: the payload, and where its reply goes.
   *
   * @param payload the message for the worker
   * @param replyTo the address the caller is waiting on
   * @tparam A the message type
   */
  final case class Request[A](payload: A, replyTo: Address)

  /**
   * A reply as it travels back: the answer, or a rendering of the failure.
   *
   * `Either[String, B]` is the sketch's placeholder. Locally a failed step fails one caller's `Promise` with
   * an `ApplicationError` *value*; that value has to cross the wire, so the real contract is a schema question
   * — how much of the error survives — rather than a processing one.
   *
   * @tparam B the reply type
   */
  type Reply[B] = Either[String, B]

  /**
   * Inbound: consume requests from a transport, ask the local worker, deliver the outcome to the caller's
   * address.
   *
   * `ask` is what makes this simple — the worker settles every message's promise on every path, so the
   * outcome is always something to deliver, and a failure fails only this request. Nothing here parks
   * indefinitely by accident: the caller's timeout lives on their receipt, and this side's concurrency is
   * capped by `parallelism`.
   *
   * @param input       requests arriving from the transport
   * @param mailbox     the mailbox used to deliver replies
   * @param target      the local worker doing the work
   * @param parallelism how many remote requests may be in flight at once
   * @tparam E the error the worker and the mailbox share
   * @tparam A the message accepted
   * @tparam B the reply produced
   */
  final class Inbound[E <: ApplicationError, A, B](
    override val input: Consumer[E, Request[A]],
    mailbox: Mailbox[E],
    target: Worker[E, A, B],
    override val parallelism: Int,
  )(using Serde[Reply[B]])
      extends Processor.Parallel[E, Request[A]] {

    /**
     * Handle one remote request.
     *
     * @param request the request and the address its reply belongs to
     * @return noop once the reply has been delivered; aborts with `E` only if *delivering* fails — the
     *         worker's own failure is part of the reply, not of this step
     */
    override def process(request: Request[A]): IO[E, Unit] =
      target
        .ask(request.payload)
        .either
        .flatMap(outcome => mailbox.deliver(request.replyTo, outcome.left.map(_.message)))
  }

  /**
   * Outbound: the caller's side, for symmetry — mint a receipt, send the request carrying its address, wait.
   *
   * Not a processor: this is an ordinary effect a caller runs, which is the point. The asymmetry with
   * [[Inbound]] is the honest one — receiving requests is a loop something must run, sending one is not.
   *
   * @param mailbox   the mailbox minting the receipt
   * @param requests  where requests are published
   * @param payload   the message to send
   * @param timeout   how long to wait before giving up
   * @tparam E the error the mailbox and transport share
   * @tparam A the message sent
   * @tparam B the reply expected
   * @return the reply, `None` if the timeout elapsed first, or a `Left` carrying the remote failure
   */
  def ask[E <: ApplicationError, A, B](
    mailbox: Mailbox[E],
    requests: Producer[E, Request[A]],
    payload: A,
    timeout: Duration,
  )(using Serde[Reply[B]]): IO[E, Option[Reply[B]]] =
    for
      receipt <- mailbox.expect[Reply[B]](timeout)
      _       <- requests.emit(Request(payload, receipt.address))
      reply   <- receipt.await
    yield reply
}
