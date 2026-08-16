package homelab.common.processing


import homelab.common.error.ApplicationError
import homelab.common.messaging.{ Consumer, Pipe }
import zio.{ IO, Promise, ZIO }


/**
 * A [[Processor]] that answers: each message carries the promise its reply is routed to, so a caller can
 * [[ask]] and wait, or [[send]] and not. Request/reply on top of an ordinary pipe — the queue is the mailbox,
 * and the [[Graph]] is what starts it.
 *
 * A failing `receive` fails *its own caller* and nothing else: the error goes to that message's promise and
 * the loop carries on. Only a defect stops the worker, which is the usual bargain — a defect is a bug, not an
 * outcome.
 *
 * @tparam E the error a reply may fail with
 * @tparam A the message accepted
 * @tparam B the reply produced
 */
trait Worker[E <: ApplicationError, A, B] extends Processor[E, (A, Promise[E, B])] {
  private type Payload = (A, Promise[E, B])

  /** The pipe this worker both consumes from and is sent to — its mailbox. */
  override def input: Pipe[E, Payload]

  /**
   * What this worker does with a message.
   *
   * @return the handler; aborts with `E` when handling fails, which fails that message's caller alone
   */
  def receive: A => IO[E, B]

  /**
   * Queue `message` without waiting for its reply. The reply, and any failure, are discarded — a promise is
   * still created because it is part of what a worker consumes, but nobody reads it.
   *
   * @param message the message to queue
   * @return noop once queued, not once handled
   */
  def send(message: A): IO[E, Unit] =
    Promise
      .make[E, B]
      .flatMap: promise =>
        input.emit((message, promise))

  /**
   * Queue `message` and wait for its reply.
   *
   * Waits indefinitely: nothing here times out, and a worker that was never started — never registered with a
   * [[Graph]], or registered after it ran — leaves its callers waiting forever rather than failing them.
   *
   * @param message the message to queue
   * @return the reply; aborts with `E` if handling it fails
   */
  def ask(message: A): IO[E, B] =
    for
      promise <- Promise.make[E, B]
      _       <- input.emit((message, promise))
      result  <- promise.await
    yield result

  /**
   * Handle one message and settle its promise with whatever happened — success, failure, defect or
   * interruption alike, from an `onExit` so that even a worker being torn down releases the caller waiting on
   * it rather than leaving it parked forever.
   *
   * The typed failure is then swallowed, because it has already reached the one caller it concerns and must
   * not stop the loop. A defect is left to propagate.
   *
   * `final` because this *is* the reply protocol: an override that forgot to settle the promise would leave
   * callers waiting for the life of their fiber, with nothing failing to say so. Per-message behaviour —
   * logging, metrics, retries — belongs in [[receive]], which is the part a worker is meant to supply.
   *
   * @param value the message and the promise its outcome belongs to
   * @return noop once the promise is settled
   */
  final override def process(value: (A, Promise[E, B])): IO[E, Unit] =
    val (message, promise) = value
    receive(message).onExit(exit => promise.done(exit)).catchAll(_ => ZIO.unit).unit

}


object Worker {

  /**
   * A [[Worker]] that handles messages concurrently, up to `parallelism` at a time.
   *
   * The trade against the serial [[Worker]] is ordering: messages are still *taken* from the pipe in order,
   * but they finish in whatever order their handlers do, and a slow message no longer holds up the ones
   * behind it. Replies stay correct regardless, since each message carries its own promise — there is no
   * shared reply channel to interleave.
   *
   * Choose this when handling is I/O-bound and independent; keep the serial one when messages share state or
   * their order matters.
   *
   * @tparam E the error a reply may fail with
   * @tparam A the message accepted
   * @tparam B the reply produced
   */
  trait Parallel[E <: ApplicationError, A, B] extends Worker[E, A, B], Processor.Parallel[E, (A, Promise[E, B])]
}
