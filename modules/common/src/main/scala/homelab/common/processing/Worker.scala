package homelab.common.processing


import homelab.common.messaging.Pipe
import zio.{ IO, Promise }


/**
 * A [[Sink]] that owns its intake: its [[input]] is a [[Pipe]] rather than a bare `Consumer`, so the node
 * both drains the channel and holds its producing end — while processing it can `send` follow-up values
 * back onto its own queue. Like any [[Sink]] it emits nothing downstream (its output is discarded); the
 * only thing it produces is more of its own input.
 *
 * `A` is invariant because [[Pipe]] is invariant in its value type.
 *
 * @tparam E the error processing aborts with
 * @tparam A the value consumed, and re-emittable through [[input]]
 */
trait Worker[E, A] extends Sink[E, A]:

  /**
   * The owned intake: a [[Pipe]] the node consumes from and can also `send` back onto.
   *
   * @return the input pipe
   */
  override def input: Pipe[E, A]

  /**
   * Enqueue `message` onto this worker's own intake — an alias for `input.send`. Lets a caller feed the
   * worker's mailbox; the value is later drained and processed by the run loop.
   *
   * @param message the value to enqueue onto [[input]]
   * @return noop once enqueued; aborts with `E` on failure
   */
  def send(message: A): IO[E, Unit] = input.send(message)

  /**
   * The **ask** pattern (Akka-style request/reply over the mailbox): make a fresh reply [[Promise]], send a
   * message that carries it, then await it. `factory` builds the message from the promise — e.g. a
   * `Query(arg, replyTo)` case — so the [[run]] loop, when it processes that message, completes `replyTo`
   * with the answer (or an error), which this call returns.
   *
   * '''Completing the reply is the processing logic's contract, and there is no built-in timeout''' (unlike
   * Akka's `ask`): the message's `run`-side handling '''must complete the promise on every path — success
   * and failure alike'''. If processing fails without failing the promise, drops the message, forgets to
   * reply, or the run loop isn't running, `ask` blocks forever. Compose a timeout at the call site
   * (`worker.ask(…).timeoutFail(…)(d)`) where you need a liveness guarantee. Interrupting the caller cancels
   * the await; the message may still be processed, harmlessly completing a promise no one awaits.
   *
   * @param factory builds the message to send from the reply promise it must complete
   * @tparam A2 the reply value type
   * @return the reply once the promise is completed; aborts with `E` if `send` fails or the reply is failed
   */
  def ask[A2](factory: Promise[E, A2] => A): IO[E, A2] =
    for
      promise <- Promise.make[E, A2]
      _       <- send(factory(promise))
      reply   <- promise.await
    yield reply


object Worker {

  /**
   * A [[Worker]] that processes one value at a time — a [[Sink.PerItem]] whose intake is a [[Pipe]].
   *
   * @tparam E the error processing aborts with
   * @tparam A the value consumed
   */
  trait PerItem[E, A] extends Sink.PerItem[E, A] with Worker[E, A]

  /**
   * A [[Worker]] that processes up to `parallelism` values concurrently — a [[Sink.Parallel]] whose intake
   * is a [[Pipe]].
   *
   * @tparam E the error processing aborts with
   * @tparam A the value consumed
   */
  trait Parallel[E, A] extends Sink.Parallel[E, A] with Worker[E, A]

  /**
   * A [[Worker]] that consumes a batch at a time — a [[Sink.Batched]] whose intake is a [[Pipe.Batched]].
   *
   * @tparam E the error processing aborts with
   * @tparam A the element consumed
   */
  trait Batched[E, A] extends Sink.Batched[E, A]:

    /**
     * The owned batched intake: a [[Pipe.Batched]] the node consumes batches from and can `send` back onto.
     *
     * @return the input pipe
     */
    override def input: Pipe.Batched[E, A]

  object Batched {

    /**
     * A [[Worker]] that processes up to `parallelism` batches concurrently — a [[Sink.Batched.Parallel]]
     * whose intake is a [[Pipe.Batched]].
     *
     * @tparam E the error processing aborts with
     * @tparam A the element consumed
     */
    trait Parallel[E, A] extends Sink.Batched.Parallel[E, A]:

      /**
       * The owned batched intake: a [[Pipe.Batched]] the node consumes batches from and can `send` back
       * onto.
       *
       * @return the input pipe
       */
      override def input: Pipe.Batched[E, A]
  }
}
