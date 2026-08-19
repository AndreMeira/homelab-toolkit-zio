package homelab.incubator.processing.pool.v1


import zio.*

import scala.util.chaining.scalaUtilChainingOps


trait PollConsumer[E, A]:
  def pollSize: Int
  def maxInFlight: Int
  def running: Ref[Int]
  def signal: PollConsumer.Signal
  def source: PollConsumer.Source[E, A]

  /**
   * Poll once: claim what the in-flight allowance permits, run `logic` over it, and settle each element. One
   * call processes one batch — a run loop calls it repeatedly.
   *
   * With nothing to claim it waits for a signal and returns, so the caller's next call re-polls. Waiting
   * happens inside the scope only because an empty claim gives every reserved permit straight back: a parked
   * consumer holds nothing, and its siblings see the whole allowance.
   *
   * @param logic processes one claimed element
   * @tparam E2 the widened error, admitting `logic`'s failures
   * @return noop once the batch is settled; aborts with the combined causes if any element failed
   */
  def consume[E2 >: E](logic: A => IO[E2, Unit]): IO[E2, Unit] = ZIO.scoped {
    fetch.flatMap:
      case Nil   => signal.take
      case elems => wakeUp.when(elems.size >= pollSize) *> handleBatch(elems, logic)
  }

  def wakeUp: UIO[Unit] = signal.offer(()).unit

  private def handleBatch[E2 >: E](elements: List[A], logic: A => IO[E2, Unit]): IO[E2, Unit] =
    ZIO.foreachPar(elements)(handle(_, logic).exit).flatMap { exits =>
      exits.collect { case Exit.Failure(cause) => cause }.pipe {
        case Nil          => ZIO.unit
        case head :: rest => ZIO.failCause(rest.fold(head)(_ && _))
      }
    }

  private def handle[E2 >: E](element: A, logic: A => IO[E2, Unit]): IO[E2, Unit] =
    ZIO.uninterruptibleMask: restore =>
      restore(logic(element)).exit.flatMap {
        case Exit.Success(_) => source.ack(element)
        case Exit.Failure(_) => source.nack(element, 10.second)
      }

  /**
   * Reserve, claim, and hand back the difference.
   *
   * The reservation comes first because the source hands out *leases*: claiming work we have no capacity to
   * run would leave it held and idle. What the source did not give us is returned immediately, so `running`
   * reflects the batch actually in hand rather than the allowance asked for — and the rest is released when
   * the scope closes.
   *
   * Two separate acquisitions rather than one: the reservation is registered before the claim is attempted,
   * so a failing `tryAcquire` still releases it. Folding them together would strand the reservation whenever
   * the source errored.
   *
   * @return the claimed elements, with their permits held for the life of the scope
   */
  private def fetch: ZIO[Scope, E, List[A]] =
    for {
      granted <- ZIO.acquireRelease(running.modify { held =>
                   val allowed = available(held)
                   allowed -> (held + allowed)
                 })(reserved => running.update(_ - reserved))
      fetched <- source.tryAcquire(upTo = granted)
      unused   = math.max(granted - fetched.size, 0)
      _       <- ZIO.acquireRelease(running.update(_ - unused))(_ => running.update(_ + unused))
    } yield fetched

  private def available(running: Int): Int =
    math.min(pollSize, math.max(maxInFlight - running, 0))


object PollConsumer:

  /**
   * The consumer's wake-up: a queue of one, dropping.
   *
   * Opaque so that it can only be built by [[Signal.make]] — the capacity and the drop policy *are* the
   * contract. A bigger queue would bank redundant tokens, each costing a poll that finds nothing; an
   * unbounded one would let a producer's signals pile up without limit. Subtyping `Queue[Unit]` keeps
   * `offer` and `take` available without a wrapper.
   */
  opaque type Signal <: Queue[Unit] = Queue[Unit]

  object Signal:

    /**
     * A fresh signal, holding at most one untaken token.
     *
     * @return the signal; never fails
     */
    def make: UIO[Signal] = Queue.dropping(1)

  trait Source[E, A]:
    /**
     * Claim up to `upTo` elements, without blocking: an empty list means nothing is available *now*, not
     * that the source is exhausted.
     *
     * '''Must return at most `upTo`.''' The consumer reserves `upTo` permits before asking and hands back
     * whatever was not claimed; more than asked for would slip past `maxInFlight` unnoticed.
     */
    def tryAcquire(upTo: Int): IO[E, List[A]]
    def ack(element: A): IO[E, Unit]
    def nack(element: A, wait: Duration): IO[E, Unit]
