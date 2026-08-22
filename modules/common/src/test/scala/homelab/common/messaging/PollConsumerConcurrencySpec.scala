package homelab.common.messaging


import zio.*
import zio.test.*


/**
 * What `concurrency` buys and what it bounds: callers really do run at the same time, and the number of
 * elements the store has leased out at any instant never exceeds the number of callers that could be running.
 *
 * The lease ceiling is the headline claim of [[PollConsumer]]'s class doc and nothing else here checks it. It
 * does not follow from the queue sizes — it holds because a caller cannot spend a second demand token until
 * its first element has been *written* — so it has to be counted against the store, where a lease actually
 * lives, rather than against anything the consumer reports about itself.
 */
object PollConsumerConcurrencySpec extends ZIOSpecDefault:

  /**
   * Leases outstanding right now, and the most that were ever outstanding at once.
   *
   * @param now how many elements the store currently has handed out and unsettled
   * @param peak the high-water mark of `now`
   */
  private final case class Leases(now: Int, peak: Int):

    /**
     * Record a claim.
     *
     * @param count how many elements were claimed
     * @return the leases, with the peak raised if this claim set a new one
     */
    def claim(count: Int): Leases = Leases(now + count, peak max (now + count))

    /**
     * Record a settlement.
     *
     * @param count how many elements were acked or nacked
     * @return the leases, with those elements no longer outstanding
     */
    def settle(count: Int): Leases = Leases(now - count, peak)

  /**
   * A store that counts its leases rather than describing them: claiming raises the count, acking or nacking
   * lowers it, and the peak is what the assertions read.
   *
   * @param available the elements not yet claimed
   * @param leases the outstanding count and its high-water mark
   * @param asks every `upTo` the fetcher offered
   * @param settled every element acked or nacked, in the order it was settled
   */
  private final class Leased(
    available: Queue[Int],
    val leases: Ref[Leases],
    val asks: Ref[List[Int]],
    val settled: Ref[List[Int]],
  ) extends PollConsumer.Source[Nothing, Int]:

    override def claim(upTo: Int): IO[Nothing, List[Int]] = asks.update(_ :+ upTo) *> take(upTo)

    override def ack(elements: List[Int]): IO[Nothing, Unit] = retire(elements)

    override def nack(elements: List[Int], wait: Duration): IO[Nothing, Unit] = retire(elements)

    /**
     * Take up to `upTo` elements and count them out, as one indivisible step.
     *
     * A claim that raised the count but never reached the fetcher would be a phantom lease, and the peak
     * would then be measuring the test rather than the consumer.
     *
     * @param upTo the ceiling the fetcher asked for
     * @return the claimed elements; never fails
     */
    private def take(upTo: Int): UIO[List[Int]] =
      available
        .takeUpTo(upTo)
        .map(_.toList)
        .tap(claims => leases.update(_.claim(claims.size)))
        .uninterruptible

    /**
     * Count a batch back in, whichever way it was settled — a lease ends the same way for both verdicts.
     *
     * @param elements the settled elements
     * @return noop once counted
     */
    private def retire(elements: List[Int]): UIO[Unit] =
      leases.update(_.settle(elements.size)) *> settled.update(_ ++ elements)

    /**
     * Park until `count` elements have been settled.
     *
     * @param count how many settlements to wait for
     * @return noop once that many have been written
     */
    def awaitSettled(count: Int): UIO[Unit] = settled.get.map(_.size).repeatUntil(_ == count).unit

  /**
   * A store over `elements`.
   *
   * @param elements what is available to claim
   * @return the store; never fails
   */
  private def leased(elements: List[Int]): UIO[Leased] =
    for
      available <- Queue.unbounded[Int]
      _         <- available.offerAll(elements)
      leases    <- Ref.make(Leases(0, 0))
      asks      <- Ref.make(List.empty[Int])
      settled   <- Ref.make(List.empty[Int])
    yield Leased(available, leases, asks, settled)

  /**
   * A one-shot barrier: nobody passes until `width` callers are inside it at the same moment, and everybody
   * passes freely afterwards.
   *
   * Used inside `logic`, it is what turns "the callers presumably overlapped" into a fact: if the pipeline
   * serialised them the barrier would never open and the test would hang rather than quietly pass.
   *
   * @param arrived how many callers have reached it
   * @param open completed by the caller that makes the count
   * @param width how many must arrive
   */
  private final class Barrier(arrived: Ref[Int], open: Promise[Nothing, Unit], width: Int):

    /**
     * Wait for the barrier to open, opening it if this caller is the one that fills it.
     *
     * @return noop once `width` callers have arrived
     */
    def pass: UIO[Unit] =
      arrived.updateAndGet(_ + 1).flatMap(count => open.succeed(()).when(count >= width)) *> open.await

  private object Barrier:

    /**
     * A barrier for `width` callers.
     *
     * @param width how many must arrive before any may pass
     * @return the barrier; never fails
     */
    def make(width: Int): UIO[Barrier] =
      for
        arrived <- Ref.make(0)
        open    <- Promise.make[Nothing, Unit]
      yield Barrier(arrived, open, width)

  def spec: Spec[TestEnvironment & Scope, Any] = suite("PollConsumer concurrency")(
    test("leases never outnumber the callers, and all of the callers are used") {
      // `pollSize` is deliberately four times `concurrency`: the ceiling on outstanding leases comes from the
      // demand tokens, not from how much the fetcher is allowed to ask for. A consumer that claimed by poll
      // size, or that let a caller run ahead of its own write, peaks above the caller count here.
      val elements = 40
      val callers  = 4
      for
        source  <- leased((1 to elements).toList)
        barrier <- Barrier.make(callers)
        _       <- ZIO.scoped {
                     for
                       consumer <- PollConsumer.make(source, concurrency = callers, pollSize = 16, nackDelay = 1.second)
                       _        <- ZIO.foreachParDiscard(1 to callers)(_ => consumer.consume(_ => barrier.pass).forever.forkScoped)
                       _        <- source.awaitSettled(elements)
                     yield ()
                   }
        leases  <- source.leases.get
      yield assertTrue(leases.peak == callers, leases.now == 0)
    },
    test("every element is handed to exactly one caller") {
      // Eight callers pulling from one supply queue: the property is that the store's work is partitioned
      // among them, not merely covered. A duplicate here is an element done twice, which for a real handler
      // is the difference between a queue and a broadcast.
      val elements = 40
      val callers  = 8
      for
        source    <- leased((1 to elements).toList)
        processed <- Ref.make(List.empty[Int])
        _         <- ZIO.scoped {
                       for
                         consumer <- PollConsumer.make(source, concurrency = callers, pollSize = 8, nackDelay = 1.second)
                         _        <- ZIO.foreachParDiscard(1 to callers) { _ =>
                                       consumer.consume(element => processed.update(_ :+ element)).forever.forkScoped
                                     }
                         _        <- source.awaitSettled(elements)
                       yield ()
                     }
        seen      <- processed.get
        settled   <- source.settled.get
      yield assertTrue(seen.sorted == (1 to elements).toList, settled.sorted == (1 to elements).toList)
    },
    test("the fetcher never asks for more than the demand it is holding") {
      // The other side of the ceiling, seen from the query: `upTo` is capacity that already exists, so no row
      // is ever marked claimed on the chance that somebody will be free to run it.
      val callers = 4
      for
        source <- leased((1 to 40).toList)
        _      <- ZIO.scoped {
                    for
                      consumer <- PollConsumer.make(source, concurrency = callers, pollSize = 16, nackDelay = 1.second)
                      _        <- ZIO.foreachParDiscard(1 to callers)(_ => consumer.consume(_ => ZIO.unit).forever.forkScoped)
                      _        <- source.awaitSettled(40)
                    yield ()
                  }
        asks   <- source.asks.get
      yield assertTrue(asks.nonEmpty, asks.forall(upTo => upTo >= 1 && upTo <= callers))
    },
    test("a claim never exceeds the poll size, however much demand is waiting") {
      // And the reverse pairing: eight callers waiting, a store willing to hand out everything at once, and a
      // poll size of two. The query stays small — which is what keeps one consumer from taking a lock on a
      // large slice of a table shared with its peers — and the work still all gets done.
      val elements = 40
      val pollSize = 2
      for
        source <- leased((1 to elements).toList)
        _      <- ZIO.scoped {
                    for
                      consumer <- PollConsumer.make(source, concurrency = 8, pollSize = pollSize, nackDelay = 1.second)
                      _        <- ZIO.foreachParDiscard(1 to 8)(_ => consumer.consume(_ => ZIO.unit).forever.forkScoped)
                      _        <- source.awaitSettled(elements)
                    yield ()
                  }
        asks    <- source.asks.get
        settled <- source.settled.get
      yield assertTrue(asks.forall(_ <= pollSize), settled.sorted == (1 to elements).toList)
    },
  ) @@ TestAspect.withLiveClock @@ TestAspect.timeout(60.seconds)
