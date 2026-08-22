package homelab.common.processing


import zio.*
import zio.test.*


/**
 * The teardown paths that leave no trace in the happy case, and that a store notices only hours later.
 *
 * [[PollConsumerSpec]] already covers the verdict a caller files on its way out. What is left is the other
 * half of shutdown: elements the fetcher claimed that no caller ever received, and the close message itself —
 * which is the one finalizer here that waits on another fiber, and so the one that can hang the whole scope.
 *
 * Every assertion is made about what the '''store''' was told, never about the consumer's internals. An
 * element left claimed is invisible from the inside; it shows up only as a claim with no matching settlement.
 */
object PollConsumerTeardownSpec extends ZIOSpecDefault:

  /**
   * A store recording every claim and every settlement, optionally holding each claim on a gate so a teardown
   * can be arranged around a poll that is in flight.
   *
   * @param available the elements not yet claimed
   * @param asks every `upTo` the fetcher offered
   * @param claimed every element handed out
   * @param acked every `ack` call, batches kept apart
   * @param nacked every `nack` call, batches kept apart
   * @param gate held before each claim, when present
   */
  private final class Store(
    available: Queue[Int],
    val asks: Ref[List[Int]],
    val claimed: Ref[List[Int]],
    val acked: Ref[List[List[Int]]],
    val nacked: Ref[List[List[Int]]],
    gate: Option[Promise[Nothing, Unit]],
  ) extends PollConsumer.Source[Nothing, Int]:

    override def claim(upTo: Int): IO[Nothing, List[Int]] =
      asks.update(_ :+ upTo) *> ZIO.foreachDiscard(gate)(_.await) *> take(upTo)

    override def ack(elements: List[Int]): IO[Nothing, Unit] = acked.update(_ :+ elements)

    override def nack(elements: List[Int], wait: Duration): IO[Nothing, Unit] = nacked.update(_ :+ elements)

    /**
     * Take up to `upTo` elements and record them, as one indivisible step.
     *
     * A real store claims in a single statement, so a claim that was recorded but never returned cannot
     * happen there and must not happen here either — it would show up as a phantom lease and fail these
     * tests for a reason the consumer is not responsible for.
     *
     * @param upTo the ceiling the fetcher asked for
     * @return the claimed elements; never fails
     */
    private def take(upTo: Int): UIO[List[Int]] =
      available
        .takeUpTo(upTo)
        .map(_.toList)
        .tap(got => claimed.update(_ ++ got))
        .uninterruptible

    /**
     * Park until `count` elements have been claimed — a load-proof stand-in for sleeping.
     *
     * @param count how many claims to wait for
     * @return noop once that many have been handed out
     */
    def awaitClaimed(count: Int): UIO[Unit] = claimed.get.map(_.size).repeatUntil(_ == count).unit

  /**
   * A store over `elements`.
   *
   * @param elements what is available to claim
   * @param gate held before each claim, when present
   * @return the store; never fails
   */
  private def store(elements: List[Int], gate: Option[Promise[Nothing, Unit]] = None): UIO[Store] =
    for
      available <- Queue.unbounded[Int]
      _         <- available.offerAll(elements)
      asks      <- Ref.make(List.empty[Int])
      claimed   <- Ref.make(List.empty[Int])
      acked     <- Ref.make(List.empty[List[Int]])
      nacked    <- Ref.make(List.empty[List[Int]])
    yield Store(available, asks, claimed, acked, nacked, gate)

  def spec: Spec[TestEnvironment & Scope, Any] = suite("PollConsumer teardown")(
    test("elements claimed but never handed to a worker are given back") {
      // The fetcher's drain, which nothing else exercises: the callers spend their demand, are interrupted
      // while the poll is still in flight, and the elements that poll returns arrive in a supply queue nobody
      // is left to take from. Delete `Fetcher.drain` and these sit claimed until their leases expire.
      for
        gate    <- Promise.make[Nothing, Unit]
        source  <- store((1 to 4).toList, gate = Some(gate))
        _       <- ZIO.scoped {
                     for
                       consumer <- PollConsumer.make(source, concurrency = 4, pollSize = 4, nackDelay = 1.second)
                       callers  <- ZIO.foreach(1 to 4)(_ => consumer.consume(_ => ZIO.unit).fork)
                       _        <- source.asks.get.repeatUntil(_.nonEmpty) // demand spent, the claim in flight
                       _        <- Fiber.interruptAll(callers)             // and now nobody is left to take it
                       _        <- gate.succeed(())
                       _        <- source.awaitClaimed(4)
                       _        <- ZIO.sleep(200.millis)                   // let the claims reach the supply queue
                     yield ()
                   }
        claims  <- source.claimed.get
        nacked  <- source.nacked.get
        acked   <- source.acked.get
      yield assertTrue(
        claims.sorted == List(1, 2, 3, 4),
        nacked.flatten.sorted == claims.sorted, // everything claimed was handed back
        nacked.size == 1,                       // in one statement, not one per stranded element
        acked.isEmpty,                          // and nothing was retired: no worker ever saw them
      )
    },
    test("nothing is left claimed once the scope has closed") {
      // The composite invariant, and the one a store actually cares about: after teardown, every element the
      // consumer ever claimed has been settled exactly once — whether it was finished, interrupted inside
      // `logic`, or stranded in the supply queue. Any leak shows up as a claim with no settlement.
      val elements = 24
      for
        source  <- store((1 to elements).toList)
        _       <- ZIO.scoped {
                     for
                       consumer <- PollConsumer.make(source, concurrency = 8, pollSize = 8, nackDelay = 1.second)
                       _        <- ZIO.foreachParDiscard(1 to 8) { _ =>
                                     consumer.consume(element => ZIO.sleep((element % 5 * 20).millis)).forever.forkScoped
                                   }
                       _        <- source.awaitClaimed(elements) // the store is empty; only in-flight work is left
                       _        <- ZIO.sleep(100.millis)         // and the last claim has reached a caller
                     yield ()
                   }
        claims  <- source.claimed.get
        acked   <- source.acked.get
        nacked  <- source.nacked.get
        settled  = (acked.flatten ++ nacked.flatten).sorted
      yield assertTrue(claims.sorted == (1 to elements).toList, settled == (1 to elements).toList)
    },
    test("closing terminates when the settler has already died") {
      // `Settler.close` posts a message and waits for the settler to answer, so it is only safe while there
      // is a settler to answer it. A dead one must not turn shutdown into a hang: the wait has to observe the
      // fiber's death, not merely its cooperation. Note the finalizer is uninterruptible, so `timeout` cannot
      // rescue a genuine deadlock here — a regression fails this suite by exhausting its own timeout.
      val broken = new PollConsumer.Source[String, Int]:
        override def claim(upTo: Int): IO[String, List[Int]]                     = ZIO.succeed(List(1))
        override def ack(elements: List[Int]): IO[String, Unit]                  = ZIO.fail("store is gone")
        override def nack(elements: List[Int], wait: Duration): IO[String, Unit] = ZIO.unit
      for
        outcome <- ZIO
                     .scoped {
                       PollConsumer
                         .make(broken, concurrency = 2, pollSize = 4, nackDelay = 1.second)
                         .flatMap(_.consume(_ => ZIO.unit).either)
                     }
                     .timeout(10.seconds)
      yield assertTrue(outcome.contains(Left("store is gone")))
    },
    test("a consumer that was never used closes promptly and touches nothing") {
      // Both background fibers are parked on empty queues here — the fetcher on the wake-up, the settler on
      // the settlement queue. The close message is what unparks the settler, and a consumer built and dropped
      // is the cheapest way to prove that path does not depend on any traffic having gone through it.
      for
        source  <- store(Nil)
        elapsed <- ZIO.scoped(PollConsumer.make(source, concurrency = 4, pollSize = 4, nackDelay = 1.second).unit).timed
        acked   <- source.acked.get
        nacked  <- source.nacked.get
      yield assertTrue(elapsed._1 < 5.seconds, acked.isEmpty, nacked.isEmpty)
    },
  ) @@ TestAspect.withLiveClock @@ TestAspect.timeout(60.seconds)
