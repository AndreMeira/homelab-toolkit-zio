package homelab.incubator.processing.pool.v3


import zio.*
import zio.test.*


/**
 * Teardown, repeated, because once is not enough to see it.
 *
 * The verdict-loss bug this guards against passed a single-shot test every time and failed roughly one run in
 * three under load — the window is between a queue completing a parked taker and that taker resuming, so it
 * only opens when the scheduler is busy. Twelve consecutive pools, each torn down with an element in flight,
 * is what made it reproducible; anything less reports success on a broken settler.
 */
object TeardownStressSpec extends ZIOSpecDefault:

  /** One pool, one element, torn down while the handler holds it. Reports what the store was told. */
  private def torndownMidFlight(run: Int): UIO[String] =
    for
      remaining <- Ref.make(1)
      claimed   <- Ref.make(List.empty[Int])
      acked     <- Ref.make(List.empty[List[Int]])
      nacked    <- Ref.make(List.empty[List[Int]])
      running   <- Promise.make[Nothing, Unit] // the handler has the element
      gate      <- Promise.make[Nothing, Unit] // never completed: it is interrupted, not finished
      source     = new DemandDriven.Source[Nothing, Int]:
                     override def tryAcquire(upTo: Int): IO[Nothing, List[Int]] =
                       remaining
                         .getAndSet(0)
                         .map(left => if left > 0 then List(1) else Nil)
                         .tap(got => claimed.update(_ ++ got))
                     override def ack(elements: List[Int]): IO[Nothing, Unit] = acked.update(_ :+ elements)
                     override def nack(elements: List[Int], wait: Duration): IO[Nothing, Unit] =
                       nacked.update(_ :+ elements)
      _         <- ZIO.scoped {
                     DemandDriven.Worker
                       .make(source, concurrency = 2, pollSize = 4, nackDelay = 1.second)
                       .flatMap(_.consume(_ => running.succeed(()) *> gate.await).forkScoped)
                       *> running.await
                   }
      got       <- claimed.get
      a         <- acked.get
      n         <- nacked.get
    yield s"run $run: claimed=$got ack=$a nack=$n"

  def spec: Spec[TestEnvironment & Scope, Any] = suite("teardown stress")(
    test("every torn-down pool records the verdict of the element it was holding") {
      // The expected line is the whole assertion: the element was claimed, and the store was told to hand it
      // back. A dropped verdict shows up as `nack=List()` — the element left claimed, invisible until its
      // lease expires. Failures print every run, so a partial loss is legible rather than a bare count.
      ZIO.foreach(1 to 12)(torndownMidFlight).map { runs =>
        assertTrue(runs.filterNot(_.endsWith("ack=List() nack=List(List(1))")) == Chunk.empty)
      }
    },
  ) @@ TestAspect.withLiveClock @@ TestAspect.timeout(60.seconds)
