package homelab.incubator.processing.pool.v1


import zio.*
import zio.test.*


/**
 * The signal's contract: one that arrives before anyone is waiting is *held*, not dropped, and one raised
 * with nothing to take costs a single extra poll rather than a spin. Both are races, so both are reproduced
 * deterministically — the source itself raises the signal at the exact moment the old wait-list would have
 * missed it.
 *
 * One `consume` is one poll, so every test here supplies its own loop.
 */
object PollConsumerSpec extends ZIOSpecDefault:

  private def consumerOf(
    src: PollConsumer.Source[Nothing, Int],
    queue: PollConsumer.Signal,
    inFlight: Ref[Int],
    poll: Int = 4,
    inFlightCap: Int = 8,
  ): PollConsumer[Nothing, Int] = new PollConsumer[Nothing, Int]:
    override def pollSize: Int                             = poll
    override def maxInFlight: Int                          = inFlightCap
    override def running: Ref[Int]                         = inFlight
    override def signal: PollConsumer.Signal                = queue
    override def source: PollConsumer.Source[Nothing, Int] = src

  private def emptySource(polls: Ref[Int]): PollConsumer.Source[Nothing, Int] =
    new PollConsumer.Source[Nothing, Int]:
      override def tryAcquire(upTo: Int): IO[Nothing, List[Int]]         = polls.update(_ + 1).as(Nil)
      override def ack(element: Int): IO[Nothing, Unit]                  = ZIO.unit
      override def nack(element: Int, wait: Duration): IO[Nothing, Unit] = ZIO.unit

  def spec: Spec[TestEnvironment & Scope, Any] = suite("PollConsumer")(
    test("a signal raised between the poll and the wait is held, not lost") {
      // The old wait-list found an empty list here and dropped it; the consumer then registered and slept on
      // work that was already available. If that regresses this test times out rather than failing.
      for
        queue     <- PollConsumer.Signal.make
        running   <- Ref.make(0)
        polls     <- Ref.make(0)
        acked     <- Ref.make(List.empty[Int])
        processed <- Ref.make(List.empty[Int])
        source     = new PollConsumer.Source[Nothing, Int]:
                       override def tryAcquire(upTo: Int): IO[Nothing, List[Int]] =
                         polls.getAndUpdate(_ + 1).flatMap:
                           case 0 => queue.offer(()).as(Nil) // *after* the check, *before* the wait
                           case _ => ZIO.succeed(List(1))
                       override def ack(element: Int): IO[Nothing, Unit]                  = acked.update(_ :+ element)
                       override def nack(element: Int, wait: Duration): IO[Nothing, Unit] = ZIO.unit
        _         <- consumerOf(source, queue, running)
                       .consume(element => processed.update(_ :+ element))
                       .repeatUntilZIO(_ => processed.get.map(_.nonEmpty)) // the caller owns the loop
        seen      <- processed.get
        settled   <- acked.get
        count     <- polls.get
      yield assertTrue(seen == List(1), settled == List(1), count == 2)
    },
    test("a signal with nothing to take costs one extra poll, not a spin") {
      for
        queue   <- PollConsumer.Signal.make
        running <- Ref.make(0)
        polls   <- Ref.make(0)
        fiber   <- consumerOf(emptySource(polls), queue, running).consume(_ => ZIO.unit).forever.fork
        _       <- queue.offer(())
        _       <- ZIO.sleep(300.millis)
        count   <- polls.get
        parked  <- fiber.poll.map(_.isEmpty)
        _       <- fiber.interrupt
      yield assertTrue(count == 2, parked)
    },
    test("holds no permits while parked, so a sibling can claim the whole allowance") {
      // `pollSize == maxInFlight`, so if the parked consumer were still holding its reservation the sibling
      // would be offered 0. Asserting on what the *source is asked for* tests the consequence rather than
      // the counter, and cannot pass by accident.
      for
        queue    <- PollConsumer.Signal.make
        running  <- Ref.make(0)
        asks     <- Ref.make(List.empty[Int])
        source    = new PollConsumer.Source[Nothing, Int]:
                      override def tryAcquire(upTo: Int): IO[Nothing, List[Int]] = asks.update(_ :+ upTo).as(Nil)
                      override def ack(element: Int): IO[Nothing, Unit]          = ZIO.unit
                      override def nack(element: Int, wait: Duration): IO[Nothing, Unit] = ZIO.unit
        parked   <- consumerOf(source, queue, running, poll = 4, inFlightCap = 4).consume(_ => ZIO.unit).forever.fork
        _        <- ZIO.sleep(150.millis)
        sibling  <- consumerOf(source, queue, running, poll = 4, inFlightCap = 4).consume(_ => ZIO.unit).forever.fork
        _        <- ZIO.sleep(150.millis)
        offered  <- asks.get
        held     <- running.get
        _        <- parked.interrupt *> sibling.interrupt
      yield assertTrue(offered == List(4, 4), held == 0)
    },
  ) @@ TestAspect.withLiveClock @@ TestAspect.timeout(15.seconds)
