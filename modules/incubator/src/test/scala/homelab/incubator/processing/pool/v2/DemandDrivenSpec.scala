package homelab.incubator.processing.pool.v2


import zio.*
import zio.test.*


/**
 * What the demand/supply split buys over v1's permit counter, asserted on what the *source is asked for* and
 * what it gets back — the consequences, rather than the bookkeeping that produces them.
 *
 * These are races, so each is made deterministic: the source itself raises the signal at the moment a naive
 * implementation would miss it, and a promise decides when capacity frees.
 */
object DemandDrivenSpec extends ZIOSpecDefault:

  /** A leased source over a fixed pool of elements, recording every ask and every settlement. */
  private final class Recording(
    available: Queue[Int],
    val asks: Ref[List[Int]],
    val acked: Ref[List[Int]],
    val nacked: Ref[List[Int]],
  ) extends DemandDriven.Source[Nothing, Int]:
    override def tryAcquire(upTo: Int): IO[Nothing, List[Int]] =
      asks.update(_ :+ upTo) *> available.takeUpTo(upTo).map(_.toList)
    override def ack(elements: List[Int]): IO[Nothing, Unit]                  = acked.update(_ ++ elements)
    override def nack(elements: List[Int], wait: Duration): IO[Nothing, Unit] = nacked.update(_ ++ elements)

  private def recording(elements: List[Int]): UIO[Recording] =
    for
      available <- Queue.unbounded[Int]
      _         <- available.offerAll(elements)
      asks      <- Ref.make(List.empty[Int])
      acked     <- Ref.make(List.empty[Int])
      nacked    <- Ref.make(List.empty[Int])
    yield Recording(available, asks, acked, nacked)

  /** The pool: `workers` consumer loops over one started fetcher, all interrupted when the scope closes. */
  private def pool[E](
    source: DemandDriven.Source[E, Int],
    workers: Int,
    pollSize: Int = 4,
  )(logic: Int => UIO[Unit]): ZIO[Scope, Nothing, DemandDriven.Worker[E, Int]] =
    DemandDriven.Worker
      .make(source, concurrency = workers, pollSize = pollSize, nackDelay = 1.second)
      .tap(worker => ZIO.foreachDiscard(1 to workers)(_ => worker.consume(logic).forever.forkScoped))

  def spec: Spec[TestEnvironment & Scope, Any] = suite("DemandDriven")(
    test("claims nothing beyond the demand in hand") {
      // v1 asked for the whole `pollSize` here and held three leases it had no capacity to run.
      for
        source  <- recording((1 to 10).toList)
        blocked <- Promise.make[Nothing, Unit] // never completed: the handler never finishes
        _       <- ZIO.scoped(pool(source, workers = 1)(_ => blocked.await) *> ZIO.sleep(250.millis))
        offered <- source.asks.get
      yield assertTrue(offered == List(1))
    },
    test("a worker finishing is what frees the next claim") {
      // The wait v1 could not express: at `maxInFlight` it parked on a signal that only ever fired for data,
      // so nothing woke it when a handler completed. Here capacity is a token, and returning it *is* the
      // wake-up — no signal involved, which is why nothing raises one in this test.
      for
        source <- recording((1 to 5).toList)
        gate   <- Promise.make[Nothing, Unit]
        result <- ZIO.scoped {
                    for
                      _       <- pool(source, workers = 2)(_ => gate.await)
                      _       <- ZIO.sleep(250.millis)
                      taken   <- source.asks.get.map(_.sum)
                      settled <- source.acked.get
                      _       <- gate.succeed(())
                      _       <- source.acked.get.map(_.size).repeatUntil(_ == 5)
                    yield assertTrue(taken == 2, settled.isEmpty)
                  }
      yield result
    },
    test("a signal raised between the poll and the wait is held, not lost") {
      // Raised *after* the source has decided it is empty and *before* the fetcher parks. A wait-list would
      // have dropped it and slept on work that was already there; the dropping queue of one holds it. If that
      // regresses, this test times out rather than failing.
      //
      // With the wake-up owned by the worker, a source that raises it has a chicken-and-egg problem: the pool
      // needs the source to exist, and the source needs the pool. A `Promise` holding the effect breaks it —
      // the first poll simply waits for the wiring, which is also what a real adapter would do.
      for
        waker     <- Promise.make[Nothing, UIO[Unit]]
        polls     <- Ref.make(0)
        handedAt  <- Ref.make(Option.empty[Int]) // which poll the single element came back on
        acked     <- Ref.make(List.empty[Int])
        source     = new DemandDriven.Source[Nothing, Int]:
                       override def tryAcquire(upTo: Int): IO[Nothing, List[Int]] =
                         polls.getAndUpdate(_ + 1).flatMap:
                           case 0    => waker.await.flatten.as(Nil) // *after* the check, *before* the wait
                           case poll =>
                             handedAt.modify:
                               case None  => List(1) -> Some(poll)
                               case taken => Nil     -> taken
                       override def ack(elements: List[Int]): IO[Nothing, Unit]                  = acked.update(_ ++ elements)
                       override def nack(elements: List[Int], wait: Duration): IO[Nothing, Unit] = ZIO.unit
        processed <- Ref.make(List.empty[Int])
        _         <- ZIO.scoped {
                       pool(source, workers = 1)(element => processed.update(_ :+ element))
                         .flatMap(worker => waker.succeed(worker.wakeUp))
                         *> processed.get.repeatUntil(_.nonEmpty)
                     }
        poll      <- handedAt.get
        settled   <- acked.get
      yield assertTrue(poll == Some(1), settled == List(1))
    },
    test("shutdown returns what was claimed but never handed over") {
      // Demand with no worker behind it: the fetcher fills `supply` and nobody takes. Those are leases with a
      // clock running, and the drain is what stops a closing pool from stranding them. Reaching past
      // `Worker.make` for the channel is what makes the orphaned-demand state constructible at all.
      for
        source   <- recording(List(1, 2))
        signal   <- DemandDriven.Signal.make
        channel  <- DemandDriven.Channel.make[Nothing, Int](2, signal)
        _        <- channel.demand.offerAll(List((), ()))
        _        <- ZIO.scoped {
                      DemandDriven.Fetcher.make(source, channel, 4, 1.second) *> ZIO.sleep(250.millis)
                    }
        returned <- source.nacked.get
        left     <- channel.supply.size
      yield assertTrue(returned.sorted == List(1, 2), left == 0)
    },
    test("a raised signal with nothing to take costs one extra poll, not a spin") {
      // Carried over from v1. The wake-up is a token, so cashing it buys exactly one poll: the fetcher finds
      // the source still empty, hands its demand back, and parks again. A latch that stayed open, or a signal
      // re-raised by the empty poll, would show up here as a poll count that keeps climbing.
      for
        source <- recording(Nil)
        result <- ZIO.scoped {
                    for
                      worker <- pool(source, workers = 1)(_ => ZIO.unit)
                      _      <- ZIO.sleep(150.millis)
                      before <- source.asks.get.map(_.size) // the opening poll, no wake-up involved
                      _      <- worker.wakeUp
                      _      <- ZIO.sleep(300.millis)
                      after  <- source.asks.get.map(_.size)
                    yield assertTrue(before == 1, after == 2)
                  }
      yield result
    },
    test("ticks are absorbed while every worker is busy") {
      // A periodic wake-up is safe precisely because it is self-limiting: a saturated pool never reaches
      // `signal.take`, so the ticker costs nothing exactly when background polling would be least welcome.
      // Ticks cannot pile up either — the dropping queue banks at most one, so a long idle spell wakes to a
      // single poll rather than a burst.
      for
        source <- recording((1 to 10).toList)
        gate   <- Promise.make[Nothing, Unit] // never completed: the one worker stays busy throughout
        result <- ZIO.scoped {
                    for
                      worker <- pool(source, workers = 1)(_ => gate.await)
                      _      <- worker.wakeUp.repeat(Schedule.spaced(20.millis)).forkScoped
                      _      <- ZIO.sleep(400.millis)
                      polled <- source.asks.get
                    yield assertTrue(polled == List(1)) // ~20 ticks, one poll — the opening one
                  }
      yield result
    },
    test("a dead fetcher aborts its workers instead of parking them") {
      // The cost of forking inside `make`: the failure has nowhere to go on its own. Without the race in
      // `consume` this test times out — the worker waits on a supply nothing will ever fill again.
      val broken = new DemandDriven.Source[String, Int]:
        override def tryAcquire(upTo: Int): IO[String, List[Int]]         = ZIO.fail("source is gone")
        override def ack(elements: List[Int]): IO[String, Unit]                  = ZIO.unit
        override def nack(elements: List[Int], wait: Duration): IO[String, Unit] = ZIO.unit
      for
        outcome <- ZIO.scoped {
                     DemandDriven.Worker
                       .make(broken, concurrency = 2, pollSize = 4, nackDelay = 1.second)
                       .flatMap(_.consume(_ => ZIO.unit).either)
                   }
      yield assertTrue(outcome == Left("source is gone"))
    },
  ) @@ TestAspect.withLiveClock @@ TestAspect.timeout(20.seconds)
