package homelab.common.messaging


import zio.*
import zio.test.*


/**
 * What the third queue changes: settlement is batched, it is still synchronous from the caller's side, and a
 * consumer torn down mid-flight still records the verdict it was holding.
 *
 * The teardown cases come first because they are the ones the design is arranged around — the registration
 * order in `PollConsumer.make` exists for them, and nothing else would catch it being wrong.
 */
object PollConsumerSpec extends ZIOSpecDefault:

  /** A store recording every claim and every settlement '''call''', so batching is observable, not inferred. */
  private final class Recording(
    available: Queue[Int],
    val asks: Ref[List[Int]],
    val handed: Ref[List[Int]],
    val ackCalls: Ref[List[List[Int]]],
    val nackCalls: Ref[List[List[Int]]],
    settleDelay: Duration,
  ) extends PollConsumer.Source[Nothing, Int]:
    override def claim(upTo: Int): IO[Nothing, List[Int]] =
      for
        _      <- asks.update(_ :+ upTo)
        claims <- available.takeUpTo(upTo).map(_.toList)
        _      <- handed.update(_ ++ claims)
      yield claims
    override def ack(elements: List[Int]): IO[Nothing, Unit] =
      ZIO.sleep(settleDelay) *> ackCalls.update(_ :+ elements)
    override def nack(elements: List[Int], wait: Duration): IO[Nothing, Unit] =
      ZIO.sleep(settleDelay) *> nackCalls.update(_ :+ elements)

    /** Park until `count` elements have actually been claimed — a load-proof stand-in for sleeping. */
    def awaitHandedOut(count: Int): UIO[Unit] = handed.get.map(_.size).repeatUntil(_ == count).unit

    /** Make one more element available, as something outside the consumer would. */
    def add(element: Int): UIO[Unit] = available.offer(element).unit

  private def recording(elements: List[Int], settleDelay: Duration = Duration.Zero): UIO[Recording] =
    for
      available <- Queue.unbounded[Int]
      _         <- available.offerAll(elements)
      asks      <- Ref.make(List.empty[Int])
      handed    <- Ref.make(List.empty[Int])
      acks      <- Ref.make(List.empty[List[Int]])
      nacks     <- Ref.make(List.empty[List[Int]])
    yield Recording(available, asks, handed, acks, nacks, settleDelay)

  extension (calls: List[List[Int]]) private def flat: List[Int] = calls.flatten.sorted

  def spec: Spec[TestEnvironment & Scope, Any] = suite("PollConsumer")(
    test("a consumer torn down mid-flight still records the verdict it was holding") {
      // The caller is interrupted inside `logic`, files a Failed verdict on the way out, and the settler —
      // which the registration order keeps alive longer than the callers — writes it. Swap the two `make`
      // lines in `PollConsumer.make` and this element is stranded until its lease expires.
      for
        source  <- recording(List(1))
        running <- Promise.make[Nothing, Unit] // the handler has the element and is inside `logic`
        gate    <- Promise.make[Nothing, Unit] // never completed: the handler is interrupted, not finished
        _       <- ZIO.scoped {
                     PollConsumer
                       .make(source, concurrency = 2, pollSize = 4, nackDelay = 1.second)
                       .flatMap(_.consume(_ => running.succeed(()) *> gate.await).forkScoped)
                       *> running.await
                   }
        nacked  <- source.nackCalls.get
      yield assertTrue(nacked.flat == List(1))
    },
    test("verdicts filed but not yet written are flushed at teardown") {
      // Settlement is made slow, so when the scope closes verdicts are queued and the settler is mid-write.
      // The flush is what stops them being dropped. Closing on "all four claimed" rather than on a sleep is
      // what keeps this honest under load: the verdicts provably exist by then, whatever the scheduler did.
      for
        source  <- recording((1 to 4).toList, settleDelay = 150.millis)
        done    <- Ref.make(0)
        _       <- ZIO.scoped {
                     PollConsumer
                       .make(source, concurrency = 4, pollSize = 4, nackDelay = 1.second)
                       .flatMap: consumer =>
                         ZIO.foreachParDiscard(1 to 4)(_ =>
                           consumer.consume(_ => done.update(_ + 1)).forkScoped
                         )
                       // Every handler has returned, so four Done verdicts are guaranteed to be filed — the
                       // offer sits in the uninterruptible half of `process`. None can have been written yet.
                       *> done.get.repeatUntil(_ == 4)
                   }
        settled <- source.ackCalls.get
      yield assertTrue(settled.flat == List(1, 2, 3, 4))
    },
    test("settlement is batched: four callers finishing together cost one write") {
      // The store is held for 200ms per write, so verdicts pile up behind the one in flight and are swept by a
      // single `takeBetween`. The assertion is "fewer writes than elements", not an exact count: how the four
      // split across batches depends on the scheduler, but *that* they share is the property. A per-element
      // settler produces four calls of one and fails here.
      for
        source <- recording((1 to 4).toList, settleDelay = 200.millis)
        result <- ZIO.scoped {
                    for
                      consumer <- PollConsumer.make(source, concurrency = 4, pollSize = 4, nackDelay = 1.second)
                      _      <- ZIO.foreachParDiscard(1 to 4)(_ => consumer.consume(_ => ZIO.unit).forkScoped)
                      _      <- source.ackCalls.get.map(_.flat.size).repeatUntil(_ == 4)
                      calls  <- source.ackCalls.get
                    yield assertTrue(calls.size < 4, calls.flat == List(1, 2, 3, 4))
                  }
      yield result
    },
    test("a mixed batch is split into one ack and one nack, by verdict") {
      // The partition in `write` is what a batching settler adds and a per-element one never needed: succeeded
      // and failed elements arrive interleaved on one queue and must leave as two statements, each carrying
      // only its own kind. Acking a failed element here would mean silently dropping work.
      for
        source <- recording((1 to 4).toList, settleDelay = 300.millis)
        result <- ZIO.scoped {
                    for
                      consumer <- PollConsumer.make(source, concurrency = 4, pollSize = 4, nackDelay = 1.second)
                      _      <- ZIO.foreachParDiscard(1 to 4) { _ =>
                                  consumer
                                    .consume(element => ZIO.fail("rejected").when(element % 2 == 0).unit)
                                    .either
                                    .forkScoped
                                }
                      _      <- (source.ackCalls.get <*> source.nackCalls.get)
                                  .map((acks, nacks) => acks.flat.size + nacks.flat.size)
                                  .repeatUntil(_ == 4)
                      acks   <- source.ackCalls.get
                      nacks  <- source.nackCalls.get
                    yield assertTrue(
                      acks.flat == List(1, 3),               // odd elements succeeded
                      nacks.flat == List(2, 4),              // even elements failed
                      acks.size + nacks.size < 4,            // and they still shared statements
                    )
                  }
      yield result
    },
    test("under sustained demand, batching collapses both the call count and the wall clock") {
      // The measurement the design is for. Settling costs a fixed 20ms regardless of how many elements the
      // call carries — which is what a round trip to a store actually looks like — so with 200 elements the
      // unbatched floor is 200 × 20ms = 4s of pure bookkeeping. Anything materially under that is batching,
      // and the call count says how much.
      val elements  = 200
      val callers   = 16
      val perCall   = 20.millis
      val unbatched = perCall * elements.toDouble
      val ceiling   = unbatched * 0.5
      for
        source  <- recording((1 to elements).toList, settleDelay = perCall)
        start   <- Clock.nanoTime
        _       <- ZIO.scoped {
                     for
                       consumer <- PollConsumer
                                   .make(source, concurrency = callers, pollSize = callers, nackDelay = 1.second)
                       _      <- ZIO.foreachParDiscard(1 to callers)(_ => consumer.consume(_ => ZIO.unit).forever.forkScoped)
                       _      <- source.ackCalls.get.map(_.flat.size).repeatUntil(_ == elements)
                     yield ()
                   }
        finish  <- Clock.nanoTime
        calls   <- source.ackCalls.get
        elapsed  = Duration.fromNanos(finish - start)
        average  = elements.toDouble / calls.size
        _       <- ZIO.debug(
                     f"batching: ${calls.size} calls for $elements elements (avg ${average}%.1f per call), "
                       + f"${elapsed.toMillis}ms against an unbatched floor of ${unbatched.toMillis}ms"
                   )
      yield assertTrue(
        calls.flat == (1 to elements).toList,   // nothing lost or duplicated on the way
        calls.size <= elements / 4,             // average batch of at least four
        calls.forall(_.size <= callers),        // and never more than the settler was allowed to take
        elapsed < ceiling,
      )
    },
    test("a quiet consumer pays no batching latency") {
      // The flip side: `takeBetween` blocks only for the *first* verdict, so a lone caller is written
      // immediately rather than waiting for a batch to fill or a timer to fire.
      for
        source <- recording(List(1))
        result <- ZIO.scoped {
                    PollConsumer
                      .make(source, concurrency = 4, pollSize = 4, nackDelay = 1.second)
                      .flatMap(_.consume(_ => ZIO.unit).timeout(2.seconds))
                  }
        calls  <- source.ackCalls.get
      yield assertTrue(result.isDefined, calls == List(List(1)))
    },
    test("consume does not return until the store has recorded the outcome") {
      // Capacity stays end-to-end: `consume` means "recorded", not "attempted", so a caller cannot run ahead
      // of what has been durably written.
      for
        source  <- recording(List(1), settleDelay = 300.millis)
        result  <- ZIO.scoped {
                     for
                       consumer <- PollConsumer.make(source, concurrency = 2, pollSize = 4, nackDelay = 1.second)
                       fiber  <- consumer.consume(_ => ZIO.unit).fork
                       _      <- ZIO.sleep(150.millis)
                       early  <- fiber.poll.map(_.isEmpty) // still waiting on the write
                       _      <- fiber.join
                       calls  <- source.ackCalls.get
                     yield assertTrue(early, calls.flat == List(1))
                   }
      yield result
    },
    test("a dead settler aborts its callers instead of leaving them unrecorded") {
      // The failure mode that matters most: a silent settler would let work be processed forever and never
      // recorded, redelivering everything on every lease expiry. It must take the consumer down instead.
      val broken = new PollConsumer.Source[String, Int]:
        override def claim(upTo: Int): IO[String, List[Int]]                     = ZIO.succeed(List(1))
        override def ack(elements: List[Int]): IO[String, Unit]                  = ZIO.fail("store is gone")
        override def nack(elements: List[Int], wait: Duration): IO[String, Unit] = ZIO.unit
      for
        outcome <- ZIO.scoped {
                     PollConsumer
                       .make(broken, concurrency = 2, pollSize = 4, nackDelay = 1.second)
                       .flatMap(_.consume(_ => ZIO.unit).either)
                   }
      yield assertTrue(outcome == Left("store is gone"))
    },
    test("a consumer parked on an empty store resumes when the wake-up is raised") {
      // Liveness, which no other test here can fail on: nothing calls a polling consumer, so an empty poll
      // parks the fetcher on the signal and only `wakeUp` (or the caller's own periodic tick) starts it
      // again. It doubles as the proof that a parked fetcher holds nothing — the second poll can only ask
      // for demand that was handed back before parking.
      for
        source <- recording(Nil)
        result <- ZIO.scoped {
                    for
                      consumer <- PollConsumer.make(source, concurrency = 2, pollSize = 4, nackDelay = 1.second)
                      caller   <- consumer.consume(_ => ZIO.unit).fork
                      _        <- source.asks.get.repeatUntil(_.nonEmpty) // the first poll came back empty
                      _        <- ZIO.sleep(100.millis)                   // and the fetcher is parked on the signal
                      early    <- caller.poll.map(_.isEmpty)
                      _        <- source.add(7) *> consumer.wakeUp
                      _        <- caller.join
                      asks     <- source.asks.get
                      acked    <- source.ackCalls.get
                    yield assertTrue(early, asks.size >= 2, asks.forall(_ == 1), acked.flat == List(7))
                  }
      yield result
    },
    test("claims nothing beyond the demand in hand") {
      // v2's invariant, unchanged: one blocked caller means exactly one claimed element.
      for
        source  <- recording((1 to 10).toList)
        blocked <- Promise.make[Nothing, Unit]
        _       <- ZIO.scoped {
                     PollConsumer
                       .make(source, concurrency = 4, pollSize = 4, nackDelay = 1.second)
                       .flatMap(_.consume(_ => blocked.await).forkScoped) *> ZIO.sleep(250.millis)
                   }
        offered <- source.asks.get
      yield assertTrue(offered == List(1))
    },
  ) @@ TestAspect.withLiveClock @@ TestAspect.timeout(30.seconds)
