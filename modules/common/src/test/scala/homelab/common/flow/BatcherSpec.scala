package homelab.common.flow


import homelab.common.data.Batch
import homelab.common.error.ApplicationError
import zio.*
import zio.test.*


// Correctness spec for the batcher family. Deterministic (promise-gated); a per-suite timeout turns any hang
// into a failure rather than blocking the run.
object BatcherSpec extends ZIOSpecDefault:

  case object Boom extends ApplicationError:
    override def message: String = "boom"

  private def serial[E, BE](
    logic: Batcher.Logic[E, BE, Int, Int]
  ): ZIO[Scope, Batcher.InvalidBatchSize, Batcher[Batcher.Failure[E, BE], Int, Int]] =
    Batcher.serial(1024, logic)

  private def dedup[E, BE](key: Int => Int, logic: Batcher.Logic[E, BE, Int, Int])
    : ZIO[Scope, Batcher.InvalidBatchSize, Batcher[Batcher.Failure[E, BE], Int, Int]] =
    Batcher.deduplicated(1024, key, logic)

  private def dedupSized[E, BE](batchSize: Int, key: Int => Int, logic: Batcher.Logic[E, BE, Int, Int])
    : ZIO[Scope, Batcher.InvalidBatchSize, Batcher[Batcher.Failure[E, BE], Int, Int]] =
    Batcher.deduplicated(batchSize, key, logic)

  // Records every batch the logic was handed, so a test can assert what reached the downstream call rather
  // than only what callers got back.
  private def recordingGatedLogic(
    seen: Ref[List[List[Int]]],
    entered: Promise[Nothing, Unit],
    gate: Promise[Nothing, Unit],
  ): Batcher.Logic[Nothing, Nothing, Int, Int] =
    in => seen.update(_ :+ in.values.toList) *> entered.succeed(()) *> gate.await.as(in.map(_ * 10))

  private def gatedLogic(
    entered: Promise[Nothing, Unit],
    gate: Promise[Nothing, Unit],
  ): Batcher.Logic[Nothing, Nothing, Int, Int] =
    in => entered.succeed(()) *> gate.await.as(in.map(_ * 10))

  private val mapped: Batcher.Logic[Nothing, Nothing, Int, Int] =
    in => ZIO.succeed(in.map(_ * 10))

  private val failsBatch: Batcher.Logic[Boom.type, Nothing, Int, Int] =
    _ => ZIO.fail(Boom)

  // Counts logic invocations and the largest batch it saw, then sleeps so concurrent callers overlap in flight.
  private def countingSleepLogic(
    calls: Ref[Int],
    maxSize: Ref[Int],
  ): Batcher.Logic[Nothing, Nothing, Int, Int] = in =>
    val size = in.values.size
    calls.update(_ + 1) *> maxSize.update(_ max size) *> ZIO.sleep(50.millis).as(in.map(_ * 10))

  def spec = suite("Batcher correctness")(
    suite("serial")(
      test("returns each caller's result under concurrency") {
        ZIO.scoped:
          for
            b   <- serial(mapped)
            out <- ZIO.foreachPar((1 to 200).toList)(b.run)
          yield assertTrue(out == (1 to 200).map(_ * 10).toList)
      },
      test("a whole-batch failure completes every caller with that error — no hang") {
        ZIO.scoped:
          for
            b   <- serial(failsBatch)
            out <- ZIO.foreachPar((1 to 100).toList)(b.run(_).either)
          yield assertTrue(out.forall(_ == Left(Boom)))
      },
      test("a per-item failure completes only that caller with the error") {
        val perItem = Batcher.Logic.fromFunction: (in: Batch.Success[Int]) =>
          ZIO.succeed(in.mapEither(i => if i % 2 == 0 then Right(i * 10) else Left(Boom)))

        ZIO.scoped:
          for
            b   <- serial(perItem)
            out <- ZIO.foreachPar((1 to 100).toList)(i => b.run(i).either.map(i -> _))
          yield assertTrue(out.forall((i, r) => r == (if i % 2 == 0 then Right(i * 10) else Left(Boom))))
      },
      test("stays usable across drain cycles (idle → busy → idle)") {
        ZIO.scoped:
          for
            b    <- serial(mapped)
            outs <- ZIO.foreach((1 to 20).toList)(b.run)
          yield assertTrue(outs == (1 to 20).map(_ * 10).toList)
      },
      test("interrupting one caller neither wedges the batcher nor strands the batch") {
        for
          gate    <- Promise.make[Nothing, Unit]
          entered <- Promise.make[Nothing, Unit]
          out     <- ZIO.scoped:
                       for
                         b      <- serial(gatedLogic(entered, gate))
                         fiberA <- b.run(1).fork
                         _      <- entered.await
                         _      <- fiberA.interrupt
                         _      <- gate.succeed(())
                         bOut   <- b.run(2)
                       yield bOut
        yield assertTrue(out == 20)
      },
      test("a defect in one batch does not strand later callers") {
        val dying: Batcher.Logic[Nothing, Nothing, Int, Int] = _ => ZIO.dieMessage("boom")
        ZIO.scoped:
          for
            b     <- Batcher.serial(1, dying) // batchSize 1 → each request is its own batch
            exits <- ZIO.foreachPar((1 to 50).toList)(b.run(_).exit)
          yield assertTrue(exits.forall(_.isFailure)) // all completed as defects, none stranded
      },
      test("closing the scope interrupts an in-flight caller — no hang") {
        for
          gate    <- Promise.make[Nothing, Unit]
          entered <- Promise.make[Nothing, Unit]
          scope   <- Scope.make
          b       <- scope.extend(serial(gatedLogic(entered, gate)))
          fiberA  <- b.run(1).fork
          _       <- entered.await          // A's request is in flight in the drain
          _       <- scope.close(Exit.unit) // returns (doesn't block on the interruptible drain)
          exit    <- fiberA.await
        yield assertTrue(exit.isInterrupted)
      },
    ),
    suite("deduplicated")(
      test("routes each key's result to its callers") {
        val keys = 10
        ZIO.scoped:
          for
            b   <- dedup(_ % keys, in => ZIO.succeed(in.map(_ % keys)))
            out <- ZIO.foreachPar((1 to 300).toList)(i => b.run(i).map(i -> _))
          yield assertTrue(out.forall((i, r) => r == i % keys))
      },
      test("a whole-batch failure completes every caller — no hang") {
        ZIO.scoped:
          for
            b   <- dedup(_ % 10, failsBatch)
            out <- ZIO.foreachPar((1 to 100).toList)(b.run(_).either)
          yield assertTrue(out.forall(_ == Left(Boom)))
      },
      test("a per-key failure is shared by that key's callers only") {
        val keys                                                = 10
        val perKey: Batcher.Logic[Nothing, Boom.type, Int, Int] =
          in => ZIO.succeed(in.mapEither(i => if i % keys < 5 then Right(i % keys) else Left(Boom)))
        ZIO.scoped:
          for
            b   <- dedup(_ % keys, perKey)
            out <- ZIO.foreachPar((1 to 300).toList)(i => b.run(i).either.map(i -> _))
          yield assertTrue(out.forall((i, r) => r == (if i % keys < 5 then Right(i % keys) else Left(Boom))))
      },
      test("duplicates arriving during a call coalesce into one follow-up, not into the call in flight") {
        // The property that makes this a *deduplicating* batcher, and its exact boundary. 100 callers on one
        // key, arriving while that key is already in flight, produce exactly *one* extra downstream item
        // between them — but they do not join the running computation and take its answer. That matters:
        // joining it would hand a caller a result computed before it asked, which for a key whose value can
        // change is a stale read. Dedup coalesces callers that are *waiting*, never one already running.
        ZIO.scoped:
          for
            seen    <- Ref.make(List.empty[List[Int]])
            entered <- Promise.make[Nothing, Unit]
            gate    <- Promise.make[Nothing, Unit]
            b       <- dedup(_ => 0, recordingGatedLogic(seen, entered, gate))
            leader  <- b.run(1).fork
            _       <- entered.await                             // the key is now in flight
            others  <- ZIO.foreachPar((2 to 101).toList)(b.run).fork
            _       <- ZIO.yieldNow.repeatN(50)                   // let all 100 arrive and share one slot
            _       <- gate.succeed(())
            first   <- leader.join
            rest    <- others.join
            batches <- seen.get
          yield assertTrue(
            batches.size == 2,                        // the in-flight call, then one for all 100
            batches.forall(_.size == 1),              // each carrying a single value, not a hundred
            first == 10,                              // the leader gets its own
            rest.distinct.size == 1,                  // the hundred share one result…
            rest.head != first,                       // …computed after they asked, not the leader's
          )
      },
      test("dedup is per in-flight window, not a cache") {
        // A key asked for again *after* its computation settled starts a fresh one — this coalesces
        // concurrent work, it does not memoise.
        ZIO.scoped:
          for
            calls <- Ref.make(0)
            b     <- dedup(_ => 0, in => calls.update(_ + 1).as(in.map(_ * 10)))
            _     <- b.run(1)
            _     <- b.run(1)
            count <- calls.get
          yield assertTrue(count == 2)
      },
      test("batchSize caps the distinct keys handed to one call") {
        ZIO.scoped:
          for
            seen    <- Ref.make(List.empty[List[Int]])
            entered <- Promise.make[Nothing, Unit]
            gate    <- Promise.make[Nothing, Unit]
            b       <- dedupSized(2, identity, recordingGatedLogic(seen, entered, gate))
            leader  <- b.run(0).fork
            _       <- entered.await                             // holds the first call open
            queued  <- ZIO.foreachPar((1 to 6).toList)(b.run).fork
            _       <- ZIO.yieldNow.repeatN(50)                   // six distinct keys pile up behind it
            _       <- gate.succeed(())
            _       <- leader.join
            out     <- queued.join
            batches <- seen.get
          yield assertTrue(
            batches.forall(_.size <= 2),
            batches.flatten.sorted == (0 to 6).toList,            // every key ran exactly once
            out == (1 to 6).map(_ * 10).toList,
          )
      },
      test("interrupting one sharer does not disturb the others") {
        // Duplicates share a promise, so an abandoned caller must not take its siblings' result with it.
        ZIO.scoped:
          for
            entered <- Promise.make[Nothing, Unit]
            gate    <- Promise.make[Nothing, Unit]
            b       <- dedup(_ => 0, gatedLogic(entered, gate))
            leader  <- b.run(1).fork
            _       <- entered.await
            sharer  <- b.run(2).fork
            _       <- ZIO.yieldNow.repeatN(20)
            _       <- sharer.interrupt                           // one duplicate walks away
            _       <- gate.succeed(())
            result  <- leader.join
          yield assertTrue(result == 10)
      },
    ),
    suite("distributed")(
      test("routes and returns each caller's result across shards") {
        ZIO.scoped:
          for
            b   <- Batcher.distributed(1024, 4, mapped)
            out <- ZIO.foreachPar((1 to 200).toList)(b.run)
          yield assertTrue(out == (1 to 200).map(_ * 10).toList)
      },
      test("over a deduplicating inner, keeps correct per-key results") {
        val keys                                             = 10
        val logic: Batcher.Logic[Nothing, Nothing, Int, Int] = in => ZIO.succeed(in.map(_ % keys))
        ZIO.scoped:
          for
            b   <- Batcher.distributed(1024, 4, (i: Int) => i % keys, logic)
            out <- ZIO.foreachPar((1 to 300).toList)(i => b.run(i).map(i -> _))
          yield assertTrue(out.forall((i, r) => r == i % keys))
      },
      test("rejects parallelism < 1") {
        ZIO
          .scoped(Batcher.distributed(1024, 0, mapped))
          .either
          .map(e => assertTrue(e == Left(Batcher.InvalidParallelism(0))))
      },
    ),
    suite("adaptive")(
      test("below the threshold, every call runs directly — no coalescing") {
        for
          calls   <- Ref.make(0)
          maxSize <- Ref.make(0)
          out     <- ZIO.scoped:
                       Batcher
                         .adaptive(100, Batcher.serial(1024, countingSleepLogic(calls, maxSize)))
                         .flatMap(b => ZIO.foreachPar((1 to 10).toList)(b.run))
          c       <- calls.get
          m       <- maxSize.get
        yield assertTrue(out == (1 to 10).map(_ * 10).toList, c == 10, m == 1)
      },
      test("at or above the threshold, calls coalesce through the batcher") {
        for
          calls   <- Ref.make(0)
          maxSize <- Ref.make(0)
          out     <- ZIO.scoped:
                       Batcher
                         .adaptive(2, Batcher.serial(1024, countingSleepLogic(calls, maxSize)))
                         .flatMap(b => ZIO.foreachPar((1 to 50).toList)(b.run))
          c       <- calls.get
          m       <- maxSize.get
        yield assertTrue(out == (1 to 50).map(_ * 10).toList, c < 50, m > 1)
      },
    ),
    test("keyed loader: fetched keys resolve, missing keys become notFound") {
      val store = Map(1 -> "one", 2 -> "two", 3 -> "three")

      val fetch: NonEmptyChunk[Int] => UIO[Map[Int, String]] =
        keys => ZIO.succeed(keys.toChunk.flatMap(k => store.get(k).map(k -> _)).toMap)

      ZIO.scoped:
        for
          b   <- Batcher.keyed(1024, fetch, (_: Int) => Boom)
          out <- ZIO.foreachPar((1 to 5).toList)(i => b.run(i).either.map(i -> _))
        yield assertTrue(out.forall((i, r) => r == store.get(i).toRight(Boom)))
    },
    test("the layers compose: adaptive over distributed over deduplicated") {
      val keys                                             = 10
      val logic: Batcher.Logic[Nothing, Nothing, Int, Int] = in => ZIO.succeed(in.map(_ % keys))
      ZIO.scoped:
        for
          b   <- Batcher.adaptive(4, Batcher.distributed(1024, 4, (i: Int) => i % keys, logic))
          out <- ZIO.foreachPar((1 to 300).toList)(i => b.run(i).map(i -> _))
        yield assertTrue(out.forall((i, r) => r == i % keys))
    },
  ) @@ TestAspect.withLiveClock @@ TestAspect.timeout(60.seconds)
