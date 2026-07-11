package homelab.incubator.flow.v2


import homelab.common.data.Batch
import homelab.common.data.Batch.LineageMismatch
import zio.*
import zio.test.*


// Rough microbenchmark (NOT a correctness gate): how much does Serial's machinery — a Promise, a couple of
// Ref.modify, a fiber fork, and the await/fulfil handoff — add on top of a raw `logic.run(Batch.single)` for
// a single uncontended call? Printed, not asserted: wall-clock timings are noisy, especially on CI.
object SerialBenchSpec extends ZIOSpecDefault:

  // BE = Nothing: `input.map` preserves lineage, so the batcher's verifyLineage always passes.
  private val fastLogic: Batcher.Logic[Any, Nothing, Nothing, Int, Int] =
    input => ZIO.succeed(input.map(i => i))

  private val slowLogic: Batcher.Logic[Any, Nothing, Nothing, Int, Int] =
    input => ZIO.sleep(1.millis).as(input.map(i => i))

  // A downstream that handles one call at a time (rate-limited API / single connection): each `logic.run`
  // takes ~1ms under the gate, and `calls` counts how many physical downstream calls actually happen.
  private def serialisedLogic(gate: Semaphore, calls: Ref[Int]): Batcher.Logic[Any, Nothing, Nothing, Int, Int] =
    input =>
      for
        _ <- calls.update(_ + 1)
        _ <- gate.withPermit(ZIO.sleep(1.millis))
      yield input.map(i => i)

  // Counts total items handed to the downstream (summed batch sizes) and returns each item's key (`_ % keys`),
  // so a caller of key k always gets k — dedup is result-preserving here.
  private def countingLogic(items: Ref[Int], keys: Int): Batcher.Logic[Any, Nothing, Nothing, Int, Int] =
    input => items.update(_ + input.values.size) *> ZIO.sleep(1.millis).as(input.map(_ % keys))

  // A downstream whose cost scales with batch size: `perItemMicros` of work per item in the batch. This is
  // where dedup (fewer items) turns into wall-clock, unlike a fixed per-call cost.
  private def perItemLogic(items: Ref[Int], keys: Int, perItemMicros: Long): Batcher.Logic[Any, Nothing, Nothing, Int, Int] =
    input =>
      val size = input.values.size
      items.update(_ + size) *> ZIO.sleep((perItemMicros * size).micros).as(input.map(_ % keys))

  // Tracks total items handed downstream (≈ promises allocated — one representative per drained slot) and the
  // largest single batch (≈ peak live state). A fixed 1ms per batch lets duplicates accumulate.
  private def hotKeyLogic(maxBatch: Ref[Int], items: Ref[Int]): Batcher.Logic[Any, Nothing, Nothing, Int, Int] =
    input =>
      val size = input.values.size
      maxBatch.update(_ max size) *> items.update(_ + size) *> ZIO.sleep(1.millis).as(input.map(_ => 0))

  /** Average wall-clock cost of `op`, in microseconds per call, over `n` runs. */
  private def measure[E](n: Int)(op: ZIO[Any, E, Any]): ZIO[Any, E, Double] =
    for
      start <- Clock.nanoTime
      _     <- op.repeatN(n - 1)
      end   <- Clock.nanoTime
    yield (end - start).toDouble / n / 1000.0

  private def bench(label: String, logic: Batcher.Logic[Any, Nothing, Nothing, Int, Int], warmup: Int, n: Int) =
    ZIO.scoped:
      for
        scope   <- ZIO.scope
        ref     <- Ref.make[Serial.State[LineageMismatch, Int, Int]](Serial.State.Idle[LineageMismatch, Int, Int]())
        batcher  = Serial[Any, Nothing, Nothing, Int, Int](1, scope, ref, logic)
        rawOp    = logic.run(Batch.single(7)).map(_.values.head)
        batOp    = batcher.run(7)
        _       <- rawOp.repeatN(warmup - 1) *> batOp.repeatN(warmup - 1) // JIT + alloc warmup
        raw     <- measure(n)(rawOp)
        bat     <- measure(n)(batOp)
        _       <- ZIO.debug(f"[$label] raw=$raw%8.3f µs/call   batcher=$bat%8.3f µs/call   overhead=${bat - raw}%8.3f µs")
      yield ()

  def spec = suite("Serial rough single-call overhead")(
    test("prints raw vs batcher per-call cost for ~0ms and 1ms logic; batcher stays correct") {
      for
        out <- ZIO.scoped:
                 for
                   scope <- ZIO.scope
                   ref   <- Ref.make[Serial.State[LineageMismatch, Int, Int]](Serial.State.Idle[LineageMismatch, Int, Int]())
                   b      = Serial[Any, Nothing, Nothing, Int, Int](1, scope, ref, fastLogic)
                   out   <- b.run(7)
                 yield out
        _   <- bench("~0ms logic", fastLogic, warmup = 2000, n = 20000)
        _   <- bench(" 1ms logic", slowLogic, warmup = 30, n = 100)
      yield assertTrue(out == 7)
    } @@ TestAspect.withLiveClock,
    test("N concurrent calls: the batcher coalesces to far fewer downstream calls") {
      val n     = 500
      val input = (1 to n).toList
      ZIO.scoped:
        for
          // No-batcher baseline: every request is its own downstream call.
          rawCalls <- Ref.make(0)
          rawGate  <- Semaphore.make(1)
          rawLogic  = serialisedLogic(rawGate, rawCalls)
          rawStart <- Clock.nanoTime
          _        <- ZIO.foreachPar(input)(i => rawLogic.run(Batch.single(i)))
          rawEnd   <- Clock.nanoTime
          rawC     <- rawCalls.get
          // The batcher: N concurrent run(i), coalesced by the drain.
          batCalls <- Ref.make(0)
          batGate  <- Semaphore.make(1)
          scope    <- ZIO.scope
          ref      <- Ref.make[Serial.State[LineageMismatch, Int, Int]](Serial.State.Idle[LineageMismatch, Int, Int]())
          batcher   = Serial[Any, Nothing, Nothing, Int, Int](1024, scope, ref, serialisedLogic(batGate, batCalls))
          batStart <- Clock.nanoTime
          results  <- ZIO.foreachPar(input)(i => batcher.run(i))
          batEnd   <- Clock.nanoTime
          batC     <- batCalls.get
          rawMs     = (rawEnd - rawStart).toDouble / 1e6
          batMs     = (batEnd - batStart).toDouble / 1e6
          _        <- ZIO.debug(
                        f"N=$n  no-batcher: $rawMs%7.1f ms / $rawC%4d calls    batcher: $batMs%7.1f ms / $batC%2d calls" +
                          f"    → ${rawMs / batMs}%.0f× faster, ${rawC.toDouble / batC}%.0f× fewer calls"
                      )
        yield assertTrue(results == input, rawC == n, batC <= 10) // correct per-caller routing + real coalescing
    } @@ TestAspect.withLiveClock,
    test("N concurrent over few keys: dedup collapses duplicate work") {
      val n     = 500
      val keys  = 10
      val input = (1 to n).toList
      val want  = input.map(_ % keys)
      ZIO.scoped:
        for
          // Serial: no dedup, so every request is its own item in the batch.
          serItems <- Ref.make(0)
          serScope <- ZIO.scope
          serRef   <- Ref.make[Serial.State[LineageMismatch, Int, Int]](Serial.State.Idle[LineageMismatch, Int, Int]())
          serial    = Serial[Any, Nothing, Nothing, Int, Int](1024, serScope, serRef, countingLogic(serItems, keys))
          serRes   <- ZIO.foreachPar(input)(serial.run)
          serI     <- serItems.get
          // SerialDeduplicated: duplicate keys collapse to one downstream item.
          dedItems <- Ref.make(0)
          dedScope <- ZIO.scope
          dedRef   <- Ref.make[SerialDeduplicated.State[Int, LineageMismatch, Int, Int]](
                        SerialDeduplicated.State.Idle[Int, LineageMismatch, Int, Int]()
                      )
          dedup     = SerialDeduplicated[Any, Nothing, Nothing, Int, Int, Int](1024, dedScope, dedRef, _ % keys, countingLogic(dedItems, keys))
          dedRes   <- ZIO.foreachPar(input)(dedup.run)
          dedI     <- dedItems.get
          _        <- ZIO.debug(
                        f"N=$n over $keys keys   serial: $serI%4d items    dedup: $dedI%3d items    → ${serI.toDouble / dedI}%.0f× less downstream work"
                      )
        // dedI ends up above `keys` because dedup is pending-only: a key drained in an early batch is
        // recomputed when its duplicates arrive during that batch's latency. Still a large collapse.
        yield assertTrue(serRes == want, dedRes == want, serI == n, dedI < n / 2)
    } @@ TestAspect.withLiveClock,
    test("N concurrent over few keys with per-item downstream cost: dedup wins on wall-clock") {
      val n       = 500
      val keys    = 10
      val perItem = 500L // µs of downstream work per item in a batch
      val input   = (1 to n).toList
      val want    = input.map(_ % keys)
      ZIO.scoped:
        for
          serItems <- Ref.make(0)
          serScope <- ZIO.scope
          serRef   <- Ref.make[Serial.State[LineageMismatch, Int, Int]](Serial.State.Idle[LineageMismatch, Int, Int]())
          serial    = Serial[Any, Nothing, Nothing, Int, Int](1024, serScope, serRef, perItemLogic(serItems, keys, perItem))
          serStart <- Clock.nanoTime
          serRes   <- ZIO.foreachPar(input)(serial.run)
          serEnd   <- Clock.nanoTime
          serI     <- serItems.get
          dedItems <- Ref.make(0)
          dedScope <- ZIO.scope
          dedRef   <- Ref.make[SerialDeduplicated.State[Int, LineageMismatch, Int, Int]](
                        SerialDeduplicated.State.Idle[Int, LineageMismatch, Int, Int]()
                      )
          dedup     = SerialDeduplicated[Any, Nothing, Nothing, Int, Int, Int](1024, dedScope, dedRef, _ % keys, perItemLogic(dedItems, keys, perItem))
          dedStart <- Clock.nanoTime
          dedRes   <- ZIO.foreachPar(input)(dedup.run)
          dedEnd   <- Clock.nanoTime
          dedI     <- dedItems.get
          serMs     = (serEnd - serStart).toDouble / 1e6
          dedMs     = (dedEnd - dedStart).toDouble / 1e6
          _        <- ZIO.debug(
                        f"N=$n over $keys keys, $perItem%dµs/item   serial: $serMs%7.1f ms ($serI items)    dedup: $dedMs%6.1f ms ($dedI items)    → ${serMs / dedMs}%.1f× faster"
                      )
        yield assertTrue(serRes == want, dedRes == want, dedMs < serMs)
    } @@ TestAspect.withLiveClock,
    test("one hot key, N duplicates: dedup holds O(1) state + allocations where Serial holds O(N)") {
      val n     = 5000
      val input = (1 to n).toList
      ZIO.scoped:
        for
          serMax   <- Ref.make(0)
          serItems <- Ref.make(0)
          serScope <- ZIO.scope
          serRef   <- Ref.make[Serial.State[LineageMismatch, Int, Int]](Serial.State.Idle[LineageMismatch, Int, Int]())
          serial    = Serial[Any, Nothing, Nothing, Int, Int](Int.MaxValue, serScope, serRef, hotKeyLogic(serMax, serItems))
          serRes   <- ZIO.foreachPar(input)(_ => serial.run(1))
          serM     <- serMax.get
          serI     <- serItems.get
          dedMax   <- Ref.make(0)
          dedItems <- Ref.make(0)
          dedScope <- ZIO.scope
          dedRef   <- Ref.make[SerialDeduplicated.State[Int, LineageMismatch, Int, Int]](
                        SerialDeduplicated.State.Idle[Int, LineageMismatch, Int, Int]()
                      )
          dedup     = SerialDeduplicated[Any, Nothing, Nothing, Int, Int, Int](Int.MaxValue, dedScope, dedRef, _ => 0, hotKeyLogic(dedMax, dedItems))
          dedRes   <- ZIO.foreachPar(input)(_ => dedup.run(1))
          dedM     <- dedMax.get
          dedI     <- dedItems.get
          _        <- ZIO.debug(
                        f"$n dups / 1 key   serial: peak batch $serM%4d, ~$serI%4d promises    dedup: peak batch $dedM%2d, ~$dedI%2d promises" +
                          f"    → ${serI.toDouble / dedI}%.0f× fewer allocations, ${serM.toDouble / dedM}%.0f× smaller peak state"
                      )
        yield assertTrue(serRes.forall(_ == 0), dedRes.forall(_ == 0), dedM < serM, dedI < serI)
    } @@ TestAspect.withLiveClock
  ) @@ TestAspect.ifPropSet("benchmarks") // opt-in: `sbt -Dbenchmarks=true …`; skipped in CI (timing-based)
