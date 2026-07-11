package homelab.incubator.flow.v2


import homelab.common.data.Batch
import homelab.common.data.Batch.LineageMismatch
import homelab.common.error.ApplicationError
import zio.*
import zio.test.*


// Correctness spec for the v2 batchers — pins the guarantees (no-hang, per-caller routing, failure isolation,
// interruption safety) that the v3 engine refactor must preserve. Deterministic (promise-gated, no sleeps);
// a per-suite timeout turns any hang into a failure instead of blocking the run.
object BatcherSpec extends ZIOSpecDefault:

  case object Boom extends ApplicationError:
    override def message: String = "boom"

  private def serial[E, BE](
    logic: Batcher.Logic[Any, E, BE, Int, Int],
    batchSize: Int = 1024,
  ): ZIO[Scope, Nothing, Batcher[Any, LineageMismatch | E | BE, Int, Int]] =
    for
      scope <- ZIO.scope
      ref   <- Ref.make[Serial.State[LineageMismatch | E | BE, Int, Int]](Serial.State.Idle[LineageMismatch | E | BE, Int, Int]())
    yield Serial[Any, E, BE, Int, Int](batchSize, scope, ref, logic)

  private def dedup[E, BE](
    key: Int => Int,
    logic: Batcher.Logic[Any, E, BE, Int, Int],
    batchSize: Int = 1024,
  ): ZIO[Scope, Nothing, Batcher[Any, LineageMismatch | E | BE, Int, Int]] =
    for
      scope <- ZIO.scope
      ref   <- Ref.make[SerialDeduplicated.State[Int, LineageMismatch | E | BE, Int, Int]](
                 SerialDeduplicated.State.Idle[Int, LineageMismatch | E | BE, Int, Int]()
               )
    yield SerialDeduplicated[Any, E, BE, Int, Int, Int](batchSize, scope, ref, key, logic)

  // Blocks the first (and only, when instant) drain in `logic` until `gate` is released, signalling `entered`
  // so a test can act while a request is genuinely in flight.
  private def gatedLogic(entered: Promise[Nothing, Unit], gate: Promise[Nothing, Unit]): Batcher.Logic[Any, Nothing, Nothing, Int, Int] =
    in => entered.succeed(()) *> gate.await.as(in.map(_ * 10))

  private val mapped: Batcher.Logic[Any, Nothing, Nothing, Int, Int]      = in => ZIO.succeed(in.map(_ * 10))
  private val failsBatch: Batcher.Logic[Any, Boom.type, Nothing, Int, Int] = _ => ZIO.fail(Boom)

  def spec = suite("Batcher correctness")(
    suite("Serial")(
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
        val perItem: Batcher.Logic[Any, Nothing, Boom.type, Int, Int] =
          in => ZIO.succeed(in.mapEither(i => if i % 2 == 0 then Right(i * 10) else Left(Boom)))
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
            outs <- ZIO.foreach((1 to 20).toList)(b.run) // sequential: each call is a fresh leader
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
                         _      <- entered.await    // A's request is in flight in the drain
                         _      <- fiberA.interrupt // interrupt the caller
                         _      <- gate.succeed(()) // release; the drain finishes A's batch and goes idle
                         bOut   <- b.run(2)         // batcher still usable
                       yield bOut
        yield assertTrue(out == 20)
      },
    ),
    suite("SerialDeduplicated")(
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
        val keys = 10
        val perKey: Batcher.Logic[Any, Nothing, Boom.type, Int, Int] =
          in => ZIO.succeed(in.mapEither(i => if i % keys < 5 then Right(i % keys) else Left(Boom)))
        ZIO.scoped:
          for
            b   <- dedup(_ % keys, perKey)
            out <- ZIO.foreachPar((1 to 300).toList)(i => b.run(i).either.map(i -> _))
          yield assertTrue(out.forall((i, r) => r == (if i % keys < 5 then Right(i % keys) else Left(Boom))))
      },
    ),
  ) @@ TestAspect.withLiveClock @@ TestAspect.timeout(60.seconds)
