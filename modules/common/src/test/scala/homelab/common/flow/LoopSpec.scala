package homelab.common.flow

import zio.*
import zio.test.*

object LoopSpec extends ZIOSpecDefault:

  // A tiny service, only to probe whether `R` is inferred from an environmental step.
  trait Counter:
    def incr: UIO[Unit]
    def value: UIO[Int]
    
  object Counter:
    val layer: ULayer[Counter] =
      ZLayer.fromZIO(Ref.make(0).map { ref =>
        new Counter:
          def incr  = ref.update(_ + 1)
          def value = ref.get
      })

  // NOTE: every `Loop(...)` below is written WITHOUT type annotations on purpose — if it compiles,
  // pure inference handled the signature. The `val _: ExpectedType = …` lines assert *what* it inferred.
  def spec = suite("Loop")(
    test("infers S and O from continue/done, counting to a bound") {
      val counted = Loop(0) { i =>
        if i < 10 then ZIO.succeed(Loop.continue(i + 1))
        else ZIO.succeed(Loop.done(i))
      }
      val _: ZIO[Any, Nothing, Int] = counted // O inferred as Int from the two branches (covariant join)
      counted.map(result => assertTrue(result == 10))
    },
    test("infers when the result O differs from the state S") {
      val summed = Loop((List(1, 2, 3, 4, 5), 0)) { case (remaining, acc) =>
        remaining match
          case Nil    => ZIO.succeed(Loop.done(acc))
          case h :: t => ZIO.succeed(Loop.continue((t, acc + h)))
      }
      val _: ZIO[Any, Nothing, Int] = summed // S = (List[Int], Int), O = Int
      summed.map(sum => assertTrue(sum == 15))
    },
    test("infers E from a failing step (O collapses to Nothing with no done branch)") {
      val mayFail = Loop(0) { i =>
        if i == 3 then ZIO.fail("boom")
        else ZIO.succeed(Loop.continue(i + 1))
      }
      val _: ZIO[Any, String, Nothing] = mayFail // E = String; no `done`, so O = Nothing
      mayFail.either.map(r => assertTrue(r == Left("boom")))
    },
    test("infers R from an environmental step") {
      val envLoop = Loop(0) { i =>
        if i < 3 then ZIO.serviceWithZIO[Counter](_.incr).as(Loop.continue(i + 1))
        else ZIO.succeed(Loop.done(i))
      }
      val _: ZIO[Counter, Nothing, Int] = envLoop // R inferred as Counter (contravariant intersection)
      (for
        result <- envLoop
        count  <- ZIO.serviceWithZIO[Counter](_.value)
      yield assertTrue(result == 3, count == 3)).provide(Counter.layer)
    },
  )
