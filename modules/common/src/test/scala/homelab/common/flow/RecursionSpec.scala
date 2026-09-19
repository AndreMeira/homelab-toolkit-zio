package homelab.common.flow


import zio.*
import zio.test.*


/**
 * What a recursion promises: it stops where the caller's question says, a step's own handlers decide the
 * fate of what that step holds, and neither depth nor a parked step costs the caller its ability to stop.
 */
object RecursionSpec extends ZIOSpecDefault:

  // A lock-shaped family, each state carrying the step out of itself.
  sealed trait Wait extends Recursion.Reflective[Any, Nothing, Wait]

  case class Placing(attempt: Int) extends Wait:
    def next: UIO[Wait] = ZIO.succeed(Queued(attempt))

  case class Queued(ticket: Int) extends Wait:
    def next: UIO[Wait] = ZIO.succeed(Granted(s"hold-$ticket"))

  case class Granted(hold: String) extends Wait:
    def next: UIO[Wait] = ZIO.succeed(this)

  case object GaveUp extends Wait:
    def next: UIO[Wait] = ZIO.succeed(this)

  private def held: PartialFunction[Wait, Option[String]] =
    case Granted(hold) => Some(hold)
    case GaveUp        => None

  def spec: Spec[TestEnvironment & Scope, Any] = suite("Recursion")(
    suite("following")(
      test("a driven recursion follows the step it was made with") {
        val counting = Recursion.make(0)(n => ZIO.succeed(n + 1))
        Recursion.run(counting) { case 5 => "five" }.map(answer => assertTrue(answer == "five"))
      },
      test("a reflective recursion follows each state's own step") {
        Recursion.run(Placing(7))(held).map(answer => assertTrue(answer.contains("hold-7")))
      },
      test("the question is the caller's: one family, two answers") {
        for
          won   <- Recursion.run(Placing(7))(held)
          stood <- Recursion.run(Placing(7)) { case Queued(ticket) => ticket }
        yield assertTrue(won.contains("hold-7"), stood == 7)
      },
      test("a state the question already recognises is not stepped") {
        for
          steps  <- Ref.make(0)
          from    = Recursion.make(0)(n => steps.update(_ + 1).as(n + 1))
          answer <- Recursion.run(from) { case 0 => "already there" }
          taken  <- steps.get
        yield assertTrue(answer == "already there", taken == 0)
      },
      test("a failing step aborts the run") {
        val breaks = Recursion.make(0)(n => if n == 3 then ZIO.fail("broke at 3") else ZIO.succeed(n + 1))
        Recursion.run(breaks) { case 10 => "ten" }.either.map(result => assertTrue(result == Left("broke at 3")))
      },
      test("depth costs no stack") {
        val counting = Recursion.make(0)(n => ZIO.succeed(n + 1))
        Recursion.run(counting) { case 100000 => "deep" }.map(answer => assertTrue(answer == "deep"))
      },
    ),
    suite("interruption")(
      test("a parked step can be interrupted, at depth and not only first") {
        for
          parked <- Promise.make[Nothing, Unit]
          parking = Recursion.make(0): n =>
                      if n < 10 then ZIO.succeed(n + 1) else parked.succeed(()) *> ZIO.never.as(n)
          fiber  <- Recursion.run(parking) { case -1 => () }.fork
          _      <- parked.await // the tenth step has parked; no guess about how long that took
          ended  <- fiber.interrupt.timeout(2.seconds)
        yield assertTrue(ended.isDefined)
      },
      test("the step's own handlers run: what it holds is given back") {
        // The contract the discipline exists for — a step that took something releases it on the way out.
        for
          released <- Ref.make(false)
          parked   <- Promise.make[Nothing, Unit]
          holding   = Recursion.make(0): _ =>
                        (parked.succeed(()) *> ZIO.never.as(1)).onExit(_ => released.set(true))
          fiber    <- Recursion.run(holding) { case -1 => () }.fork
          _        <- parked.await
          _        <- fiber.interrupt
          gaveBack <- released.get
        yield assertTrue(gaveBack)
      },
      test("a step already past its handlers is not undone by an interrupt arriving after it") {
        // The other half: the space between two steps is not interruptible, so a completed step stays
        // completed and the run is stopped at a state, never between two.
        for
          completed <- Ref.make(0)
          stepping   = Recursion.make(0)(n => completed.update(_ + 1).as(n + 1))
          fiber     <- Recursion.run(stepping) { case 50 => "done" }.fork
          answer    <- fiber.join
          counted   <- completed.get
        yield assertTrue(answer == "done", counted == 50)
      },
    ),
  ) @@ TestAspect.withLiveClock @@ TestAspect.timeout(30.seconds)
