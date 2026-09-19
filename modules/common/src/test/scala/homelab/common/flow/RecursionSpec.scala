package homelab.common.flow


import zio.*
import zio.test.*


object RecursionSpec extends ZIOSpecDefault:

  // the reflective shape: a lock-shaped family, each state carrying its own step
  sealed trait Wait                extends Recursion.Reflective[Any, Nothing, Wait]
  case class Placing(attempt: Int) extends Wait:
    def next: UIO[Wait] = ZIO.succeed(Queued(attempt))
  case class Queued(ticket: Int)   extends Wait:
    def next: UIO[Wait] = ZIO.succeed(Granted(s"hold-$ticket"))
  case class Granted(hold: String) extends Wait:
    def next: UIO[Wait] = ZIO.succeed(this)
  case object GaveUp               extends Wait:
    def next: UIO[Wait] = ZIO.succeed(this)

  private def held: PartialFunction[Wait, Option[String]] =
    case Granted(hold) => Some(hold)
    case GaveUp        => None

  def spec: Spec[TestEnvironment & Scope, Any] = suite("Recursion")(
    test("plain: no annotation needed") {
      // what v4 could not do — inference lands on Any, not Nothing
      val machine = Recursion.make(0)(n => ZIO.succeed(n + 1))
      Recursion.run(machine) { case 5 => "five" }.map(a => assertTrue(a == "five"))
    },
    test("reflective: each state carries its own step") {
      Recursion.run(Placing(7))(held).map(a => assertTrue(a.contains("hold-7")))
    },
    test("the question is the caller's, not the family's") {
      Recursion.run(Placing(7)) { case Queued(t) => t }.map(t => assertTrue(t == 7))
    },
    test("a step is interruptible at depth, not only the first") {
      val parking = Recursion.make(0)(n => if n < 10 then ZIO.succeed(n + 1) else ZIO.sleep(1.hour).as(n))
      for
        fiber <- Recursion.run(parking) { case -1 => () }.fork
        _     <- ZIO.sleep(300.millis)
        exit  <- fiber.interrupt.timeout(3.seconds)
      yield assertTrue(exit.isDefined)
    },
  ) @@ TestAspect.withLiveClock @@ TestAspect.timeout(30.seconds)
