package homelab.common.flow


import zio.*
import zio.test.*


/**
 * What a recursion promises: it stops where the caller's question says, a step's own handlers decide the
 * fate of what that step holds, and neither depth nor a parked step costs the caller its ability to stop.
 */
object RecursionSpec extends ZIOSpecDefault:
  import Support.*

  def spec: Spec[TestEnvironment & Scope, Any] = suite("Recursion")(
    suite("following")(
      test("a driven recursion follows the step it was made with") {
        val counting = Recursion(0)(n => ZIO.succeed(n + 1))
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
          from    = Recursion(0)(n => steps.update(_ + 1).as(n + 1))
          answer <- Recursion.run(from) { case 0 => "already there" }
          taken  <- steps.get
        yield assertTrue(answer == "already there", taken == 0)
      },
      test("a failing step aborts the run") {
        val breaks = Recursion(0)(n => if n == 3 then ZIO.fail("broke at 3") else ZIO.succeed(n + 1))
        Recursion.run(breaks) { case 10 => "ten" }.either.map(result => assertTrue(result == Left("broke at 3")))
      },
      test("depth costs no stack") {
        val counting = Recursion(0)(n => ZIO.succeed(n + 1))
        Recursion.run(counting) { case 100000 => "deep" }.map(answer => assertTrue(answer == "deep"))
      },
    ),
    suite("interruption")(
      test("a parked step can be interrupted, at depth and not only first") {
        for
          parked <- Promise.make[Nothing, Unit]
          fiber  <- Recursion(0) {
                      case n if n < 10 => ZIO.succeed(n + 1)
                      case n           => parked.succeed(()) *> ZIO.never.as(n)
                    }.terminate {
                      case -1 => ()
                    }.fork
          _      <- parked.await // the tenth step has parked; no guess about how long that took
          ended  <- fiber.interrupt.timeout(2.seconds)
        yield assertTrue(ended.isDefined)
      },
      test("the step's own handlers run: what it holds is given back") {
        // The contract the discipline exists for — a step that took something releases it on the way out.
        for
          released <- Ref.make(false)
          parked   <- Promise.make[Nothing, Unit]
          holding   = Recursion(0): _ =>
                        (parked.succeed(()) *> ZIO.never.as(1)).onExit(_ => released.set(true))
          fiber    <- Recursion.run(holding) { case -1 => () }.fork
          _        <- parked.await
          _        <- fiber.interrupt
          gaveBack <- released.get
        yield assertTrue(gaveBack)
      },
      test("a cancel held by the step that reaches the answer ends the run interrupted: the answer is lost") {
        // The step is uninterruptible, so the cancel waits for it, then takes effect at the next interruptible
        // point. With no next step, that point is the run handing its answer back to an interruptible caller.
        for
          entered <- Promise.make[Nothing, Unit]
          gate    <- Promise.make[Nothing, Unit]
          step     = ZIO.uninterruptible(entered.succeed(()) *> gate.await.as(1))
          exit    <- cancelledDuring(entered, gate, Recursion(0)(_ => step).terminate { case 1 => "answer" })
        yield assertTrue(exit.isInterrupted)
      } @@ TestAspect.nonFlaky(50),
      test("the same holds for a reflective step") {
        for
          entered <- Promise.make[Nothing, Unit]
          gate    <- Promise.make[Nothing, Unit]
          start    = Masked(ZIO.uninterruptible(entered.succeed(()) *> gate.await.as(Done)))
          exit    <- cancelledDuring(entered, gate, start.terminate { case Done => "answer" })
        yield assertTrue(exit.isInterrupted)
      } @@ TestAspect.nonFlaky(50),
      test("a cancel held by a step that does not answer stops the run before the next step") {
        for
          entered <- Promise.make[Nothing, Unit]
          gate    <- Promise.make[Nothing, Unit]
          second  <- Ref.make(false)
          stepping = Recursion(0):
                       case 0 => ZIO.uninterruptible(entered.succeed(()) *> gate.await.as(1))
                       case _ => second.set(true).as(2)
          exit    <- cancelledDuring(entered, gate, stepping.terminate { case 2 => "answer" })
          started <- second.get
        yield assertTrue(exit.isInterrupted, !started)
      } @@ TestAspect.nonFlaky(50),
      test("every completed step's work stays done, and the run stops at the state the question recognises") {
        for
          completed <- Ref.make(0)
          stepping   = Recursion(0)(n => completed.update(_ + 1).as(n + 1))
          fiber     <- Recursion.run(stepping) { case 50 => "done" }.fork
          answer    <- fiber.join
          counted   <- completed.get
        yield assertTrue(answer == "done", counted == 50)
      },
      test("an enum map works") {
        enum State { case One, Two, Three }
        Recursion(State.One) {
          case State.One   => ZIO.succeed(State.Two)
          case State.Two   => ZIO.succeed(State.Three)
          case State.Three => ZIO.succeed(State.Three)
        }.terminate {
          case State.Three => "three"
        }.map { result =>
          assertTrue(result == "three")
        }
      },
    ),
  ) @@ TestAspect.withLiveClock @@ TestAspect.timeout(30.seconds)

  /** A state family carrying the step out of itself, and what its end states hold. */
  private object Support {

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

    def held: PartialFunction[Wait, Option[String]] =
      case Granted(hold) => Some(hold)
      case GaveUp        => None

    // A family of two reflective states, the first stepping to the second with whatever effect it is given.
    sealed trait Two extends Recursion.Reflective[Any, Nothing, Two]

    case class Masked(step: UIO[Two]) extends Two:
      def next: UIO[Two] = step

    case object Done extends Two:
      def next: UIO[Two] = ZIO.succeed(this)

    /**
     * Run `run`, cancel it while its step is held at `gate`, then let the step finish.
     *
     * @param entered completed by the step once it is running
     * @param gate what the step waits for before it finishes
     * @param run the run to cancel
     * @tparam A what the run answers with
     * @return how the run ended
     */
    def cancelledDuring[A](entered: Promise[Nothing, Unit], gate: Promise[Nothing, Unit], run: UIO[A]): UIO[Exit[Nothing, A]] =
      for
        fiber <- run.fork
        _     <- entered.await
        _     <- fiber.interruptFork
        _     <- gate.succeed(())
        exit  <- fiber.await
      yield exit
  }
