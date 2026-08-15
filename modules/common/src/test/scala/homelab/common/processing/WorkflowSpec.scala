package homelab.common.processing


import homelab.common.flow.KeyLock
import homelab.common.processing.Workflow.Step
import homelab.common.store.KeyValueStore
import homelab.common.store.inmemory.InMemoryKeyValueStore
import zio.*
import zio.test.*


// End-to-end spec for the Workflow stepper and the combinators that decorate it. A bare `run` steps to
// completion in memory; `persisted` checkpoints each produced native state under its input, resumes from the
// last checkpoint after a crash (replaying the failed step), and releases the slot on completion;
// `serialised` serialises runs per input. The store holds native `S` — no Serde — so there is no decode path.
// A per-suite timeout turns any hang into a failure rather than blocking the run.
object WorkflowSpec extends ZIOSpecDefault:

  private val boom = new RuntimeException("boom")

  // A counter keyed by a run id: seed to 0, increment until `target`, then finish with the count reached.
  private def counter(target: Int): Workflow[Any, Nothing, String, Int, Int] =
    Workflow.make("counter") {
      case Step.Init(_)     => Step.Continue.succeed(0)
      case Step.Continue(n) => if n >= target then Step.Done.succeed(n) else Step.Continue.succeed(n + 1)
    }

  // A printable tag per step kind, so a tap's trace pins both the order and the kind of what it observed.
  private def label(step: Step[String, Int, Int]): String = step match
    case Step.Init(input)     => s"init $input"
    case Step.Continue(state) => s"continue $state"
    case Step.Done(output)    => s"done $output"

  def spec: Spec[TestEnvironment & Scope, Any] = suite("Workflow")(
    test("in-memory run steps Init → Continue* → Done and returns the output") {
      counter(3).run("run").map(out => assertTrue(out == 3))
    },
    test("a step may restart the workflow with Init, re-seeding its state") {
      for
        restarted <- Ref.make(false)
        wf         = Workflow.make[Any, Nothing, String, Int, Int]("restart") {
                       case Step.Init(_)     => Step.Continue.succeed(0)
                       case Step.Continue(n) =>
                         if n == 2 then
                           restarted.getAndSet(true).map {
                             case false => Step.Init("again") // restart once at n == 2, re-seeding to 0
                             case true  => Step.Continue(n + 1)
                           }
                         else if n < 5 then Step.Continue.succeed(n + 1)
                         else Step.Done.succeed(n)
                     }
        out       <- wf.run("start")
      yield assertTrue(out == 5) // 0→1→2 (restart), 0→1→2→3→4→5, done at 5
    },
    suite("persisted")(
      test("runs to completion, returns the output, and deletes the checkpoint") {
        for
          store <- KeyValueStore.inmemory[String, Int]
          out   <- counter(3).persisted(store).run("run")
          left  <- store.get("run")
        yield assertTrue(out == 3, left.isEmpty)
      },
      test("resumes from the last checkpoint after a crash, replaying the failed step") {
        for
          store   <- KeyValueStore.inmemory[String, Int]
          seen    <- Ref.make(List.empty[Int])
          tripped <- Ref.make(false)
          wf       = Workflow.make[Any, RuntimeException, String, Int, Int]("counter") {
                       case Step.Init(_)     => Step.Continue.succeed(0)
                       case Step.Continue(n) =>
                         seen.update(_ :+ n) *> {
                           if n == 2 then
                             tripped.getAndSet(true).flatMap {
                               case false => ZIO.fail(boom)               // crash the first time at n == 2
                               case true  => Step.Continue.succeed(n + 1) // succeed on resume
                             }
                           else if n < 4 then Step.Continue.succeed(n + 1)
                           else Step.Done.succeed(n)
                         }
                     }
          durable  = wf.persisted(store)
          exit1   <- durable.run("run").exit
          mid     <- store.get("run") // checkpoint left behind by the crash
          out2    <- durable.run("run")
          after   <- store.get("run")
          trace   <- seen.get
        yield assertTrue(
          exit1.isFailure,
          mid.contains(2),                 // the state that failed to step, not the one before it
          out2 == 4,                       // the second run finished
          after.isEmpty,                   // slot released on completion
          trace == List(0, 1, 2, 2, 3, 4), // resumed from 2 rather than re-seeding, and replayed it
        )
      },
      test("keys each run by its own input, so distinct runs never share a slot") {
        for
          store <- KeyValueStore.inmemory[String, Int]
          wf     = counter(3).persisted(store)
          a     <- wf.run("a")
          b     <- wf.run("b")
          again <- wf.run("a") // a completed run left nothing behind, so this seeds afresh
          leftA <- store.get("a")
          leftB <- store.get("b")
        yield assertTrue(a == 3, b == 3, again == 3, leftA.isEmpty, leftB.isEmpty)
      },
      test("a restart keeps checkpointing under the original input") {
        for
          store     <- KeyValueStore.inmemory[String, Int]
          restarted <- Ref.make(false)
          wf         = Workflow.make[Any, Nothing, String, Int, Int]("restart") {
                         case Step.Init(_)     => Step.Continue.succeed(0)
                         case Step.Continue(n) =>
                           if n == 2 then
                             restarted.getAndSet(true).map {
                               case false => Step.Init("again") // re-seeds the state, NOT the slot
                               case true  => Step.Continue(n + 1)
                             }
                           else if n < 5 then Step.Continue.succeed(n + 1)
                           else Step.Done.succeed(n)
                       }
          out       <- wf.persisted(store).run("start")
          atStart   <- store.get("start")
          atAgain   <- store.get("again")
        yield assertTrue(out == 5, atStart.isEmpty, atAgain.isEmpty) // "again" was never a slot at all
      },
      test("a shared store namespaces by workflow name, so two workflows never collide on one input") {
        // What Runner.Default's composite (name, input) key used to do, now composed at the call site.
        def crashing(name: String): Workflow[Any, RuntimeException, String, Int, Int] =
          Workflow
            .make[Any, Nothing, String, Int, Int](name) {
              case Step.Init(_)     => Step.Continue.succeed(0)
              case Step.Continue(n) => if n >= 5 then Step.Done.succeed(n) else Step.Continue.succeed(n + 1)
            }
            .intercept {
              case Step.Continue(n) if n >= 2 => ZIO.fail(boom)
              case step                       => ZIO.succeed(step)
            }

        for
          store  <- KeyValueStore.inmemory[(String, String), Int]
          first   = crashing("first")
          second  = crashing("second")
          _      <- first.persisted(store.contramap(input => first.name -> input)).run("run").exit
          _      <- second.persisted(store.contramap(input => second.name -> input)).run("run").exit
          atOne  <- store.get(("first", "run"))
          atTwo  <- store.get(("second", "run"))
        yield assertTrue(atOne.contains(1), atTwo.contains(1)) // same input, two live checkpoints
      },
    ),
    suite("serialised")(
      test("runs of the same input never overlap") {
        for
          lock     <- KeyLock.make[String]
          inFlight <- Ref.make(0)
          peak     <- Ref.make(0)
          wf        = Workflow.make[Any, Nothing, String, Int, Int]("busy") {
                        case Step.Init(_)     => Step.Continue.succeed(0)
                        case Step.Continue(n) =>
                          inFlight.updateAndGet(_ + 1).flatMap(c => peak.update(_ max c)) *>
                            ZIO.yieldNow.repeatN(4) *> // give the other run every chance to interleave
                            inFlight.update(_ - 1) *>
                            (if n >= 2 then Step.Done.succeed(n) else Step.Continue.succeed(n + 1))
                      }
          guarded   = wf.serialised(lock)
          _        <- guarded.run("same") <&> guarded.run("same")
          observed <- peak.get
        yield assertTrue(observed == 1) // unguarded, the two runs interleave and this reaches 2
      },
      test("runs of distinct inputs still proceed concurrently") {
        for
          lock    <- KeyLock.make[String]
          both    <- Promise.make[Nothing, Unit]
          arrived <- Ref.make(0)
          wf       = Workflow.make[Any, Nothing, String, Int, Int]("gate") {
                       case Step.Init(_)     => Step.Continue.succeed(0)
                       case Step.Continue(n) =>
                         // Completes only if BOTH runs are inside a step at once — a global lock deadlocks here.
                         arrived.updateAndGet(_ + 1).flatMap(c => both.succeed(()).when(c == 2)) *>
                           both.await *> Step.Done.succeed(n)
                     }
          guarded  = wf.serialised(lock)
          _       <- guarded.run("a") <&> guarded.run("b")
          count   <- arrived.get
        yield assertTrue(count == 2)
      },
    ),
    suite("intercept")(
      test("maps a Done's output, retyping the workflow's result") {
        counter(3)
          .intercept[Any, Nothing, String] {
            case Step.Done(n)         => Step.Done.succeed(s"done at $n")
            case Step.Init(input)     => Step.Init.succeed(input)
            case Step.Continue(state) => Step.Continue.succeed(state)
          }
          .run("run")
          .map(out => assertTrue(out == "done at 3")) // O (Int) is consumed by fn, never propagated
      },
      test("ends a run early by rewriting a Continue into a Done") {
        counter(10)
          .intercept {
            case Step.Continue(n) if n >= 2 => Step.Done.succeed(n) // cut the loop short at 2…
            case step                       => ZIO.succeed(step)
          }
          .run("run")
          .map(out => assertTrue(out == 2)) // …so the workflow's own target of 10 is never reached
      },
      test("resurrects a finished run by rewriting a Done into a Continue") {
        for
          revived <- Ref.make(false)
          wf       = counter(2).intercept {
                       case Step.Done(n) =>
                         revived.getAndSet(true).map {
                           case false => Step.Continue(n + 1) // send the finished run back into the loop, once
                           case true  => Step.Done(n)
                         }
                       case step         => ZIO.succeed(step)
                     }
          out     <- wf.run("run")
        yield assertTrue(out == 3) // done at 2, resumed from 3, done again at 3
      },
      test("keeps the wrapped workflow's name, so an intercepted run shares its checkpoint slots") {
        for
          store   <- KeyValueStore.inmemory[String, Int]
          wrapped  = counter(5).intercept {
                       case Step.Continue(n) if n >= 2 => ZIO.fail(boom) // crash after two checkpoints
                       case step                       => ZIO.succeed(step)
                     }
          crashed <- wrapped.persisted(store).run("run").exit
          slot    <- store.get("run")
          out     <- counter(5).persisted(store).run("run") // the plain workflow resumes from the wrapper's state
        yield assertTrue(crashed.isFailure, slot.contains(1), out == 5)
      },
    ),
    suite("tap")(
      test("observes every produced step, in order, without changing the run") {
        for
          seen  <- Ref.make(List.empty[String])
          out   <- counter(2).tap(step => seen.update(_ :+ label(step))).run("run")
          trace <- seen.get
        yield assertTrue(
          out == 2, // the output is untouched — tap cannot retype or reroute
          trace == List("continue 0", "continue 1", "continue 2", "done 2"),
        )
      },
      test("a failing tap aborts the run at the step it observed") {
        for
          seen  <- Ref.make(List.empty[String])
          exit  <- counter(5)
                     .tap {
                       case Step.Continue(2) => ZIO.fail(boom) // an observer CAN break a run
                       case step             => seen.update(_ :+ label(step))
                     }
                     .run("run")
                     .exit
          trace <- seen.get
        yield assertTrue(exit == Exit.fail(boom), trace == List("continue 0", "continue 1"))
      },
    ),
  ) @@ TestAspect.timeout(60.seconds)
