package homelab.inmemory.processing


import homelab.common.processing.Workflow
import homelab.common.processing.Workflow.Step
import homelab.inmemory.store.InMemoryKeyValueStore
import zio.*
import zio.test.*


// End-to-end spec for the Workflow stepper and its Runners over the in-memory KeyValueStore. The Inline path
// steps to completion in memory; the Default path checkpoints each produced state under
// (workflow name, encoded input), resumes from the last checkpoint after a crash (replaying the failed
// step), deletes the checkpoint on completion, and surfaces a corrupt checkpoint as an AdapterError. A
// per-suite timeout turns any hang into a failure rather than blocking the run.
object WorkflowSpec extends ZIOSpecDefault:

  private given Workflow.Serde[Int] with
    def encode(value: Int): String                   = value.toString
    def decode(encoded: String): Either[String, Int] = encoded.toIntOption.toRight(s"not an Int: $encoded")

  private given Workflow.Serde[String] with
    def encode(value: String): String                   = value
    def decode(encoded: String): Either[String, String] = Right(encoded)

  // A counter keyed by a run id: seed to 0, increment until `target`, then finish with the count reached.
  private def counter(target: Int): Workflow[Any, Nothing, String, Int, Int] =
    Workflow.make("counter") {
      case Step.Init(_)     => ZIO.succeed(Step.Continue(0))
      case Step.Continue(n) => ZIO.succeed(if n >= target then Step.Done(n) else Step.Continue(n + 1))
    }

  def spec = suite("Workflow")(
    test("in-memory run steps Init → Continue* → Done and returns the output") {
      counter(3).run("run").map(out => assertTrue(out == 3))
    },
    test("Runner.Default runs to completion, returns the output, and deletes the checkpoint") {
      ZIO.scoped {
        for
          store  <- InMemoryKeyValueStore.make[(String, String), String]
          runner <- Workflow.Runner.make(store)
          out    <- runner.run(counter(3), "run")
          left   <- store.get(("counter", "run"))
        yield assertTrue(out == 3, left.isEmpty) // seeded at 0, stepped to 3; checkpoint gone on completion
      }
    },
    test("Runner.Default resumes from the last checkpoint after a failure, replaying the failed step") {
      val boom = new RuntimeException("boom")
      ZIO.scoped {
        for
          store   <- InMemoryKeyValueStore.make[(String, String), String]
          seen    <- Ref.make(List.empty[Int])
          tripped <- Ref.make(false)
          wf       = Workflow.make[Any, RuntimeException, String, Int, Int]("counter") {
                       case Step.Init(_)     => ZIO.succeed(Step.Continue(0))
                       case Step.Continue(n) =>
                         seen.update(_ :+ n) *> {
                           if n == 2 then
                             tripped.getAndSet(true).flatMap {
                               case false => ZIO.fail(boom)                    // crash the first time at n == 2
                               case true  => ZIO.succeed(Step.Continue(n + 1)) // succeed on resume
                             }
                           else if n < 4 then ZIO.succeed(Step.Continue(n + 1))
                           else ZIO.succeed(Step.Done(n))
                         }
                     }
          runner  <- Workflow.Runner.make(store)
          exit1   <- runner.run(wf, "run").exit
          mid     <- store.get(("counter", "run")) // checkpoint left behind by the crash
          out2    <- runner.run(wf, "run")
          after   <- store.get(("counter", "run"))
          trace   <- seen.get
        yield assertTrue(
          exit1.isFailure,                    // the first run crashed…
          mid == Some("2"),                   // …leaving state 2 checkpointed (the state that failed to step)
          out2 == 4,                          // the resumed run finished
          after.isEmpty,                      // checkpoint deleted on completion
          trace == List(0, 1, 2, 2, 3, 4),    // resumed from 2 (not re-seeded) and replayed the failed step
        )
      }
    },
    test("Runner.Default surfaces a corrupt checkpoint as an AdapterError") {
      ZIO.scoped {
        for
          store  <- InMemoryKeyValueStore.make[(String, String), String]
          _      <- store.set(("counter", "run"), "not-a-number")
          runner <- Workflow.Runner.make(store)
          exit   <- runner.run(counter(3), "run").exit
        yield assertTrue(exit.isFailure) // decode of "not-a-number" fails → DecodingError & AdapterError
      }
    },
    test("distinct inputs run independently under their own key") {
      ZIO.scoped {
        for
          store  <- InMemoryKeyValueStore.make[(String, String), String]
          runner <- Workflow.Runner.make(store)
          a      <- runner.run(counter(3), "a")
          b      <- runner.run(counter(3), "b")
          left   <- store.get(("counter", "a")) <*> store.get(("counter", "b"))
        yield assertTrue(a == 3, b == 3, left == (None, None))
      }
    },
    test("a step may restart the workflow with Init, re-seeding its state") {
      for
        restarted <- Ref.make(false)
        wf         = Workflow.make[Any, Nothing, String, Int, Int]("restart") {
                       case Step.Init(_)     => ZIO.succeed(Step.Continue(0))
                       case Step.Continue(n) =>
                         if n == 2 then
                           restarted.getAndSet(true).map {
                             case false => Step.Init("again") // restart once at n == 2, re-seeding to 0
                             case true  => Step.Continue(n + 1)
                           }
                         else if n < 5 then ZIO.succeed(Step.Continue(n + 1))
                         else ZIO.succeed(Step.Done(n))
                     }
        out       <- wf.run("start")
      yield assertTrue(out == 5) // 0→1→2 (restart), 0→1→2→3→4→5, done at 5
    },
  ) @@ TestAspect.timeout(60.seconds)
