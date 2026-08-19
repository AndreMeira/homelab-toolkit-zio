package homelab.common.processing


import zio.*
import zio.test.*


/**
 * A probe for the two ZIO behaviours [[PollConsumer.Fetcher.make]] depends on and neither of which is visible
 * in its code: a forked fiber inherits its parent's interrupt status, and scope finalizers run
 * last-registered-first. Together they are why the fork must sit in the `tap` rather than in the acquire.
 *
 * Each ordering test gates on the forked fiber having actually started. Without that the probe is racy for a
 * reason that has nothing to do with what it measures: a fiber interrupted before its first timeslice never
 * installed its `onInterrupt`, so it records nothing and the assertion fails under load.
 */
object ScopeOrderingSpec extends ZIOSpecDefault:

  /** Forks `ZIO.never`, recording `label` when interrupted, and completes once it is genuinely parked. */
  private def parked(order: Ref[List[String]], label: String, started: Promise[Nothing, Unit]) =
    (started.succeed(()) *> ZIO.never).onInterrupt(order.update(_ :+ label))

  def spec: Spec[TestEnvironment & Scope, Any] = suite("scope ordering")(
    test("a fiber forked inside an uninterruptible region inherits that status") {
      // `acquireRelease` runs its acquire uninterruptibly. A fiber forked there is born uninterruptible, so
      // nothing can ever stop it — `interrupt` waits forever, and a scope containing it never closes.
      for
        fiber   <- ZIO.uninterruptible(ZIO.never.fork)
        stopped <- fiber.interrupt.timeout(1.second)
      yield assertTrue(stopped.isEmpty)
    },
    test("restoring interruptibility for the forked body fixes it") {
      for
        fiber   <- ZIO.uninterruptible(ZIO.never.interruptible.fork)
        stopped <- fiber.interrupt.timeout(1.second)
      yield assertTrue(stopped.isDefined)
    },
    test("finalizers run last-registered-first") {
      for
        order <- Ref.make(List.empty[String])
        _     <- ZIO.scoped {
                   ZIO.acquireRelease(ZIO.unit)(_ => order.update(_ :+ "registered first"))
                     *> ZIO.acquireRelease(ZIO.unit)(_ => order.update(_ :+ "registered second"))
                 }
        seen  <- order.get
      yield assertTrue(seen == List("registered second", "registered first"))
    },
    test("rejected shape — forking inside acquire drains while the fetcher still runs") {
      // The fork registers its interrupt-on-close during the acquire, the acquireRelease registers the drain
      // afterwards, so LIFO runs the drain first. (`.interruptible` only so this can be observed at all —
      // without it the scope deadlocks, per the first test.)
      for
        order   <- Ref.make(List.empty[String])
        started <- Promise.make[Nothing, Unit]
        _       <- ZIO.scoped {
                     ZIO.acquireRelease(
                       parked(order, "fetcher stopped", started).interruptible.forkScoped
                     )(_ => order.update(_ :+ "drain ran")) *> started.await
                   }
        seen    <- order.get
      yield assertTrue(seen == List("drain ran", "fetcher stopped"))
    },
    test("adopted shape — a trivial acquire and a forked tap put the drain last") {
      // The acquire no longer forks, so the drain registers FIRST and the fetcher's interrupt second; LIFO
      // then stops the fetcher before draining. And because the fork happens in the tap, outside the
      // uninterruptible acquire, the fiber is interruptible without asking.
      for
        order   <- Ref.make(List.empty[String])
        started <- Promise.make[Nothing, Unit]
        _       <- ZIO.scoped {
                     ZIO
                       .acquireRelease(ZIO.succeed("fetcher"))(_ => order.update(_ :+ "drain ran"))
                       .tap(_ => parked(order, "fetcher stopped", started).forkScoped) *> started.await
                   }
        seen    <- order.get
      yield assertTrue(seen == List("fetcher stopped", "drain ran"))
    },
  ) @@ TestAspect.withLiveClock @@ TestAspect.timeout(20.seconds)
