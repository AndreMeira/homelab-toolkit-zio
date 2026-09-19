package homelab.common.processing


import homelab.common.messaging.Consumer
import zio.*
import zio.test.*


/**
 * The two properties of [[Processor.parallel]] that nothing else observes: that it reaches its parallelism,
 * and that its spawn loop is bounded.
 *
 * Both failure modes are silent. A listener loop that never forks passes every other test in this module
 * while capping concurrency at one, and a spawn loop with nothing to park on looks correct at a small
 * parallelism because the semaphore hides it.
 */
object ProcessorParallelSpec extends ZIOSpecDefault:

  private val parallelism = 8

  def spec: Spec[TestEnvironment & Scope, Any] = suite("Processor.parallel")(
    test("handles run concurrently, up to the parallelism") {
      // Fails at 1 if the listener is a single looping fiber rather than one forked per value.
      for
        live <- Ref.make(0)
        peak <- Ref.make(0)
        _    <- ZIO.scoped(Processor.parallel(always(1), parallelism)(hold(live, peak)).forkScoped *> settle)
        most <- peak.get
      yield assertTrue(most == parallelism)
    },
    test("a silent intake leaves exactly one listener waiting") {
      // Fails unbounded if the spawn loop has nothing to park on: it forks a listener per iteration
      // whether or not the previous one ever received a value.
      for
        entered <- Ref.make(0)
        _       <- ZIO.scoped(Processor.parallel(silent(entered), 50_000)(_ => ZIO.unit).forkScoped *> settle)
        count   <- entered.get
      yield assertTrue(count == 1)
    },
  ) @@ TestAspect.withLiveClock @@ TestAspect.timeout(1.minute)

  /** Long enough for the loop to reach its steady state, short enough to keep the suite quick. */
  private val settle: UIO[Unit] = ZIO.sleep(300.millis)

  /**
   * An intake with a value always ready.
   *
   * @param value what every consume hands over
   * @return the consumer
   */
  private def always(value: Int): Consumer[Nothing, Int] = new Consumer[Nothing, Int]:
    override def consume[E2](logic: Int => IO[E2, Unit]): IO[E2, Unit] = logic(value)

  /**
   * An intake that never yields, counting the listeners that reach it.
   *
   * @param entered counts arrivals
   * @return the consumer
   */
  private def silent(entered: Ref[Int]): Consumer[Nothing, Int] = new Consumer[Nothing, Int]:
    override def consume[E2](logic: Int => IO[E2, Unit]): IO[E2, Unit] = entered.update(_ + 1) *> ZIO.never

  /**
   * Occupy a slot long enough to overlap with its peers, recording the high-water mark.
   *
   * @param live how many handles are running now
   * @param peak the most seen at once
   * @return the handler
   */
  private def hold(live: Ref[Int], peak: Ref[Int]): Int => UIO[Unit] =
    _ =>
      live.updateAndGet(_ + 1).flatMap(running => peak.update(_ max running))
        *> ZIO.sleep(50.millis)
        *> live.update(_ - 1)
