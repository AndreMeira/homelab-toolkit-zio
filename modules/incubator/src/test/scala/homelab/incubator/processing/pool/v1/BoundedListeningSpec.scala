package homelab.incubator.processing.pool.v1


import homelab.common.messaging.Consumer
import zio.*
import zio.test.*


/**
 * The difference the semaphore's placement makes, made observable: with one permit and a handler that never
 * finishes, how many values has the intake handed out?
 */
object BoundedListeningSpec extends ZIOSpecDefault:

  /** An intake that records every value it hands over — standing in for a lease being taken. */
  private def recording(queue: Queue[Int], claimed: Ref[List[Int]]): Consumer[Nothing, Int] =
    new Consumer[Nothing, Int]:
      override def consume[E2 >: Nothing](logic: Int => IO[E2, Unit]): IO[E2, Unit] =
        queue.take.flatMap(value => claimed.update(_ :+ value) *> logic(value))

  private def claimsUnder(
    strategy: (Consumer[Nothing, Int], Int) => (Int => IO[Nothing, Unit]) => ZIO[Scope, Nothing, Nothing]
  ): UIO[List[Int]] =
    for
      queue   <- Queue.unbounded[Int]
      claimed <- Ref.make(List.empty[Int])
      gate    <- Promise.make[Nothing, Unit] // never completed: the handler never finishes
      _       <- queue.offerAll(List(1, 2, 3))
      fiber   <- ZIO.scoped(strategy(recording(queue, claimed), 1)(_ => gate.await)).fork
      _       <- ZIO.sleep(250.millis)
      seen    <- claimed.get
      _       <- fiber.interrupt
    yield seen

  def spec: Spec[TestEnvironment & Scope, Any] = suite("BoundedListening")(
    test("claiming first takes a value it has no capacity to run") {
      // One permit, one handler occupying it — and a second value already claimed, waiting. On a leased
      // intake that value's clock is running while it waits.
      for seen <- claimsUnder(BoundedListening.eager(_, _))
      yield assertTrue(seen == List(1, 2))
    },
    test("reserving first claims only what it can run") {
      for seen <- claimsUnder(BoundedListening.reserved(_, _))
      yield assertTrue(seen == List(1))
    },
  ) @@ TestAspect.withLiveClock @@ TestAspect.timeout(20.seconds)
