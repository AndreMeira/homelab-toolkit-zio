package homelab.incubator.time.v1

import zio.*


class Ticker private (
  period: Duration,
  slackDuration: Duration,
  queue: Queue[Long],
  version: Ref[Long],
  signal: Ref[Promise[Nothing, Unit]],
  scope: Scope,
) {

  def awaitNextTick: UIO[Unit] =
    signal.get.flatMap(_.await)

  def next(duration: Duration): UIO[Unit] =
    for {
      floor = duration max slackDuration
      next <- version.incrementAndGet
      _    <- queue.offer(next).delay(floor).forkIn(scope)
    } yield ()

  def run: UIO[Nothing] =
    for {
      ticked  <- queue.take
      current <- version.get
      _       <- tick.when(ticked >= current)
      never   <- run
    } yield never

  private def tick: UIO[Unit] =
    for
      newSignal <- Promise.make[Nothing, Unit]
      oldSignal <- signal.getAndSet(newSignal)
      _         <- oldSignal.succeed(())
      // Read at tick time, so the backstop is never stale: it is the version the next pass of `run` reads,
      // unless a `next` comes in between, and then that request's own tick arms a fresh backstop.
      following <- version.incrementAndGet
      _         <- queue.offer(following).delay(period).forkIn(scope)
    yield ()
}


object Ticker {

  /**
   * A ticker whose first tick is due at once: its queue starts with the version the first pass of `run`
   * reads.
   *
   * @param period the gap between backstop ticks
   * @param slackDuration the shortest delay a requested tick may have
   * @return the ticker; the delayed offers it forks live in the caller's scope
   */
  def make(period: Duration, slackDuration: Duration): URIO[Scope, Ticker] =
    for {
      scope   <- ZIO.scope
      queue   <- Queue.sliding[Long](1)
      version <- Ref.make(0L)
      first   <- Promise.make[Nothing, Unit]
      signal  <- Ref.make(first)
      _       <- queue.offer(0L)
    } yield new Ticker(period, slackDuration, queue, version, signal, scope)
}
