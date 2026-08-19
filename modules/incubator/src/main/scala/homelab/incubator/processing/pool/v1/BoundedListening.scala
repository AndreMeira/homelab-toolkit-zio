package homelab.incubator.processing.pool.v1


import homelab.common.messaging.Consumer
import zio.*


/**
 * Two placements of the same semaphore, side by side.
 *
 * `Processor.parallel` spawns listeners on demand and caps concurrency with a semaphore taken *inside* the
 * delivery callback. That makes the permit gate `handle`, not `consume` — a value is claimed from the intake
 * first, and only then does its fiber wait for capacity. The pile-up is bounded (the `started` promise is
 * completed inside the permit, so the next listener is not spawned until the current one holds both), but
 * one value can always be sitting claimed with no capacity to run it.
 *
 * Whether that matters is a property of the intake, not of the processor:
 *
 *   - '''In-memory''' (a queue-backed pipe): claiming is free and reversible on interruption. [[eager]] is
 *     the better trade — no permit is spent on a listener that is merely waiting.
 *   - '''Leased''' (JetStream, SQS, `PollConsumer.Source`): a claimed value has a clock running. Held
 *     without capacity it ages toward redelivery, and a heartbeat covers the handler, not the wait.
 *     [[reserved]] never claims what it cannot immediately run.
 *   - '''Keyed''' (`Pipe.KeySafe`): `consume` holds the key for its whole call, so a value waiting for a
 *     permit blocks its key. [[reserved]] moves that wait outside the key.
 *
 * [[reserved]] costs no concurrency, which is easy to get wrong: the listener parked on the intake does hold
 * a permit, but only while the intake has nothing to give — and then no other value needs it. When work is
 * plentiful every permit goes to a handler, so `n` permits still means `n` concurrent handlers. What
 * [[eager]] keeps is a one-deep prefetch: its value is already in hand when a permit frees, hiding the
 * intake's latency behind the previous handler's work.
 */
object BoundedListening {

  /**
   * Claim first, then wait for capacity — the placement `Processor.parallel` uses.
   *
   * @param input the intake to pull values from
   * @param parallelism how many values may run `handle` at once
   * @param handle the per-value effect
   * @return never completes successfully; aborts with `E` on the first failure
   */
  def eager[E, A](input: Consumer[E, A], parallelism: Int)(handle: A => IO[E, Unit]): ZIO[Scope, E, Nothing] =
    listen(parallelism) { (sem, failure, started) =>
      input
        .consume(value => sem.withPermit(started.succeed(()) *> handle(value)))
        .catchAllCause(failure.failCause(_).unit)
    }

  /**
   * Wait for capacity, then claim — nothing is taken from the intake without a permit in hand.
   *
   * @param input the intake to pull values from
   * @param parallelism how many values may be *claimed* at once, one of which may be a listener still waiting
   * @param handle the per-value effect
   * @return never completes successfully; aborts with `E` on the first failure
   */
  def reserved[E, A](input: Consumer[E, A], parallelism: Int)(handle: A => IO[E, Unit]): ZIO[Scope, E, Nothing] =
    listen(parallelism) { (sem, failure, started) =>
      sem
        .withPermit(input.consume(value => started.succeed(()) *> handle(value)))
        .catchAllCause(failure.failCause(_).unit)
    }

  /**
   * The shared skeleton: keep exactly one listener waiting, spawn the next only once the current one holds
   * both a value and a permit, and abort on the first failure any listener reports.
   *
   * @param parallelism the semaphore's size
   * @param listener builds one listener from the permit source, the failure sink, and the promise it
   *                 completes once it holds both a value and a permit
   * @return never completes successfully; aborts with `E` on the first failure
   */
  private def listen[E](
    parallelism: Int
  )(listener: (Semaphore, Promise[E, Nothing], Promise[Nothing, Unit]) => IO[Nothing, Unit]): ZIO[Scope, E, Nothing] =
    for
      scope   <- ZIO.scope
      sem     <- Semaphore.make(parallelism)
      failure <- Promise.make[E, Nothing]
      spawn    = Promise
                   .make[Nothing, Unit]
                   .flatMap(started => listener(sem, failure, started).forkIn(scope) *> started.await)
      _       <- spawn.forever.forkIn(scope)
      never   <- failure.await
    yield never
}
