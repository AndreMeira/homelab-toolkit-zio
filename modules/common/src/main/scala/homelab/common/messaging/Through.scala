package homelab.common.messaging

import zio.*


/**
 * The 1-input / 1-output [[Processor]]: consume `A`, transform, emit `B` on a single `output`.
 * Multi-output or output-less workers extend [[Processor]] (or [[Worker]]) directly and manage
 * producers as fields.
 *
 * @tparam E the error processing aborts with
 * @tparam A the input value
 * @tparam B the output value
 */
trait Through[+E, +A, -B] extends Processor[E, A] {

  /**
   * The single output this through node emits to.
   *
   * @return the output producer
   */
  def output: Producer[E, B]
}


object Through {

  /**
   * A [[Through]] that transforms one input into one output at a time.
   *
   * @tparam E the error processing aborts with
   * @tparam A the input value
   * @tparam B the output value
   */
  trait PerItem[E, A, B] extends Through[E, A, B] {

    /**
     * Transform a single input into a single output.
     *
     * @param value the input to transform
     * @return the output; aborts with `E` on failure
     */
    def process(value: A): IO[E, B]

    /**
     * Consume, transform, and emit in a loop until interrupted or the first failure.
     *
     * @return never completes successfully; aborts with `E` on failure
     */
    def run: IO[E, Nothing] = input.consume(value => process(value).flatMap(output.emit)).forever
  }

  /**
   * A [[Through]] that processes up to `parallelism` values concurrently by spawning consume calls
   * *on demand*: exactly one listener waits on the input at a time, and the moment it receives a value
   * (and a permit) it hands listening over to a freshly spawned listener and processes inline. Fibers
   * therefore scale with actual work — an idle processor is a single parked fiber, and a backlog holds
   * at most `parallelism` processing calls plus one waiting listener, regardless of depth.
   *
   * `process` always runs *inside* the consume call that delivered the value — never forked out of it —
   * because keyed inputs (e.g. a distributer) hold a value's key only for the duration of the call: an
   * early-returning dispatch would release the key mid-processing and break per-key serialisation.
   *
   * Errors fail fast: the first failing `process` (or `consume`) aborts `run` with that error and
   * interrupts all in-flight work.
   *
   * @tparam E the error processing aborts with
   * @tparam A the value consumed
   * @tparam B the value emitted
   */
  trait Parallel[E, A, B] extends Through.PerItem[E, A, B] {

    /**
     * The concurrency cap — how many values may process at once. Must be positive.
     *
     * @return the maximum number of concurrent `process` runs
     */
    def parallelism: Int

    /**
     * Run the spawn-on-availability loop until interrupted or the first failure, processing up to
     * `parallelism` values concurrently.
     *
     * @return never completes successfully; aborts with `E` on the first failure
     */
    override def run: IO[E, Nothing] =
      for
        sem     <- Semaphore.make(parallelism)
        failure <- Promise.make[E, Nothing]
        _       <- spawn(sem, failure).forever.fork
        never   <- failure.await
      yield never

    /**
     * One spawn step: fork a listener and park until it starts processing — holding a value *and* a
     * permit — then return, so [[run]]'s `forever` spawns the next listener exactly then. Repeated, this
     * keeps one waiting listener and up to `parallelism` processing ones, with fibers appearing only
     * when there is work. `process` runs inside the listener's consume call, keeping any key held for
     * the whole processing. A listener's failure is captured inside its fiber into `failure` (it is
     * otherwise unobserved), which is what aborts [[run]].
     *
     * @param sem     caps concurrent `process` runs at `parallelism`
     * @param failure the sink a listener's failure is reported to
     * @return noop once the spawned listener has started processing (holding a value and a permit)
     */
    private def spawn(sem: Semaphore, failure: Promise[E, Nothing]): UIO[Unit] =
      for
        started <- Promise.make[Nothing, Unit]
        _       <- input
                     // Signal INSIDE the permit: the next listener spawns only once this value holds a
                     // permit, so claimed-but-waiting fibers cannot pile up under a deep backlog.
                     .consume: value =>
                       sem.withPermit(started.succeed(()) *> process(value).flatMap(output.emit))
                     .catchAllCause(failure.failCause(_).unit)
                     .fork
        _       <- started.await
      yield ()
  }

  /**
   * A [[Through]] that transforms a batch of inputs into a batch of outputs at a time. The transform is
   * free to change cardinality (filter, fan-out): `process` maps a `List[A]` to a `List[B]` of any
   * length.
   *
   * @tparam E the error processing aborts with
   * @tparam A the input element
   * @tparam B the output element
   */
  trait Batched[E, A, B] extends Processor.Batched[E, A] with Through[E, List[A], B] {

    /**
     * Transform a batch of inputs into a batch of outputs.
     *
     * @param values the input batch
     * @return the output batch (of any length); aborts with `E` on failure
     */
    def process(values: List[A]): IO[E, List[B]]

    /**
     * Consume a batch, transform it, and emit the results in a loop until interrupted or the first
     * failure.
     *
     * @return never completes successfully; aborts with `E` on failure
     */
    def run: IO[E, Nothing] = input.consume(values => process(values).flatMap(output.emitMany)).forever
  }

  object Batched {

    /**
     * The batched counterpart of [[Through.Parallel]]: processes up to `parallelism` *batches*
     * concurrently by spawning consume calls *on demand*. Exactly one listener waits on the input at a
     * time, and the moment it receives a batch (and a permit) it hands listening over to a freshly spawned
     * listener and processes the batch inline. Fibers therefore scale with actual work — an idle processor
     * is a single parked fiber, and a backlog holds at most `parallelism` in-flight batches plus one
     * waiting listener, regardless of depth.
     *
     * As in [[Through.Parallel]], `process` always runs *inside* the consume call that delivered the batch
     * — never forked out of it — so keyed inputs keep a batch's key for the whole processing. Errors fail
     * fast: the first failing `process` (or `consume`) aborts [[run]] with that error and interrupts all
     * in-flight work.
     *
     * @tparam E the error processing aborts with
     * @tparam A the input element
     * @tparam B the output element
     */
    trait Parallel[E, A, B] extends Batched[E, A, B] {

      /**
       * The concurrency cap — how many values may process at once. Must be positive.
       *
       * @return the maximum number of concurrent `process` runs
       */
      def parallelism: Int

      /**
       * Run the spawn-on-availability loop until interrupted or the first failure, processing up to
       * `parallelism` batches concurrently.
       *
       * @return never completes successfully; aborts with `E` on the first failure
       */
      override def run: IO[E, Nothing] =
        for
          sem     <- Semaphore.make(parallelism)
          failure <- Promise.make[E, Nothing]
          _       <- spawn(sem, failure).forever.fork
          never   <- failure.await
        yield never

      /**
       * One spawn step: fork a listener and park until it starts processing — holding a value *and* a
       * permit — then return, so [[run]]'s `forever` spawns the next listener exactly then. Repeated, this
       * keeps one waiting listener and up to `parallelism` processing ones, with fibers appearing only
       * when there is work. `process` runs inside the listener's consume call, keeping any key held for
       * the whole processing. A listener's failure is captured inside its fiber into `failure` (it is
       * otherwise unobserved), which is what aborts [[run]].
       *
       * @param sem     caps concurrent `process` runs at `parallelism`
       * @param failure the sink a listener's failure is reported to
       * @return noop once the spawned listener has started processing (holding a value and a permit)
       */
      private def spawn(sem: Semaphore, failure: Promise[E, Nothing]): UIO[Unit] =
        for
          started <- Promise.make[Nothing, Unit]
          _       <- input
                       // Signal INSIDE the permit: the next listener spawns only once this value holds a
                       // permit, so claimed-but-waiting fibers cannot pile up under a deep backlog.
                       .consume: value =>
                         sem.withPermit(started.succeed(()) *> process(value).flatMap(output.emitMany))
                       .catchAllCause(failure.failCause(_).unit)
                       .fork
          _       <- started.await
        yield ()

    }
  }
}
