package homelab.common.messaging

import zio.*


/**
 * A [[Worker]] with an input: it consumes `A` and emits however it likes — to zero, one, or many
 * [[Producer]]s held as its own fields. Emission is intentionally unspecified here; the common
 * 1-in/1-out case is [[Pipe]], which also supplies the run loop. A worker that fans out to several
 * outputs (success + dead-letter, say) extends `Processor` directly and writes its own `run`.
 *
 * @tparam E the error processing aborts with
 * @tparam A the value consumed
 */
trait Processor[+E, +A] extends Worker[E] {

  /**
   * The intake this processor consumes from.
   *
   * @return the input consumer
   */
  def input: Consumer[E, A]
}


object Processor {

  /**
   * A [[Processor]] whose intake delivers batches — its `input` is a [[Consumer.Batched]].
   *
   * @tparam E the error processing aborts with
   * @tparam A the element type of each consumed batch
   */
  trait Batched[E, A] extends Processor[E, List[A]] {

    /**
     * The batched intake this processor consumes from.
     *
     * @return the batched input consumer
     */
    def input: Consumer.Batched[E, A]
  }

  /**
   * A [[Processor]] that processes up to `parallelism` values concurrently by spawning consume calls
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
   */
  trait Parallel[E, A] extends Processor[E, A] {

    /**
     * The concurrency cap — how many values may process at once. Must be positive.
     *
     * @return the maximum number of concurrent `process` runs
     */
    def parallelism: Int

    /**
     * Process a single consumed value.
     *
     * @param value the value to process
     * @return unit once processed; aborts with `E` on failure
     */
    def process(value: A): IO[E, Unit]

    /**
     * Run the spawn-on-availability loop until interrupted or the first failure, processing up to
     * `parallelism` values concurrently.
     *
     * @return never completes successfully; aborts with `E` on the first failure
     */
    def run: IO[E, Nothing] =
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
     * @return unit once the spawned listener has started processing (holding a value and a permit)
     */
    private def spawn(sem: Semaphore, failure: Promise[E, Nothing]): UIO[Unit] =
      for
        started <- Promise.make[Nothing, Unit]
        _       <- input
                     // Signal INSIDE the permit: the next listener spawns only once this value holds a
                     // permit, so claimed-but-waiting fibers cannot pile up under a deep backlog.
                     .consume(value => sem.withPermit(started.succeed(()) *> process(value)))
                     .catchAllCause(failure.failCause(_).unit)
                     .fork
        _       <- started.await
      yield ()

  }

}
