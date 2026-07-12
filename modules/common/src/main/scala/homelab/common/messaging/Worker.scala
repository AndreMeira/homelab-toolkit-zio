package homelab.common.messaging


import zio.*


/**
 * A long-running unit of work in a messaging topology: it runs until interrupted or its first failure.
 *
 * `Worker` is the root of the topology hierarchy — a [[Source]] that only emits, a [[Processor]] that
 * only consumes, or a [[Pipe]] that does both. It owns no lifecycle of its own; the composition root
 * forks `run` and supervises it.
 *
 * @tparam E the error a run aborts with
 */
trait Worker[+E] {

  /**
   * Run this worker's loop until interrupted or the first failure.
   *
   * @return never completes successfully; aborts with `E` on the first failure
   */
  def run: IO[E, Nothing]
}
