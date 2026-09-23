package homelab.incubator.llm.v4.playground.outstanding


import homelab.common.error.ApplicationError
import zio.IO


/**
 * What does the work an [[Investigate]] call starts.
 *
 * Unimplemented, and the interesting half. A real one hands the topic to a second agent, a batch job or a
 * person, and when that finishes it calls [[InvestigationStore.finish]] and then [[Attendant.delivered]] — in
 * that order, so word of an answer never outruns the answer. What it queues carries the id and nothing
 * else: whoever picks it up reads the findings from the store, so a signal that arrives twice delivers
 * once.
 */
trait Investigator {

  /**
   * Start work and return.
   *
   * Answers as soon as the work is accepted, not when it is done — a caller that waited here would be the
   * thing this design exists to avoid.
   *
   * @param investigation what to work on, already recorded as started
   * @return noop once it is accepted; aborts when it cannot be started at all
   */
  def start(investigation: Investigation): IO[ApplicationError, Unit]
}
