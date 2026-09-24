package homelab.incubator.llm.v4.playground.outstanding


import homelab.common.error.ApplicationError
import zio.IO


/**
 * Where a tool keeps what it started.
 *
 * The whole of the bookkeeping the toolkit deliberately does not do. A conversation records that the tool
 * answered "working"; this records what is actually running, which conversation it belongs to, and what it
 * came back with. Nothing reconciles the two, because only this one is asked.
 *
 * Unimplemented. A real one is a table keyed by id with a nullable answer, and a query for rows that have
 * been running too long — the sweep a conversation cannot answer.
 */
trait InvestigationStore {

  /**
   * Record work that has just been started.
   *
   * @param investigation what was started, with no answer yet
   * @return noop once recorded; aborts when the store does
   */
  def start(investigation: Investigation): IO[ApplicationError.AdapterError, Unit]

  /**
   * Record what a piece of work came back with.
   *
   * @param id which work finished
   * @param answer what it found
   * @return noop once recorded; aborts when the store does
   */
  def finish(id: Investigation.Id, answer: String): IO[ApplicationError.AdapterError, Unit]

  /**
   * Read one piece of work.
   *
   * @param id which one
   * @return it, or nothing when no such work was started; aborts when the store does
   */
  def get(id: Investigation.Id): IO[ApplicationError.AdapterError, Option[Investigation]]
}
