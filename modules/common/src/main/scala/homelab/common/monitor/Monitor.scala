package homelab.common.monitor

import zio.*


/**
 * Observability port — wrap an operation to open a trace span AND record execution metrics (a hit
 * counter, latency, and an error count) about it, so any use case can be observed without importing the
 * telemetry backend. Implemented by an adapter that captures the tracer/meter (e.g. zio-telemetry) at
 * construction.
 *
 * Transparent to `R`, `E`, and `A`: the wrapped effect's type is returned unchanged. The monitor
 * observes the effect's *exit* — recording latency on success, or marking the span errored and bumping
 * an error metric on failure — then re-raises the original outcome untouched. It neither narrows the
 * error channel nor adds to the environment; the backend is held by the implementation, not required
 * in `R`.
 */
trait Monitor {

  /**
   * Observe a top-level operation (the root of a unit of work) named `name`.
   *
   * Continues an inbound trace context when one is in scope (e.g. a `traceparent` the inbound adapter
   * placed there), so a trace links across services; otherwise it begins a new trace.
   *
   * @tparam R the wrapped effect's environment
   * @tparam E the wrapped effect's error
   * @tparam A the wrapped effect's result
   * @param name   the span / metric name for this operation
   * @param tags   span attributes (key → value) to attach
   * @param effect the work to observe
   * @return `effect`'s result unchanged — the span and metrics are recorded around it as a side effect
   */
  def start[R, E, A](name: String, tags: (String, String)*)(effect: => ZIO[R, E, A]): ZIO[R, E, A]

  /**
   * Observe a nested step named `name` — a child span under whatever span is currently in scope, plus
   * the same hit / latency / error metrics.
   *
   * @tparam R the wrapped effect's environment
   * @tparam E the wrapped effect's error
   * @tparam A the wrapped effect's result
   * @param name   the span / metric name for this step
   * @param tags   span attributes (key → value) to attach
   * @param effect the work to observe
   * @return `effect`'s result unchanged — the span and metrics are recorded around it as a side effect
   */
  def track[R, E, A](name: String, tags: (String, String)*)(effect: => ZIO[R, E, A]): ZIO[R, E, A]
}


object Monitor:

  /**
   * A [[Monitor]] that observes nothing — every operation runs unchanged, with no span opened and no
   * metric recorded. For tests, and for running without a telemetry backend without touching call sites.
   */
  object Noop extends Monitor:

    /**
     * Run `effect` unobserved.
     *
     * @tparam R the wrapped effect's environment
     * @tparam E the wrapped effect's error
     * @tparam A the wrapped effect's result
     * @param name   ignored
     * @param tags   ignored
     * @param effect the work to run
     * @return `effect` unchanged
     */
    def start[R, E, A](name: String, tags: (String, String)*)(effect: => ZIO[R, E, A]): ZIO[R, E, A] = effect

    /**
     * Run `effect` unobserved.
     *
     * @tparam R the wrapped effect's environment
     * @tparam E the wrapped effect's error
     * @tparam A the wrapped effect's result
     * @param name   ignored
     * @param tags   ignored
     * @param effect the work to run
     * @return `effect` unchanged
     */
    def track[R, E, A](name: String, tags: (String, String)*)(effect: => ZIO[R, E, A]): ZIO[R, E, A] = effect
