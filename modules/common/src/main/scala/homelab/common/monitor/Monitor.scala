package homelab.common.monitor

import zio.*


/**
 * Observability port — wrap an operation so it appears on a trace, and optionally so it is counted and
 * timed, without importing the telemetry backend. Implemented by an adapter that captures the
 * tracer/meter (e.g. zio-telemetry) at construction.
 *
 * '''Two methods, one question: is this worth a metric?''' Both open a span and both record a failure;
 * [[measure]] additionally counts the operation and times it. Metrics are tagged with the operation's
 * name, so every distinct name is a series somebody pays for — which is why observing a step on the trace
 * is not the same decision as putting it on a dashboard.
 *
 * Either continues an inbound trace context when one is in scope (e.g. a `traceparent` the inbound adapter
 * placed there), so a trace links across services; otherwise it begins a new trace.
 *
 * Transparent to `R`, `E`, and `A`: the wrapped effect's type is returned unchanged. The monitor observes
 * the effect's *exit* — marking the span errored and bumping an error metric on failure — then re-raises
 * the original outcome untouched. It neither narrows the error channel nor adds to the environment; the
 * backend is held by the implementation, not required in `R`.
 */
trait Monitor {

  /**
   * Observe an operation named `name` on the trace, without counting or timing it.
   *
   * Failures are still recorded — the span is marked and an error metric bumped — because a failure is
   * worth knowing about wherever it happens. What is skipped is the hit count and the latency histogram.
   *
   * Reach for this where a metric series would cost more than it tells: a step that is fine-grained, one
   * whose name varies, or one already covered by the [[measure]] around it. When in doubt, [[measure]].
   *
   * @tparam R the wrapped effect's environment
   * @tparam E the wrapped effect's error
   * @tparam A the wrapped effect's result
   * @param name   the span name for this operation, and the tag any recorded failure carries
   * @param tags   span attributes (key → value) to attach
   * @param effect the work to observe
   * @return `effect`'s result unchanged — the span is recorded around it as a side effect
   */
  def trace[R, E, A](name: String, tags: (String, String)*)(effect: => ZIO[R, E, A]): ZIO[R, E, A]

  /**
   * Observe an operation named `name` on the trace, and count and time it.
   *
   * Everything [[trace]] does, plus a hit counter and a latency measurement tagged with this operation —
   * the pair that answers "how often, and how slow". The default choice for anything a dashboard or an
   * alert would ever ask about: use cases, calls to another service, work against a store.
   *
   * @tparam R the wrapped effect's environment
   * @tparam E the wrapped effect's error
   * @tparam A the wrapped effect's result
   * @param name   the span and metric name for this operation
   * @param tags   span and metric attributes (key → value) to attach
   * @param effect the work to observe
   * @return `effect`'s result unchanged — the span and metrics are recorded around it as a side effect
   */
  def measure[R, E, A](name: String, tags: (String, String)*)(effect: => ZIO[R, E, A]): ZIO[R, E, A]
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
    def trace[R, E, A](name: String, tags: (String, String)*)(effect: => ZIO[R, E, A]): ZIO[R, E, A] = effect

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
    def measure[R, E, A](name: String, tags: (String, String)*)(effect: => ZIO[R, E, A]): ZIO[R, E, A] = effect

  /**
   * A [[Monitor]] that logs the failures of whatever it wraps.
   *
   * A decorator rather than an implementation: every call is delegated untouched, and a failure is written
   * to the log on its way past. '''Nothing is logged when work starts or succeeds''' — that was tried, and
   * two lines per operation is unusable in production, where it becomes the noisiest thing in the process.
   *
   * What it is for: a failure that reached a span and an error counter is invisible to anyone without a
   * telemetry backend, and greppable here.
   *
   * '''Two things to know.'''
   *
   *  - '''A cancelled fiber is not a failure''', so an interruption writes nothing. A defect does: it is
   *    the failure most worth a line, and it is logged with its `Cause` rather than interpolated into a
   *    string, so the stack trace survives to whoever reads the log at three in the morning. This matches
   *    what the wrapped adapter records, so the log and the metrics agree about what failed.
   *  - '''The line is written after the delegate's span has closed''', so a backend that annotates logs
   *    with the current trace will not correlate it with the operation it describes.
   *
   * @param monitor what to delegate to; the logging is wrapped around it
   */
  class WithLogging(monitor: Monitor) extends Monitor:

    /**
     * Delegate to [[Monitor.trace]], logging a failure or a defect on the way past.
     *
     * @tparam R the wrapped effect's environment
     * @tparam E the wrapped effect's error
     * @tparam A the wrapped effect's result
     * @param name   the operation name, used in the failure line and passed on unchanged
     * @param tags   the operation's tags, passed on unchanged
     * @param effect the work to observe
     * @return `effect`'s result unchanged; fails exactly as `effect` does
     */
    def trace[R, E, A](name: String, tags: (String, String)*)(effect: => ZIO[R, E, A]): ZIO[R, E, A] =
      monitor
        .trace(name, tags*)(effect)
        .onError(cause => ZIO.logErrorCause(s"Operation $name failed", cause).unless(cause.isInterruptedOnly))

    /**
     * Delegate to [[Monitor.measure]], logging a failure or a defect on the way past.
     *
     * @tparam R the wrapped effect's environment
     * @tparam E the wrapped effect's error
     * @tparam A the wrapped effect's result
     * @param name   the operation name, used in the failure line and passed on unchanged
     * @param tags   the operation's tags, passed on unchanged
     * @param effect the work to observe
     * @return `effect`'s result unchanged; fails exactly as `effect` does
     */
    def measure[R, E, A](name: String, tags: (String, String)*)(effect: => ZIO[R, E, A]): ZIO[R, E, A] =
      monitor
        .measure(name, tags*)(effect)
        .onError(cause => ZIO.logErrorCause(s"Operation $name failed", cause).unless(cause.isInterruptedOnly))
