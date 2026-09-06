package homelab.telemetry


import homelab.common.monitor.Monitor
import io.opentelemetry.api
import io.opentelemetry.api.common.Attributes
import io.opentelemetry.api.trace.Span
import io.opentelemetry.context.Context
import zio.*
import zio.telemetry.opentelemetry.OpenTelemetry
import zio.telemetry.opentelemetry.context.ContextStorage
import zio.telemetry.opentelemetry.metrics.{ Counter, Histogram, Meter }
import zio.telemetry.opentelemetry.tracing.Tracing


/**
 * OpenTelemetry [[Monitor]] (via zio-telemetry). Every observed operation opens a span named for it and
 * records a failure against a **shared** error counter; [[measure]] additionally records into a shared hit
 * counter and latency histogram. Every measurement is tagged with `operation = <name>` plus the caller's
 * tags.
 *
 * The instruments are built once by [[OtelMonitor.make]], not per call, and the operation name is a metric
 * *attribute* rather than part of the metric name — so one metric covers all operations and a dashboard
 * aggregates or breaks down by `operation` (e.g. `sum by (operation) (rate(operation_hits_total[5m]))`)
 * instead of a metric-per-operation explosion. The tracer and instruments are held by the adapter, never
 * required in the wrapped effect's `R`.
 *
 * '''The current span lives in a `FiberRef`, and the ambient one is adopted once.''' A Java agent keeps it
 * in a thread-local instead, which does not survive a fiber parking and resuming on another worker — so an
 * operation that waits for anything would open every later span in a trace of its own. This adapter always
 * uses the fiber-local storage, and reads the thread-local only for an operation on a fiber that carries no
 * span yet: that is the inbound edge of a request, still running on the thread the agent made its span
 * current on, so the tree joins the agent's trace and then stays joined across any number of parks. Spans
 * the agent opens for itself — a Redis or JDBC call — still read the thread-local and so may fall outside;
 * the operation measured around them is the compensation.
 *
 * Failures are classified ([[ErrorType]]): the error counter is tagged with a bounded `error.kind`,
 * and the span is marked errored only for server-side errors — client errors (validation, unauthorised, …)
 * stay green. Interruptions aren't counted as errors.
 *
 * @param tracing the zio-telemetry tracer
 * @param storage where the current span is kept — the fiber-local one, and the thread-local it adopts from
 * @param hits the shared hit counter
 * @param latency the shared latency histogram (milliseconds)
 * @param errors the shared error counter
 * @param classify how a failure maps to its `error.kind` label and server/client side
 */
final class OtelMonitor private (
  tracing: Tracing,
  storage: ContextStorage,
  hits: Counter[Long],
  latency: Histogram[Double],
  errors: Counter[Long],
  classify: Any => ErrorType,
) extends Monitor:

  import tracing.aspects.span

  /**
   * Observe an operation on the trace: a span named `name`, tagged with `operation` and the caller's tags,
   * continuing the inbound trace context when one is in scope and starting a new trace otherwise.
   *
   * A failure still reaches the error counter and the span; what this does not do is touch the hit counter
   * or the latency histogram, so no `operation` series is created for an operation that never fails.
   *
   * The span kind is left at the default for now — telling a `SERVER` entry point from an `INTERNAL` step
   * is a separate axis from whether an operation is worth a metric, and hanging it on this pair would
   * conflate the two.
   *
   * @tparam R the wrapped effect's environment
   * @tparam E the wrapped effect's error
   * @tparam A the wrapped effect's result
   * @param name the span name and `operation` tag value
   * @param tags extra span attributes
   * @param effect the work to observe
   * @return `effect`'s result unchanged
   */
  def trace[R, E, A](name: String, tags: (String, String)*)(effect: => ZIO[R, E, A]): ZIO[R, E, A] =
    trace(name, attributesOf(name, tags), effect)

  /**
   * Observe an operation on the trace, and record the shared hit/latency/error instruments against it.
   *
   * The span is what [[trace]] opens; what is added here is `hits.inc` before the work and
   * `latency.record` after it, both tagged with this `operation`. The hit is counted before the span
   * opens on purpose — an operation that was attempted counts even if opening the span or the work itself
   * blows up, so hits and errors stay comparable.
   *
   * @tparam R the wrapped effect's environment
   * @tparam E the wrapped effect's error
   * @tparam A the wrapped effect's result
   * @param name the span name and `operation` tag value
   * @param tags extra span and metric attributes
   * @param effect the work to observe
   * @return `effect`'s result unchanged
   */
  def measure[R, E, A](name: String, tags: (String, String)*)(effect: => ZIO[R, E, A]): ZIO[R, E, A] =
    val attributes = attributesOf(name, tags)
    for
      _           <- hits.inc(attributes)
      (time, res) <- trace(name, attributes, effect).timed
      _           <- latency.record(time.toMillis.toDouble, attributes)
    yield res

  /**
   * The span both public methods put around the work, and the failure reporting that goes with it.
   *
   * Takes the attributes already built rather than the caller's tags, because [[measure]] needs the same
   * set for its hit and latency instruments and should not build it twice.
   *
   * `onError` rather than a fold: the outcome is observed and re-raised untouched, so neither the error
   * channel nor the success value is disturbed by being monitored. What counts as an error is
   * [[recordError]]'s decision, not this method's.
   *
   * @tparam R the wrapped effect's environment
   * @tparam E the wrapped effect's error
   * @tparam A the wrapped effect's result
   * @param name the span name
   * @param attributes the span attributes, and the tags any recorded error carries
   * @param effect the work to observe
   * @return `effect`'s result unchanged, failing exactly as it would have unobserved
   */
  private def trace[R, E, A](name: String, attributes: Attributes, effect: => ZIO[R, E, A]): ZIO[R, E, A] =
    adopting(effect @@ span(name, attributes = attributes))
      .onError(recordError(name, attributes, _))

  /**
   * Run `observed` under the span an agent has made current, but only on a fiber carrying none of its own.
   *
   * Reading the thread-local inside the effect rather than where it is described is the whole trick: an
   * operation is described wherever the graph was built and run on a worker, and it is the worker that an
   * agent has made the inbound span current on. A fiber that already carries a span is left alone, which is
   * what keeps a deeper operation nesting under its caller rather than re-adopting something stale.
   *
   * @tparam R the observed effect's environment
   * @tparam E the observed effect's error
   * @tparam A the observed effect's result
   * @param observed the span-wrapped effect, whose span reads the context this sets
   * @return `observed`'s result unchanged
   */
  private def adopting[R, E, A](observed: ZIO[R, E, A]): ZIO[R, E, A] =
    storage.get.flatMap: carried =>
      if recorded(carried) then observed
      else
        ZIO.succeed(Context.current()).flatMap: ambient =>
          if recorded(ambient) then storage.locally(ambient)(observed) else observed

  /**
   * Whether a context names a span worth being the parent of one — a root context does not.
   *
   * @param context the context to judge
   * @return true when it carries a valid span context
   */
  private def recorded(context: Context): Boolean =
    Span.fromContext(context).getSpanContext.isValid

  /**
   * Report a failure — unless it was a pure interruption (a cancelled fiber isn't an error). Uses the
   * first typed failure, or the defect for a die.
   *
   * @tparam E the failed effect's error type
   * @param name the operation name (for the span event)
   * @param attributes the operation tags to record against the error counter
   * @param cause the failure cause
   * @return noop
   */
  private def recordError[E](name: String, attributes: Attributes, cause: Cause[E]): UIO[Unit] =
    cause.failureOption.orElse(cause.dieOption) match
      case Some(error) => report(name, attributes, error)
      case None        => ZIO.unit

  /**
   * Classify `error` and record it: bump the shared error counter tagged with its `error.kind`, add a span
   * event, and — only for a server-side error — mark the span errored.
   *
   * @param name the operation name (for the span event)
   * @param attributes the operation tags
   * @param error the failure value (or defect) to classify and record
   * @return noop
   */
  private def report(name: String, attributes: Attributes, error: Any): UIO[Unit] = {
    val errorType       = classify(error)
    val errorAttributes = attributes.toBuilder.put("error.kind", errorType.kind).build()
    for
      _ <- tracing.setAttribute("error.kind", errorType.kind)
      _ <- ZIO.when(errorType.serverSide)(tracing.setAttribute("error", true))
      _ <- tracing.addEvent(s"$name failed [${errorType.kind}]: $error")
      _ <- errors.inc(errorAttributes)
    yield ()
  }

  /**
   * Build the attributes for an operation — the caller's `tags` plus `operation = name`.
   *
   * @param name the operation name
   * @param tags the caller's key/value tags
   * @return the attributes
   */
  private def attributesOf(name: String, tags: Seq[(String, String)]): Attributes =
    tags
      .foldLeft(Attributes.builder()):
        case (builder, (key, value)) => builder.put(key, value)
      .put("operation", name)
      .build()


object OtelMonitor:

  /**
   * Build an [[OtelMonitor]] on `otel`, with its tracer, meter and shared instruments made once.
   *
   * By convention the toolkit leaves layer wiring to the application, so this hands back an effect and the
   * caller decides what to wrap it in:
   *
   * {{{
   * val monitor: ZLayer[Any, Throwable, Monitor] =
   *   ZLayer.scoped:
   *     for
   *       otel    <- ZIO.attempt(GlobalOpenTelemetry.get())
   *       monitor <- OtelMonitor.make(otel, "orders-service")
   *     yield monitor
   * }}}
   *
   * '''Which SDK is the caller's decision; how the context is stored is not.''' `GlobalOpenTelemetry.get()`
   * is whatever registered itself — a Java agent when one is loaded, a no-op otherwise — and a test can pass
   * its own instead. The `ContextStorage` is deliberately not a parameter: the thread-local one loses the
   * current span whenever a fiber parks, which is a silent wrong answer rather than a failure, so it is not
   * offered. See the note on the class about what is adopted from a thread-local and what is not.
   *
   * Scoped because the tracer and meter are: closing the scope releases them.
   *
   * @param otel the SDK to build the tracer and meter from
   * @param scope the instrumentation scope every span and instrument is attributed to
   * @param classify how failures map to their `error.kind` label and server/client side. A total function;
   *                 to override only some failures, wrap a partial one with [[ErrorType.refine]], which
   *                 falls back to [[ErrorType.defaultClassifier]] for everything else
   * @return the monitor, with its instruments pre-built
   */
  def make(
    otel: api.OpenTelemetry,
    scope: String,
    classify: ErrorType.Classifier = ErrorType.defaultClassifier,
  ): URIO[Scope, OtelMonitor] =
    for
      built   <- components(otel, scope).build
      meter    = built.get[Meter]
      hits    <- meter.counter("operation.hits")
      latency <- meter.histogram("operation.latency", unit = Some("ms"))
      errors  <- meter.counter("operation.errors")
    yield new OtelMonitor(built.get[Tracing], built.get[ContextStorage], hits, latency, errors, classify)

  /**
   * The tracer, the meter, and the one context storage both are built on.
   *
   * `base` is a single value referenced three times on purpose: a layer graph memoises by reference, so the
   * tracer, the meter and the monitor all share one `FiberRef`. Calling `OpenTelemetry.contextZIO` at each
   * use site would build three, and a span written to one would be invisible to the others.
   *
   * @param otel the SDK to build on
   * @param scope the instrumentation scope
   * @return the layer, kept private so no caller can substitute the storage
   */
  private def components(otel: api.OpenTelemetry, scope: String): ULayer[Tracing & Meter & ContextStorage] =
    val base = ZLayer.succeed(otel) ++ OpenTelemetry.contextZIO
    (base >>> OpenTelemetry.tracing(scope)) ++ (base >>> OpenTelemetry.metrics(scope)) ++ base
