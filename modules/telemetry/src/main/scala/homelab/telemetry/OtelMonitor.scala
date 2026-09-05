package homelab.telemetry


import homelab.common.monitor.Monitor
import io.opentelemetry.api.common.Attributes
import zio.*
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
 * Failures are classified ([[ErrorType]]): the error counter is tagged with a bounded `error.kind`,
 * and the span is marked errored only for server-side errors — client errors (validation, unauthorised, …)
 * stay green. Interruptions aren't counted as errors.
 *
 * @param tracing the zio-telemetry tracer
 * @param hits the shared hit counter
 * @param latency the shared latency histogram (milliseconds)
 * @param errors the shared error counter
 * @param classify how a failure maps to its `error.kind` label and server/client side
 */
final class OtelMonitor private (
  tracing: Tracing,
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
    (effect @@ span(name, attributes = attributes))
      .onError(recordError(name, attributes, _))

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
   * Build an [[OtelMonitor]], creating the three shared instruments once from `meter`.
   *
   * By convention the toolkit leaves layers wiring responsibility to the application.
   * Here is what that looks like with zio-telemetry supplying `Tracing` and `Meter`:
   *
   * {{{
   * val otel    = OpenTelemetry.global ++ OpenTelemetry.contextJVM
   * val tracing = otel >>> OpenTelemetry.tracing("orders-service")
   * val metrics = otel >>> OpenTelemetry.metrics("orders-service")
   *
   * val monitor: ZLayer[Any, Throwable, Monitor] =
   *   (tracing ++ metrics) >>> ZLayer:
   *     for
   *       tracer  <- ZIO.service[Tracing]
   *       meter   <- ZIO.service[Meter]
   *       monitor <- OtelMonitor.make(tracer, meter)
   *     yield monitor
   * }}}
   *
   * `ContextStorage` is what carries the current span across fibers, so `contextJVM` feeds both the tracer
   * and the meter; the error channel is `Throwable` because `OpenTelemetry.global` is a `TaskLayer`. A
   * service that wants nothing observed provides [[Monitor.Noop]] instead, and no call site changes.
   *
   * @param tracing the zio-telemetry tracer
   * @param meter the zio-telemetry meter
   * @param classify how failures map to their `error.kind` label and server/client side. A total function;
   *                 to override only some failures, wrap a partial one with [[ErrorType.refine]], which
   *                 falls back to [[ErrorType.defaultClassifier]] for everything else
   * @return         the monitor, with its instruments pre-built
   */
  def make(
    tracing: Tracing,
    meter: Meter,
    classify: ErrorType.Classifier = ErrorType.defaultClassifier,
  ): UIO[OtelMonitor] =
    for
      hits    <- meter.counter("operation.hits")
      latency <- meter.histogram("operation.latency", unit = Some("ms"))
      errors  <- meter.counter("operation.errors")
    yield new OtelMonitor(tracing, hits, latency, errors, classify)
