package homelab.telemetry


import homelab.common.monitor.Monitor
import io.opentelemetry.api.common.Attributes
import io.opentelemetry.api.trace.SpanKind
import zio.*
import zio.telemetry.opentelemetry.metrics.{ Counter, Histogram, Meter }
import zio.telemetry.opentelemetry.tracing.Tracing


/**
 * OpenTelemetry [[Monitor]] (via zio-telemetry). Each observed operation opens a span (named per
 * operation) and records into three **shared** instruments — a hit counter, a latency histogram, and an
 * error counter — tagging every measurement with `operation = <name>` plus the caller's tags.
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
   * Observe a top-level operation as a `SERVER`-kind span — a child of the inbound trace context when one
   * is in scope (so a trace links across services), otherwise a new root — recording the shared
   * hit/latency/error metrics tagged with this `operation`.
   *
   * @tparam R the wrapped effect's environment
   * @tparam E the wrapped effect's error
   * @tparam A the wrapped effect's result
   * @param name the span name and `operation` tag value
   * @param tags extra span/metric attributes
   * @param effect the work to observe
   * @return `effect`'s result unchanged
   */
  def start[R, E, A](name: String, tags: (String, String)*)(effect: => ZIO[R, E, A]): ZIO[R, E, A] =
    val attributes = attributesOf(name, tags)
    for
      _       <- hits.inc(attributes)
      outcome <- (effect.onError(recordError(name, attributes, _)) @@ span(name, spanKind = SpanKind.SERVER, attributes = attributes)).timed
      _       <- latency.record(outcome._1.toMillis.toDouble, attributes)
    yield outcome._2

  /**
   * Observe a nested step as an `INTERNAL`-kind child span under the current one, recording the shared
   * hit/latency/error metrics tagged with this `operation`.
   *
   * @tparam R the wrapped effect's environment
   * @tparam E the wrapped effect's error
   * @tparam A the wrapped effect's result
   * @param name the span name and `operation` tag value
   * @param tags extra span/metric attributes
   * @param effect the work to observe
   * @return `effect`'s result unchanged
   */
  def track[R, E, A](name: String, tags: (String, String)*)(effect: => ZIO[R, E, A]): ZIO[R, E, A] =
    val attributes = attributesOf(name, tags)
    for
      _       <- hits.inc(attributes)
      outcome <- (effect.onError(recordError(name, attributes, _))
                   @@ span(name, spanKind = SpanKind.INTERNAL, attributes = attributes)).timed
      _       <- latency.record(outcome._1.toMillis.toDouble, attributes)
    yield outcome._2

  /**
   * Report a failure — unless it was a pure interruption (a cancelled fiber isn't an error). Uses the
   * first typed failure, or the defect for a die.
   *
   * @tparam E the failed effect's error type
   * @param name the operation name (for the span event)
   * @param attributes the operation tags to record against the error counter
   * @param cause the failure cause
   * @return unit
   */
  private def recordError[E](name: String, attributes: Attributes, cause: Cause[E]): UIO[Unit] =
    val failure: Option[Any] = cause.failureOption.orElse(cause.dieOption)
    failure match
      case Some(error) => report(name, attributes, error)
      case None        => ZIO.unit

  /**
   * Classify `error` and record it: bump the shared error counter tagged with its `error.kind`, add a span
   * event, and — only for a server-side error — mark the span errored.
   *
   * @param name the operation name (for the span event)
   * @param attributes the operation tags
   * @param error the failure value (or defect) to classify and record
   * @return unit
   */
  private def report(name: String, attributes: Attributes, error: Any): UIO[Unit] =
    val errorType       = classify(error)
    val errorAttributes = attributes.toBuilder.put("error.kind", errorType.kind).build()
    for
      _ <- tracing.setAttribute("error.kind", errorType.kind)
      _ <- ZIO.when(errorType.serverSide)(tracing.setAttribute("error", true))
      _ <- tracing.addEvent(s"$name failed [${errorType.kind}]: $error")
      _ <- errors.inc(errorAttributes)
    yield ()

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
   * @param tracing the zio-telemetry tracer
   * @param meter the zio-telemetry meter
   * @param classify how failures map to their `error.kind` label and server/client side
   *                 (defaults to [[ErrorType.defaultClassifier]])
   * @return         the monitor, with its instruments pre-built
   */
  def make(
    tracing: Tracing,
    meter: Meter,
  )(
    classify: PartialFunction[Any, ErrorType] = PartialFunction.empty
  ): UIO[OtelMonitor] =
    for
      hits    <- meter.counter("operation.hits")
      latency <- meter.histogram("operation.latency", unit = Some("ms"))
      errors  <- meter.counter("operation.errors")
    yield new OtelMonitor(tracing, hits, latency, errors, ErrorType.refine(classify))
