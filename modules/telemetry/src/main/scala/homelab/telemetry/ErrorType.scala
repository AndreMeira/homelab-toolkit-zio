package homelab.telemetry


import homelab.common.error.ApplicationError
import homelab.common.error.ApplicationError.*
import homelab.common.error.ValidationError


/**
 * How a failure is reported to telemetry: a bounded, low-cardinality `kind` (the `error.kind` metric
 * label) and whether it is `serverSide`. Server-side errors mark the span errored and are what infra
 * alerts should fire on; client-side errors (bad input, unauthorised, not-found) are expected and leave
 * the span green.
 *
 * @param kind a bounded metric-label value — a fixed set, safe as a Prometheus/Grafana label
 * @param serverSide whether this is an alertable server-side error (vs an expected client-side one)
 */
final case class ErrorType(kind: String, serverSide: Boolean)


/**
 * Classifies a failure into an [[ErrorType]]. A plain `Any => ErrorType` — the observed effect's error
 * is generic, so the value is untyped — that a service can replace, delegating to [[defaultClassifier]] for the
 * cases it doesn't care to override.
 */
object ErrorType:

  /** A total classification of a failure value into its [[ErrorType]]. */
  type Classifier = Any => ErrorType

  /**
   * The default classification, from the `common` [[ApplicationError]] marker hierarchy: domain errors
   * (validation / unauthorised / not-found / conflict / other business rules) are client-side;
   * infrastructure errors and anything unrecognised — including defects — are server-side.
   * [[ApplicationError.InconsistentState]] is a `DomainError` but treated as server-side: it signals a
   * bug, not bad input.
   *
   * @return the error type for a given failure value
   */
  val defaultClassifier: Classifier = {
    case _: ValidationError    => ErrorType("validation", serverSide = false)
    case _: UnauthorisedError  => ErrorType("unauthorised", serverSide = false)
    case _: NotFoundError      => ErrorType("not_found", serverSide = false)
    case _: ConflictError      => ErrorType("conflict", serverSide = false)
    case _: InconsistentState  => ErrorType("inconsistent_state", serverSide = true)
    case _: DomainError        => ErrorType("domain", serverSide = false)
    case _: TransientError     => ErrorType("transient", serverSide = true)
    case _: NetworkError       => ErrorType("network", serverSide = true)
    case _: PersistenceError   => ErrorType("persistence", serverSide = true)
    case _: VendorError        => ErrorType("vendor", serverSide = true)
    case _: DecodingError      => ErrorType("decoding", serverSide = true)
    case _: UnrecoverableError => ErrorType("unrecoverable", serverSide = true)
    case _: AdapterError       => ErrorType("adapter", serverSide = true)
    case _: ApplicationError   => ErrorType("application", serverSide = true)
    case _                     => ErrorType("defect", serverSide = true)
  }

  /**
   * A [[Classifier]] that applies `fn` where it's defined and falls back to [[defaultClassifier]]
   * everywhere else — so a service overrides only the errors it cares about.
   *
   * @param fn the partial overrides
   * @return a total classifier
   */
  def refine(fn: PartialFunction[Any, ErrorType]): Classifier = value =>
    fn.lift(value) match
      case Some(errorType) => errorType
      case None            => defaultClassifier(value)
