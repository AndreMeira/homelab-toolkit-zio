package homelab.common.error


import homelab.common.error.ApplicationError.DomainError
import zio.NonEmptyChunk


/**
 * Aggregates one or more validation failures so every input problem can be
 * reported together in a single pass (accumulating, not fail-fast).
 *
 * @param errors a non-empty collection of validation errors
 */
case class ValidationError(errors: NonEmptyChunk[ValidationError.InvalidInput]) extends DomainError:

  /**
   * Every problem, phrased for whoever sent the request.
   *
   * The problems' own `message`s rather than their `toString`s: this is what an adapter puts in a status
   * description or a response body, so it has to read as prose. `toString` carries the `kind` alongside it
   * and is for a log.
   *
   * @return the problems, joined
   */
  override def message: String = errors.map(_.message).mkString("; ")


object ValidationError:

  /**
   * Contract for a single validation-constraint violation.
   */
  trait InvalidInput:

    /** What was wrong, phrased for whoever sent it. */
    def message: String

    /**
     * A short, stable name for this kind of problem — for grouping, metrics and machine-readable codes.
     *
     * `productPrefix` first, because the obvious way to enumerate problems is a Scala 3 `enum` and a
     * singleton case of one is an anonymous class: `getClass.getSimpleName` answers with the empty string
     * for exactly the shape this contract invites. Cases with parameters and plain classes fall back to the
     * class name, which is what they are named.
     */
    def kind: String = this match
      case product: Product => product.productPrefix
      case _                => getClass.getSimpleName

    override def toString: String = s"{kind: $kind, message: $message}"
