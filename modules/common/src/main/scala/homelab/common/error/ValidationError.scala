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
  override def message: String = errors.map(_.toString).mkString(", ")


object ValidationError:

  /**
   * Contract for a single validation-constraint violation.
   */
  trait InvalidInput:
    def message: String
    def kind: String              = getClass.getSimpleName
    override def toString: String = s"{kind: $kind, message: $message}"
