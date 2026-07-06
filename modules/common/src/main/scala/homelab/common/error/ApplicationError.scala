package homelab.common.error


/**
 * Base trait for all application errors in the registration authenticate.
 *
 * Root error type for the whole application: every domain-specific error extends
 * this trait directly or one of the marker subtypes below. Each error carries a
 * human-readable [[message]] and a [[kind]] used for categorisation/logging.
 */
trait ApplicationError:
  /**
   * A human-readable message describing the error.
   */
  def message: String

  /**
   * The kind/type of error, derived from the class name.
   */
  def kind: String = getClass.getSimpleName

  override def toString: String = s"{kind: $kind, message: $message}"


object ApplicationError:

  /**
   * Temporary issues that may succeed on retry (network blips, resource contention).
   */
  trait TransientError extends ApplicationError

  /**
   * Failures arising from interactions with external systems (DB, IdP, …).
   */
  trait VendorError extends ApplicationError

  /**
   * Critical failures the application cannot recover from.
   */
  trait UnrecoverableError extends ApplicationError

  /**
   * Failures during network communication.
   */
  trait NetworkError extends ApplicationError

  /**
   * Failures during interactions with persistent storage.
   */
  trait PersistenceError extends ApplicationError

  /**
   * Failures during interactions with external adapters or services.
   */
  trait AdapterError extends ApplicationError

  /**
   * Failures during decoding or parsing of data.
   */
  trait DecodingError extends ApplicationError

  /**
   * Business-logic violations and constraint failures — expected, recoverable.
   */
  trait DomainError extends ApplicationError

  /**
   * Constraint violations such as creating something that already exists.
   */
  trait ConflictError extends DomainError

  /**
   * TokenIssuer/authorisation failures.
   */
  trait UnauthorisedError extends DomainError

  /**
   * Internal state is unexpected/invalid — typically a bug or data corruption.
   */
  trait InconsistentState extends DomainError

  /**
   * A requested resource cannot be found.
   */
  trait NotFoundError extends DomainError
