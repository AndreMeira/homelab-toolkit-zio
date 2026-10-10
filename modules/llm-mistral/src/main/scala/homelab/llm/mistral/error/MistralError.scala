package homelab.llm.mistral.error

import homelab.common.error.ApplicationError


/**
 * What a call to Mistral refuses with.
 *
 * Every case is an `ApplicationError.AdapterError`, which is what `homelab.llm.Model` bounds its
 * failure by, and the ones a caller can act on differently carry a second marker: a retry is worth making
 * on an [[MistralError.Unavailable]], and is not on the rest.
 */
enum MistralError extends ApplicationError.AdapterError {

  /**
   * The call did not reach Mistral, or it answered that it could not serve this one now.
   *
   * @param detail what the transport or the API said
   */
  case Unavailable(detail: String) extends MistralError, ApplicationError.TransientError

  /**
   * The credentials were refused.
   *
   * @param detail what the API said
   */
  case Refused(detail: String) extends MistralError, ApplicationError.UnauthorisedError

  /**
   * The API answered, and the body is not what this client reads.
   *
   * @param detail what could not be read, and out of what
   */
  case Malformed(detail: String) extends MistralError, ApplicationError.DecodingError

  /**
   * The API rejected the request itself — an unknown model, a field its schema does not name, a tool-call
   * id it will not take, or messages in an order it does not accept.
   *
   * @param status what it answered with
   * @param detail what it said about it
   */
  case Rejected(status: Int, detail: String)

  /**
   * What went wrong, for whoever is reading the logs.
   *
   * @return the reason, naming the kind
   */
  override def message: String = this match
    case Unavailable(detail)      => s"mistral is unavailable: $detail"
    case Refused(detail)          => s"mistral refused the credentials: $detail"
    case Malformed(detail)        => s"mistral answered with something unreadable: $detail"
    case Rejected(status, detail) => s"mistral rejected the request with $status: $detail"
}
