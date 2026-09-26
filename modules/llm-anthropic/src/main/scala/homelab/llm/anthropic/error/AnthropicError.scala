package homelab.llm.anthropic.error

import homelab.common.error.ApplicationError


/**
 * What a Messages call refuses with.
 *
 * Every case is an [[ApplicationError.AdapterError]], which is what [[homelab.llm.Model]] bounds its
 * failure by, and the ones a caller can act on differently carry a second marker: a retry is worth making
 * on an [[Unavailable]], and is not on the rest.
 */
enum AnthropicError extends ApplicationError.AdapterError {

  /**
   * The call did not reach Anthropic, or it answered that it could not serve this one now.
   *
   * @param detail what the transport or the API said
   */
  case Unavailable(detail: String) extends AnthropicError, ApplicationError.TransientError

  /**
   * The credentials were refused.
   *
   * @param detail what the API said
   */
  case Refused(detail: String) extends AnthropicError, ApplicationError.UnauthorisedError

  /**
   * The API answered, and the body is not what this adapter reads.
   *
   * @param detail what could not be read, and out of what
   */
  case Malformed(detail: String) extends AnthropicError, ApplicationError.DecodingError

  /**
   * The API rejected the request itself — an unknown model, a schema it will not accept, a body that does
   * not parse, or one missing something it requires.
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
    case Unavailable(detail)      => s"anthropic is unavailable: $detail"
    case Refused(detail)          => s"anthropic refused the credentials: $detail"
    case Malformed(detail)        => s"anthropic answered with something unreadable: $detail"
    case Rejected(status, detail) => s"anthropic rejected the request with $status: $detail"
}

object AnthropicError:
  /**
   * A response came back from the model with no content and no finished reason.
   *
   * @return AnthropicError.Malformed the error with the appropriate message
   */
  def noResponseContent: AnthropicError.Malformed =
    AnthropicError.Malformed("the response carried no content and no reason for stopping")
