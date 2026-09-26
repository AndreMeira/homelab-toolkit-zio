package homelab.llm.openai.error

import homelab.common.error.ApplicationError


/**
 * What a chat-completions call refuses with.
 *
 * Every case is an [[ApplicationError.AdapterError]], which is what [[homelab.llm.Model]] bounds its
 * failure by, and the ones a caller can act on differently carry a second marker: a retry is worth making
 * on an [[Unavailable]], and is not on the rest.
 *
 * The kinds are the protocol's rather than any one provider's, so the same four serve every endpoint that
 * speaks it.
 */
enum ChatCompletionError extends ApplicationError.AdapterError {

  /**
   * The call did not reach the provider, or it answered that it could not serve this one now.
   *
   * @param detail what the transport or the provider said
   */
  case Unavailable(detail: String) extends ChatCompletionError, ApplicationError.TransientError

  /**
   * The provider refused the credentials.
   *
   * @param detail what it said
   */
  case Refused(detail: String) extends ChatCompletionError, ApplicationError.UnauthorisedError

  /**
   * The provider answered, and the body is not what this adapter reads.
   *
   * @param detail what could not be read, and out of what
   */
  case Malformed(detail: String) extends ChatCompletionError, ApplicationError.DecodingError

  /**
   * The provider rejected the request itself — an unknown model, a schema it will not accept, a body that
   * does not parse.
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
    case Unavailable(detail)      => s"the provider is unavailable: $detail"
    case Refused(detail)          => s"the provider refused the credentials: $detail"
    case Malformed(detail)        => s"the provider answered with something unreadable: $detail"
    case Rejected(status, detail) => s"the provider rejected the request with $status: $detail"
}
