package homelab.incubator.auth.v1

import homelab.common.error.ApplicationError.{ AdapterError, TransientError, UnauthorisedError }

/** A presented token failed verification — the caller is unauthorised (→ 401). */
final case class InvalidToken(message: String) extends UnauthorisedError

/** The token key source (JWKS) couldn't be reached — a retryable infrastructure failure (→ 503). */
final case class KeySourceUnavailable(message: String, cause: Throwable) extends AdapterError, TransientError
