package homelab.common.auth


import homelab.common.auth.Requester.User
import homelab.common.error.ApplicationError.{ AdapterError, UnauthorisedError }
import homelab.common.types.*
import zio.*


/**
 * Port: verifies a user token and reconstructs the calling [[Requester.User]]. Two entry points, one
 * per authorization posture a route takes — optional auth ([[any]]) vs required auth ([[authenticate]]).
 */
trait UserAuthenticator {

  /**
   * Optional auth — for routes that also serve anonymous callers. A *missing* token yields
   * [[Requester.User.Anonymous]] and a valid one a [[Requester.User.Authenticated]]; a *present-but-invalid*
   * token is rejected with `UnauthorisedError` (whether to tolerate that as anonymous is the application's
   * decision, not this port's).
   *
   * @param token the bearer token, if one was presented
   * @return the caller (anonymous or authenticated); fails with `UnauthorisedError` if a token was
   *         presented but is invalid or expired, or with `AdapterError` on an infrastructure failure
   */
  def any(token: Option[SignedToken]): IO[AdapterError | UnauthorisedError, User]

  /**
   * Required auth — for routes that mandate a signed-in user.
   *
   * @param token the bearer token presented on the request
   * @return the authenticated caller; fails with `UnauthorisedError` if the token is missing, invalid,
   *         or expired, or with `AdapterError` on an infrastructure failure
   */
  def authenticate(token: SignedToken): IO[AdapterError | UnauthorisedError, User.Authenticated]
}
