package homelab.common.auth


import homelab.common.auth.Requester.Service
import homelab.common.error.ApplicationError.{ AdapterError, UnauthorisedError }
import homelab.common.types.SignedToken
import zio.*


/**
 * Port: verifies a service-to-service token and reconstructs the calling [[Requester.Service]].
 * Implemented by an adapter that checks the token against the issuer's keys (e.g. a JWKS endpoint).
 */
trait ServiceAuthenticator {

  /**
   * Authenticate a calling service from its bearer token.
   *
   * @param token the signed service token presented on the call
   * @return the calling service; fails with `UnauthorisedError` if the token is missing, invalid, or
   *         expired, or with `AdapterError` if the check itself can't run (e.g. the key source is unreachable)
   */
  def authenticate(token: SignedToken): IO[AdapterError | UnauthorisedError, Service]
}
