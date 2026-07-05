package homelab.incubator.auth.v1

import homelab.common.auth.Requester.Service
import homelab.common.auth.ServiceAuthenticator
import homelab.common.error.ApplicationError.{ AdapterError, UnauthorisedError }
import homelab.common.types.{ ServiceName, SignedToken }
import zio.*

/**
 * Sketch: [[ServiceAuthenticator]] over a [[TokenVerifier]]. Verifies the service token and builds the
 * calling [[Service]] from its subject claim, translating the verifier's failure into the port's error
 * channel — an invalid token → `UnauthorisedError`, an unreachable key source → `AdapterError`.
 */
final class JwtServiceAuthenticator(verifier: TokenVerifier) extends ServiceAuthenticator:

  def authenticate(token: SignedToken): IO[AdapterError | UnauthorisedError, Service] =
    verifier
      .verify(token)
      .mapError(toApplicationError)
      .map(claims => Service(ServiceName(claims.subject)))

  private def toApplicationError(failure: TokenVerifier.Failure): AdapterError | UnauthorisedError =
    failure match
      case TokenVerifier.Failure.Invalid(reason)            => InvalidToken(reason)
      case TokenVerifier.Failure.Unavailable(reason, cause) => KeySourceUnavailable(reason, cause)
