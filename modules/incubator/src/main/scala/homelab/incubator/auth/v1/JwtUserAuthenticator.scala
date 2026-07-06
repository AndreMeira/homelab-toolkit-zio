package homelab.incubator.auth.v1


import homelab.common.auth.Requester.User
import homelab.common.auth.UserAuthenticator
import homelab.common.error.ApplicationError.{ AdapterError, UnauthorisedError }
import homelab.common.types.{ SignedToken, UserId, UserName }
import zio.*

import java.util.UUID


/**
 * Sketch: [[UserAuthenticator]] over a [[TokenVerifier]].
 *
 *   - `authenticate` (required) verifies the token and builds the [[User.Authenticated]] caller.
 *   - `any` (optional) never rejects: no token, an invalid token, or a token whose subject isn't a
 *     valid user id all yield [[User.Anonymous]]; only an infrastructure failure escapes (as
 *     `AdapterError`). Matches the port's documented posture.
 */
final class JwtUserAuthenticator(verifier: TokenVerifier) extends UserAuthenticator:

  def authenticate(token: SignedToken): IO[AdapterError | UnauthorisedError, User.Authenticated] =
    verifier
      .verify(token)
      .mapError(toApplicationError)
      .flatMap(toAuthenticated)

  def any(token: Option[SignedToken]): IO[AdapterError, User] =
    token match
      case None    => ZIO.succeed(User.Anonymous)
      case Some(t) =>
        authenticate(t).catchAll {
          case _: UnauthorisedError => ZIO.succeed(User.Anonymous) // present-but-invalid → anonymous
          case infra: AdapterError  => ZIO.fail(infra)             // infrastructure failure still escapes
        }

  private def toAuthenticated(claims: Claims): IO[UnauthorisedError, User.Authenticated] =
    ZIO
      .attempt(UUID.fromString(claims.subject))
      .mapBoth(_ => InvalidToken(s"subject is not a valid user id: ${claims.subject}"), UserId(_))
      .map(id => User.Authenticated(id, UserName(claims.name)))

  private def toApplicationError(failure: TokenVerifier.Failure): AdapterError | UnauthorisedError =
    failure match
      case TokenVerifier.Failure.Invalid(reason)            => InvalidToken(reason)
      case TokenVerifier.Failure.Unavailable(reason, cause) => KeySourceUnavailable(reason, cause)
