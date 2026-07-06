package homelab.incubator.auth.v1


import homelab.common.types.SignedToken
import pdi.jwt.{ Jwt, JwtClaim }
import pdi.jwt.algorithms.JwtHmacAlgorithm
import zio.*
import zio.json.*


/**
 * Sketch: a [[TokenVerifier]] backed by jwt-scala. Verifies the token's signature and expiry against
 * `secret`/`algorithm`, then reads the standard `sub` claim and a custom `name` claim.
 *
 * Uses an HMAC secret here for testability; the real homelab verifier would verify EdDSA signatures
 * against the public key from the issuer's JWKS — same shape, but an asymmetric key and a key source
 * that can be `Unavailable` (which is why [[TokenVerifier.Failure]] keeps that case).
 */
final class JwtTokenVerifier(secret: String, algorithm: JwtHmacAlgorithm) extends TokenVerifier:

  def verify(token: SignedToken): IO[TokenVerifier.Failure, Claims] =
    ZIO.suspendSucceed {
      Jwt.decode(token, secret, Seq(algorithm)).fold(error => ZIO.fail(classify(error)), fromClaim)
    }


  private def fromClaim(claim: JwtClaim): IO[TokenVerifier.Failure, Claims] =
    for
      subject <- ZIO.fromOption(claim.subject).orElseFail(TokenVerifier.Failure.Invalid("missing subject claim"))
      name    <- ZIO
                   .fromEither(claim.content.fromJson[JwtTokenVerifier.NameClaim])
                   .mapError(reason => TokenVerifier.Failure.Invalid(s"malformed claims: $reason"))
    yield Claims(subject, name.name)


  // jwt-scala's decode failures (bad signature, expired, malformed) are all token-level → Invalid. A
  // JWKS-backed impl would additionally surface Unavailable when the key source can't be reached.
  private def classify(error: Throwable): TokenVerifier.Failure =
    TokenVerifier.Failure.Invalid(Option(error.getMessage).getOrElse(error.toString))


object JwtTokenVerifier:
  final private case class NameClaim(name: String) derives JsonDecoder
