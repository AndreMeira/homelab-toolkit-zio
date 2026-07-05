package homelab.incubator.auth.v1

import homelab.common.types.SignedToken
import zio.*

/** Verified claims lifted from a signed token — signature and expiry already checked by the verifier. */
final case class Claims(subject: String, name: String)

/**
 * Sketch seam: verify a signed token against the issuer's keys and return its claims. It separates the
 * JWT/JWKS crypto (a real impl would use jwt-scala + a JWKS key source) from the claim → principal
 * mapping the authenticators do. Its [[TokenVerifier.Failure]] distinguishes "the token is invalid"
 * from "I couldn't check it", so the authenticators classify the port error correctly.
 */
trait TokenVerifier:
  def verify(token: SignedToken): IO[TokenVerifier.Failure, Claims]

object TokenVerifier:
  enum Failure:
    /** Malformed, unsigned, tampered, or expired — the caller is unauthorised. */
    case Invalid(reason: String)

    /** Verification couldn't run (e.g. the key source is unreachable) — an infrastructure failure. */
    case Unavailable(reason: String, cause: Throwable)
