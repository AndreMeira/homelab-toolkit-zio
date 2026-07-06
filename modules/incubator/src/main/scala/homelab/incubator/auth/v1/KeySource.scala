package homelab.incubator.auth.v1


import zio.*

import java.security.PublicKey


/**
 * Sketch port: resolve a signing key id (`kid`, from a JWT header) to the issuer's public key, backed by
 * a JWKS the issuer publishes. This is where the real `Unavailable` failure lives — fetching the key set
 * can fail — as distinct from [[KeySource.Failure.UnknownKey]], which means the set was read but has no
 * such key (an untrusted token).
 */
trait KeySource:
  def publicKey(keyId: String): IO[KeySource.Failure, PublicKey]


object KeySource:

  enum Failure:
    /** The key set was reachable but has no key with this id — the token is untrusted (→ invalid). */
    case UnknownKey(keyId: String)

    /** The key set couldn't be fetched (e.g. the JWKS endpoint is unreachable) — infrastructure. */
    case Unavailable(reason: String, cause: Throwable)
