package homelab.incubator.auth.v1


import zio.*

import java.security.PublicKey


/**
 * Sketch: a caching [[KeySource]] over a JWKS. Keeps the fetched keys in a `Ref` and, on a cache miss,
 * refetches once (issuers rotate keys, so an unknown `kid` may just mean "the cache is stale") before
 * giving up with [[KeySource.Failure.UnknownKey]]. A failed fetch surfaces as
 * [[KeySource.Failure.Unavailable]].
 *
 * `fetchAll` — the raw JWKS fetch + parse — is injected, so the caching / refresh / failure logic is
 * testable without HTTP. A real one would GET the issuer's `.well-known/jwks.json`, parse each JWK, and
 * rebuild the `PublicKey` (EdDSA `x` coordinate → `EdECPublicKey`, RSA `n`/`e` → `RSAPublicKey`).
 *
 * Not single-flighted: concurrent misses on the same key each refetch. A production version would dedupe
 * in-flight refreshes (e.g. via `Ref.Synchronized`) and add a TTL.
 */
final class JwksKeySource(cache: Ref[Map[String, PublicKey]], fetchAll: JwksKeySource.FetchAll) extends KeySource:

  def publicKey(keyId: String): IO[KeySource.Failure, PublicKey] =
    cache.get.flatMap { keys =>
      keys.get(keyId) match
        case Some(key) => ZIO.succeed(key)
        case None      => refreshAndLookup(keyId)
    }


  private def refreshAndLookup(keyId: String): IO[KeySource.Failure, PublicKey] =
    for
      fresh <- fetchAll
      _     <- cache.set(fresh)
      key   <- ZIO.fromOption(fresh.get(keyId)).orElseFail(KeySource.Failure.UnknownKey(keyId))
    yield key


object JwksKeySource:

  /** The raw JWKS fetch: all published keys by id, or an infrastructure failure (an `Unavailable`). */
  type FetchAll = IO[KeySource.Failure, Map[String, PublicKey]]


  /** An empty-cache key source over the given fetch. */
  def make(fetchAll: FetchAll): UIO[JwksKeySource] =
    Ref.make(Map.empty[String, PublicKey]).map(new JwksKeySource(_, fetchAll))
