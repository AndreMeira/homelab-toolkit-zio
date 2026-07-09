package homelab.auth


import homelab.common.error.ApplicationError.{ AdapterError, UnauthorisedError }
import homelab.common.types.SignedToken
import pdi.jwt.JwtClaim
import zio.*

import java.time.Instant


/**
 * A [[TokenVerifier]] decorator that caches successful verifications for a short `ttl`, so repeated
 * requests bearing the same token don't re-run the (potentially expensive) underlying verification.
 *
 * It exists chiefly to front [[K8sTokenReviewer]], whose `verify` is a network round-trip to the API
 * server on every call. Caching trades a little freshness for much less load: a token *revoked* upstream
 * (its bound pod/secret deleted) stays accepted until its cache entry goes stale, so `ttl` is the
 * freshness knob — keep it short (the apiserver itself caches webhook TokenReviews for ~2 minutes). A long
 * `ttl` means you may as well verify offline with [[JwksTokenVerifier]] instead.
 *
 * Only successful verifications are cached: an [[AdapterError]] (e.g. the API server unreachable) is
 * transient and always retried, and an entry never outlives the token's own `exp`. Failures pass straight
 * through. Stale entries are pruned as new ones are written, so the cache stays bounded by the number of
 * currently-valid distinct tokens.
 *
 * @param inner the verifier whose results are cached
 * @param ttl   how long a successful verification is reused before re-verifying
 * @param cache verified claims keyed by token, each with the instant it becomes stale
 */
final class CachedTokenVerifier private (
  inner: TokenVerifier,
  ttl: Duration,
  cache: Ref[Map[SignedToken, CachedTokenVerifier.Entry]],
) extends TokenVerifier:

  /**
   * Verify `token`, returning a cached result while one is still fresh.
   *
   * @param token the signed token to verify
   * @return the token's claims — cached if fresh, otherwise from `inner`; fails with whatever `inner` fails
   *         with (an `UnauthorisedError` for a bad token, an `AdapterError` for a verification failure)
   */
  override def verify(token: SignedToken): IO[AdapterError | UnauthorisedError, JwtClaim] =
    for
      now   <- Clock.instant
      hit   <- cache.get.map(_.get(token).filter(_.isFresh(now)))
      claim <- hit.fold(verifyAndStore(token, now))(entry => ZIO.succeed(entry.claim))
    yield claim

  /**
   * Verify `token` via `inner` and, on success, cache the claims until they go stale (pruning stale entries
   * as we write).
   *
   * @param token the token to verify
   * @param now   the current instant, used to bound the new entry's freshness and to prune
   * @return the verified claims; fails with `inner`'s error — nothing is cached on failure
   */
  private def verifyAndStore(token: SignedToken, now: Instant): IO[AdapterError | UnauthorisedError, JwtClaim] =
    for
      claim <- inner.verify(token)
      entry  = CachedTokenVerifier.Entry(claim, expirationFrom(now, claim))
      _     <- cache.update(_.filter { case (_, e) => e.isFresh(now) }.updated(token, entry))
    yield claim

  /**
   * The instant a fresh entry for `claim` becomes stale: `ttl` from now, but never past the token's `exp`.
   *
   * @param now   the current instant
   * @param claim the verified claims, whose `exp` caps the entry's lifetime
   * @return the earlier of `now + ttl` and the token's expiry (or just `now + ttl` if it carries no `exp`)
   */
  private def expirationFrom(now: Instant, claim: JwtClaim): Instant =
    val byTtl = now.plus(ttl)
    claim.expiration.map(Instant.ofEpochSecond) match
      case Some(exp) if exp.isBefore(byTtl) => exp
      case _                                => byTtl


object CachedTokenVerifier:

  /**
   * A short-lived cache over `inner`, reusing each successful verification for `ttl`.
   *
   * @param inner the verifier to cache
   * @param ttl   how long a successful verification is reused (default one minute)
   * @return the caching verifier
   */
  def make(inner: TokenVerifier, ttl: Duration = 1.minute): UIO[CachedTokenVerifier] =
    Ref.make(Map.empty[SignedToken, Entry]).map(new CachedTokenVerifier(inner, ttl, _))

  /** A cached verification: the token's claims and the instant the entry becomes stale. */
  final private case class Entry(claim: JwtClaim, staleAt: Instant):

    /**
     * Whether this entry is still fresh at `now`.
     *
     * @param now the current instant
     * @return `true` while `now` is before the entry's stale instant
     */
    def isFresh(now: Instant): Boolean = now.isBefore(staleAt)
