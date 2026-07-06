package homelab.auth

import homelab.common.error.ApplicationError.{ AdapterError, DecodingError }
import homelab.common.types.SignedToken
import homelab.auth.CachedTokenProvider.TokenExpiryUnreadable
import pdi.jwt.{ Jwt, JwtClaim, JwtOptions }
import zio.*

import java.time.Instant

/**
 * A [[JwtProvider]] decorator that caches the current token and hands it back on every call until it's
 * within `refreshSkew` of its own `exp`, at which point it refetches from `source`.
 *
 * This sits over a [[ProjectedTokenProvider]] (or any [[JwtProvider]]): the underlying read is cheap but
 * per-call, and the cluster token is valid for ~an hour, so caching avoids re-reading the file on every
 * request while `refreshSkew` guarantees we roll over to the freshly-rotated token before the held one
 * actually expires. Expiry is read from the token's own `exp` claim — decoded **without** signature or
 * time verification, since this is our *own* outbound credential, not one we're authenticating.
 *
 * @param source      where a fresh token comes from when the cache is empty or stale
 * @param refreshSkew how far before `exp` to consider the cached token due for refresh
 * @param cache       the current token and the instant at which it becomes due for refresh
 */
final class CachedTokenProvider private (
  source: JwtProvider,
  refreshSkew: Duration,
  cache: Ref[Option[CachedTokenProvider.Entry]],
) extends JwtProvider:

  /**
   * The current token: the cached one while it's still fresh, otherwise a refetched one.
   *
   * @return the token; fails with whatever `source` fails with, or
   *         [[CachedTokenProvider.TokenExpiryUnreadable]] if a refetched token's `exp` can't be read
   */
  def get: IO[AdapterError, SignedToken] =
    for
      now   <- Clock.instant
      token <- cache.get.flatMap:
                 case Some(entry) if entry.isFresh(now) => ZIO.succeed(entry.token)
                 case _                                 => refresh(now)
    yield token

  /**
   * Fetch a fresh token, cache it with its skew-adjusted refresh instant, and return it.
   *
   * @param now the current instant, used only if the token carries no usable expiry
   * @return the fresh token; fails with `source`'s error, or [[CachedTokenProvider.TokenExpiryUnreadable]]
   *         if its `exp` can't be read
   */
  private def refresh(now: Instant): IO[AdapterError, SignedToken] =
    for
      token  <- source.get
      expiry <- expiryOf(token)
      entry   = CachedTokenProvider.Entry(token, expiry.minus(refreshSkew))
      _      <- cache.set(Some(entry))
    yield token

  /**
   * Read the `exp` claim from a token, decoding it without any verification.
   *
   * @param token the token to inspect
   * @return the expiry instant; fails with [[CachedTokenProvider.TokenExpiryUnreadable]] if the token
   *         can't be decoded or carries no `exp`
   */
  private def expiryOf(token: SignedToken): IO[AdapterError, Instant] =
    for
      claim  <- claimOf(token)
      expiry <- expiryOf(claim)
    yield expiry

  /**
   * Decode a token into its claims, without any signature or time verification.
   *
   * @param token the token to decode
   * @return the decoded claims; fails with [[CachedTokenProvider.TokenExpiryUnreadable]] if the token can't be decoded
   */
  private def claimOf(token: SignedToken): IO[AdapterError, JwtClaim] = {
    val decoded = Jwt.decode(token, JwtOptions(signature = false, expiration = false, notBefore = false))
    ZIO.fromTry(decoded).mapError(err => TokenExpiryUnreadable(err.getMessage))
  }

  /**
   * Extract the expiry instant from decoded claims.
   *
   * @param claim the decoded claims
   * @return the expiry instant; fails with [[CachedTokenProvider.TokenExpiryUnreadable]] if there's no `exp` claim
   */
  private def expiryOf(claim: JwtClaim): IO[AdapterError, Instant] =
    ZIO
      .fromOption(claim.expiration)
      .map(Instant.ofEpochSecond)
      .orElseFail(TokenExpiryUnreadable("token has no exp claim"))

object CachedTokenProvider:

  /**
   * A cache over `source` that refreshes `refreshSkew` before each token's expiry.
   *
   * @param source      where a fresh token comes from
   * @param refreshSkew how far before `exp` to refresh (default one minute)
   * @return the caching provider
   */
  def make(source: JwtProvider, refreshSkew: Duration = 1.minute): UIO[CachedTokenProvider] =
    Ref.make(Option.empty[Entry]).map(new CachedTokenProvider(source, refreshSkew, _))

  /** The cached token and the instant from which it's considered due for refresh (`exp` minus the skew). */
  final private case class Entry(token: SignedToken, refreshAt: Instant):

    /**
     * Whether the cached token is still fresh — not yet due for refresh — at `now`.
     *
     * @param now the current instant
     * @return `true` while `now` is before the refresh instant
     */
    def isFresh(now: Instant): Boolean = now.isBefore(refreshAt)

  /** A refetched token couldn't be decoded, or carried no `exp` — so its lifetime is unknown. */
  final case class TokenExpiryUnreadable(reason: String) extends DecodingError, AdapterError:
    override def message: String = s"could not read the token's expiry: $reason"
