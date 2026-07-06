package homelab.auth

import homelab.common.error.ApplicationError
import homelab.common.error.ApplicationError.AdapterError
import zio.*

/**
 * A [[JwksSource]] decorator that caches the fetched [[JsonWebKey.Set]] in a `Ref`, serving from memory
 * and refetching from `source` only when the cache is empty or a requested `kid` is absent. The latter
 * covers key rotation: a freshly-rotated signing key first shows up as an unknown `kid`, forcing a
 * refetch of the whole set.
 *
 * @param source the underlying source, fetched on a cache miss
 * @param cache  the last fetched key set, if any
 */
class CachedJwksSource(source: JwksSource, cache: Ref[Option[JsonWebKey.Set]]) extends JwksSource {

  /**
   * The full key set — cached if present, otherwise fetched and cached.
   *
   * @return the [[JsonWebKey.Set]]; fails with `source`'s `AdapterError` when a fetch is needed and fails
   */
  override def all: IO[ApplicationError.AdapterError, JsonWebKey.Set] =
    cache.get.flatMap:
      case Some(set) => ZIO.succeed(set)
      case None      => refreshedCache

  /**
   * The key for `keyId` — from the cached set if present, otherwise from a refetched set (so a rotated-in
   * key resolves on first use); `None` if still absent after a refetch.
   *
   * @param keyId the `kid` to resolve
   * @return the matching [[JsonWebKey]] or `None`; fails with `source`'s `AdapterError` if a refetch is needed and fails
   */
  override def get(keyId: String): IO[AdapterError, Option[JsonWebKey]] =
    for
      found <- cache.get.map:
                 case Some(set) => set.keys.find(_.keyId == keyId)
                 case None      => None
      found <- found match
                 case Some(found) => ZIO.succeed(Some(found))
                 case None        => refreshedCache.map(_.keys.find(_.keyId == keyId))
    yield found

  /**
   * Fetch the whole key set from `source` and replace the cache with it.
   *
   * @return the freshly fetched [[JsonWebKey.Set]]; fails with `source`'s `AdapterError`
   */
  private def refreshedCache: IO[AdapterError, JsonWebKey.Set] =
    source.all.tap(set => cache.set(Some(set)))
}
