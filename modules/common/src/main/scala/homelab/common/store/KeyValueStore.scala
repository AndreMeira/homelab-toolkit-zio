package homelab.common.store

import homelab.common.error.ApplicationError.AdapterError
import zio.*

/**
 * Port: a generic key-value store over keys `K` and values `V` — get / upsert / delete. Implemented by
 * an adapter over whatever backend fits (in-memory, Redis, a database table).
 *
 * Absence is never a failure: [[get]] returns `None` for a missing key, and [[delete]] reports whether
 * the key existed (`false` = already gone) instead of erroring — so callers decide what a missing key
 * means rather than catching it. The only error is `AdapterError` — the opaque infrastructure umbrella;
 * the store surfaces no domain outcomes.
 *
 * @tparam K the key type
 * @tparam V the stored value type
 */
trait KeyValueStore[K, V] {

  /**
   * Look up the value stored under `key`.
   *
   * @return the value, or `None` if the key is absent; fails with `AdapterError` on an infrastructure failure
   */
  def get(key: K): IO[AdapterError, Option[V]]

  /**
   * Store `value` under `key`, overwriting any existing value (upsert).
   *
   * @return unit; fails with `AdapterError` on an infrastructure failure
   */
  def set(key: K, value: V): IO[AdapterError, Unit]

  /**
   * Remove `key`.
   *
   * @return `true` if the key existed and was removed, `false` if it was already absent; fails with
   *         `AdapterError` on an infrastructure failure
   */
  def delete(key: K): IO[AdapterError, Boolean]
}
