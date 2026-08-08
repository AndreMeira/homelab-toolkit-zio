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
trait KeyValueStore[-K, V] {
  self =>

  /**
   * Look up the value stored under `key`.
   *
   * @return the value, or `None` if the key is absent; fails with `AdapterError` on an infrastructure failure
   */
  def get(key: K): IO[AdapterError, Option[V]]

  /**
   * Store `value` under `key`, overwriting any existing value (upsert).
   *
   * @return noop; fails with `AdapterError` on an infrastructure failure
   */
  def set(key: K, value: V): IO[AdapterError, Unit]

  /**
   * Remove `key`.
   *
   * @return `true` if the key existed and was removed, `false` if it was already absent; fails with
   *         `AdapterError` on an infrastructure failure
   */
  def delete(key: K): IO[AdapterError, Boolean]

  /**
   * Re-key this store: adapt it to keys `K2` by mapping each incoming key to a `K` before delegating. Every
   * operation applies `fn` to its key and runs against this store; the stored value type `V` is unchanged.
   *
   * @param fn maps an incoming `K2` key to this store's `K`
   * @tparam K2 the adapted key type
   * @return a store keyed by `K2` that runs each operation on `fn(key)` through this one
   */
  def contramap[K2](fn: K2 => K): KeyValueStore[K2, V] = new KeyValueStore[K2, V]:
    def get(key: K2): IO[AdapterError, Option[V]]      = self.get(fn(key))
    def set(key: K2, value: V): IO[AdapterError, Unit] = self.set(fn(key), value)
    def delete(key: K2): IO[AdapterError, Boolean]     = self.delete(fn(key))
}
