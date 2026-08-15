package homelab.common.store


import homelab.common.error.ApplicationError.AdapterError
import homelab.common.store.inmemory.InMemoryKeyValueStore
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
  self =>

  /**
   * Look up the value stored under `key`.
   *
   * @param key the key to look up
   * @return the value, or `None` if the key is absent; fails with `AdapterError` on an infrastructure failure
   */
  def get(key: K): IO[AdapterError, Option[V]]

  /**
   * Store `value` under `key`, overwriting any existing value (upsert).
   *
   * @param key the key to store under
   * @param value the value to store
   * @return noop; fails with `AdapterError` on an infrastructure failure
   */
  def set(key: K, value: V): IO[AdapterError, Unit]

  /**
   * Remove `key`.
   *
   * @param key the key to remove
   * @return `true` if the key existed and was removed, `false` if it was already absent; fails with
   *         `AdapterError` on an infrastructure failure
   */
  def delete(key: K): IO[AdapterError, Boolean]

  /**
   * Read `key`, computing and storing its value when the slot is empty — the store as a cache: a hit returns
   * what is there and runs nothing, a miss runs `fn` and writes the result before returning it. `fn` receives
   * the key it is computing for, so one function can serve every slot.
   *
   * Not atomic. Concurrent misses on one key each run `fn` and the last write wins, so this suits values that
   * are expensive but reproducible (a fetched JWKS key, a derived projection). Where the computation must run
   * exactly once, guard the call with a lock (see [[homelab.common.flow.KeyLock]]).
   *
   * @param key the key to read, and the key `fn` computes for on a miss
   * @param fn  computes the value for a missing key
   * @tparam R  the environment `fn` needs
   * @tparam E2 the error `fn` may fail with
   * @return the stored value, or the freshly computed and stored one; fails with `AdapterError` if the store
   *         fails, or `E2` if the computation does
   */
  def computed[R, E2](key: K)(fn: K => ZIO[R, E2, V]): ZIO[R, AdapterError | E2, V] =
    get(key).someOrElseZIO(fn(key).tap(value => set(key, value)))

  /**
   * View this store as one keyed by a *subtype* of `K` — every `K1` is already a `K`, so each operation
   * delegates unchanged. `K` is invariant (it is both an input to [[get]]/[[delete]] and, via [[bucket]],
   * captured), so this recovers by hand the widening that contravariance would otherwise give: a
   * `KeyValueStore[Any, V]` can serve a caller that only has `String` keys. For a key that is not a subtype,
   * use [[contramap]].
   *
   * @tparam K1 the narrower key type this view accepts
   * @return the same store, accepting only `K1` keys
   */
  def narrow[K1 <: K]: KeyValueStore[K1, V] = new KeyValueStore[K1, V] {
    override def set(key: K1, value: V): IO[AdapterError, Unit] = self.set(key, value)
    override def get(key: K1): IO[AdapterError, Option[V]]      = self.get(key)
    override def delete(key: K1): IO[AdapterError, Boolean]     = self.delete(key)
  }

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

  /**
   * View this store as a [[Memo]]: the same slots, but exposed as a derivation rather than as storage.
   * Callers of the result can compute-and-retain but cannot `set`, `delete`, or read a slot directly, so the
   * narrower capability is visible in the type.
   *
   * Effectful because it provisions a [[homelab.common.flow.KeyLock]] of its own — that lock is the whole
   * difference between this and [[computed]]: concurrent misses on one key share a single computation here,
   * whereas [[computed]] lets each of them run and the last write win.
   *
   * @return a memo retaining its results in this store, with a fresh lock
   */
  def memoized: UIO[Memo[K, V]] = Memo.make(self)

  /**
   * Pin this store to one `key`, yielding a [[Bucket]] — a single-slot view whose operations take no key at
   * all. For a caller that only ever touches one slot (a singleton config, one entity's state), this closes
   * over the key once instead of threading it through every call, and makes the narrower capability visible
   * in the type: a `Bucket` cannot reach any other key.
   *
   * @param key the key every operation on the returned bucket resolves to
   * @return a single-slot view of this store at `key`
   */
  def bucket(key: K): Bucket[V] = new Bucket[V]:
    override def get: IO[AdapterError, Option[V]]   = self.get(key)
    override def empty: IO[AdapterError, Boolean]   = self.delete(key)
    override def set(value: V): IO[AdapterError, V] = self.set(key, value).as(value)
}


/** Companion of the [[KeyValueStore]] port, carrying the constructor for its dependency-free adapter. */
object KeyValueStore:

  /**
   * A store over a fresh, empty [[InMemoryKeyValueStore]] — the port's trivial implementation, offered here
   * so a caller that needs *a* store (a test, a single-node run, a default) does not have to name an adapter.
   * Anything durable is wired explicitly from its own module.
   *
   * @tparam K the key type
   * @tparam V the stored value type
   * @return the new, empty in-memory store
   */
  def inmemory[K, V]: UIO[InMemoryKeyValueStore[K, V]] = InMemoryKeyValueStore.make[K, V]
  
  