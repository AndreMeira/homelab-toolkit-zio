package homelab.inmemory.store


import homelab.common.store.KeyValueStore
import zio.*


/**
 * In-memory [[KeyValueStore]] backed by a [[Ref]] over an immutable `Map`. Get, upsert and delete are each
 * a single atomic `Ref` operation, so concurrent access is safe without further locking. Never fails — the
 * natural in-memory stand-in for a persistent key-value backend in tests and single-node runs.
 *
 * @param ref the backing map, guarded by a `Ref` for atomic get / upsert / delete
 * @tparam K the key type
 * @tparam V the stored value type
 */
final class InMemoryKeyValueStore[K, V](ref: Ref[Map[K, V]]) extends KeyValueStore[K, V] {

  override def get(key: K): IO[Nothing, Option[V]] = ref.get.map(_.get(key))

  override def set(key: K, value: V): IO[Nothing, Unit] = ref.update(_.updated(key, value))

  override def delete(key: K): IO[Nothing, Boolean] = ref.modify(map => (map.contains(key), map - key))
}


object InMemoryKeyValueStore {

  /**
   * A store over a fresh, empty map.
   *
   * @tparam K the key type
   * @tparam V the stored value type
   * @return the new, empty store
   */
  def make[K, V]: UIO[InMemoryKeyValueStore[K, V]] =
    Ref.make(Map.empty[K, V]).map(new InMemoryKeyValueStore(_))

  /**
   * A store seeded with `initial`'s entries.
   *
   * @param initial the entries the store starts with
   * @tparam K the key type
   * @tparam V the stored value type
   * @return the new store holding `initial`
   */
  def fromMap[K, V](initial: Map[K, V]): UIO[InMemoryKeyValueStore[K, V]] =
    Ref.make(initial).map(new InMemoryKeyValueStore(_))
}
