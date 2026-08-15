package homelab.common.store.inmemory


import homelab.common.store.Bucket
import zio.*


/**
 * In-memory [[Bucket]] backed by a [[Ref]] over an `Option`. Get, set and empty are each a single atomic
 * `Ref` operation, so concurrent access is safe without further locking. Never fails — the natural in-memory
 * stand-in for one slot of a persistent store.
 *
 * A `Ref[Option[A]]` rather than a one-key [[InMemoryKeyValueStore]]: a bucket holds exactly one value, so
 * there is nothing to look up and no map to allocate on every write.
 *
 * @param ref the backing cell, guarded by a `Ref` for atomic get / set / empty
 * @tparam A the value held
 */
final class InMemoryBucket[A](ref: Ref[Option[A]]) extends Bucket[A] {

  override def get: IO[Nothing, Option[A]] = ref.get

  override def set(value: A): IO[Nothing, A] = ref.set(Some(value)).as(value)

  override def empty: IO[Nothing, Boolean] = ref.modify(current => (current.isDefined, None))
}


object InMemoryBucket {

  /**
   * A bucket over a fresh, empty cell.
   *
   * @tparam A the value held
   * @return the new, empty bucket
   */
  def make[A]: UIO[InMemoryBucket[A]] = Ref.make(Option.empty[A]).map(new InMemoryBucket(_))

  /**
   * A bucket already holding `value`.
   *
   * @param value the value the bucket starts with
   * @tparam A the value held
   * @return the new, filled bucket
   */
  def of[A](value: A): UIO[InMemoryBucket[A]] = Ref.make(Option(value)).map(new InMemoryBucket(_))
}
