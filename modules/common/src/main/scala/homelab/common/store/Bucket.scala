package homelab.common.store


import homelab.common.error.ApplicationError.AdapterError
import homelab.common.store.inmemory.InMemoryBucket
import zio.*


/**
 * Port: a single storage slot holding at most one `A` — read it, fill it, empty it. A [[KeyValueStore]] with
 * the key already chosen ([[KeyValueStore.bucket]]) and therefore no longer reachable, so a holder can touch
 * this value and no other: the narrowing is the point, not the convenience.
 *
 * Absence is never a failure: [[get]] returns `None` for an empty slot and [[empty]] reports whether there
 * was anything to remove, so callers decide what emptiness means rather than catching it. The only error is
 * `AdapterError` — the opaque infrastructure umbrella; a bucket surfaces no domain outcomes.
 *
 * @tparam A the value held
 */
trait Bucket[A]:

  /**
   * Fill the slot with `value`, replacing whatever was there.
   *
   * @param value the value to store
   * @return `value`, so a caller can carry on with it without re-reading; fails with `AdapterError` on an
   *         infrastructure failure
   */
  def set(value: A): IO[AdapterError, A]

  /**
   * Read the slot.
   *
   * @return the value, or `None` if the slot is empty; fails with `AdapterError` on an infrastructure failure
   */
  def get: IO[AdapterError, Option[A]]

  /**
   * Empty the slot.
   *
   * @return `true` if it held a value and now does not, `false` if it was already empty; fails with
   *         `AdapterError` on an infrastructure failure
   */
  def empty: IO[AdapterError, Boolean]


/** Companion of the [[Bucket]] port, carrying the constructor for its dependency-free adapter. */
object Bucket:

  /**
   * A slot of its own, backed by a fresh [[InMemoryBucket]] — for a caller that needs somewhere to put one
   * value and does not care where. Nothing is shared: each call allocates its own cell, so two buckets built
   * this way never see each other's contents.
   *
   * @tparam A the value held
   * @return the new, empty slot
   */
  def inmemory[A]: UIO[Bucket[A]] = InMemoryBucket.make[A]
