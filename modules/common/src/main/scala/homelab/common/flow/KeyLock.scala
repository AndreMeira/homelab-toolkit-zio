package homelab.common.flow

import zio.*

/**
 * A lock keyed by `K`: [[withPermit]] serialises effects that share a key, while effects under different
 * keys run freely. Each key's mutual exclusion is a [[zio.Semaphore]] made on first use and evicted once no
 * effect holds or awaits it, so idle keys cost nothing. Not reentrant — a `withPermit(key)` nested inside
 * another `withPermit(key)` for the same key deadlocks on itself.
 *
 * @tparam K the key type
 */
trait KeyLock[K] {

  /**
   * Run `zio` while holding `key`'s permit, so it never overlaps another effect under the same key. Effects
   * under other keys are unaffected. The permit is released when `zio` finishes, fails, or is interrupted.
   *
   * @param key the key to serialise on
   * @param zio the effect to run under the key's permit
   * @tparam R the effect's environment
   * @tparam E the effect's error
   * @tparam A the effect's result
   * @return `zio`'s result, run in mutual exclusion per `key`; fails with `E` if `zio` does
   */
  def withPermit[R, E, A](key: K)(zio: ZIO[R, E, A]): ZIO[R, E, A]
}

object KeyLock:

  /**
   * A fresh key lock: each key gets its own [[zio.Semaphore]], made on first use and evicted when idle.
   *
   * @tparam K the key type
   * @return a new [[KeyLock]] with no keys held
   */
  def make[K]: UIO[KeyLock[K]] =
    Ref.Synchronized.make(Map.empty[K, (Semaphore, Int)]).map(new Apply(_))

  /**
   * The map-backed [[KeyLock]]: a `Ref.Synchronized` from key to its `(semaphore, holder count)` pair. A
   * key's semaphore is created on the first [[withPermit]] and dropped when the last holder releases, so the
   * map only ever holds currently-contended keys.
   *
   * @param ref the key → (semaphore, holder count) table
   * @tparam K the key type
   */
  private class Apply[K](ref: Ref.Synchronized[Map[K, (Semaphore, Int)]]) extends KeyLock[K]:
    def withPermit[R, E, A](key: K)(zio: ZIO[R, E, A]): ZIO[R, E, A] =
      ZIO.acquireReleaseWith(acquire(key))(_ => release(key)) { sem =>
        sem.withPermit(zio)
      }

    /**
     * Get `key`'s semaphore — creating it with a single permit if absent — and record one more holder. The
     * increment and the map insert happen atomically inside the `Ref.Synchronized`.
     *
     * @param key the key to acquire the semaphore for
     * @return the key's semaphore, its holder count now bumped by one
     */
    private def acquire(key: K): UIO[Semaphore] =
      ref.modifyZIO { map =>
        map.get(key) match
          case Some((s, n)) => ZIO.succeed((s, map.updated(key, (s, n + 1))))
          case None         => Semaphore.make(1).map(s => (s, map.updated(key, (s, 1))))
      }

    /**
     * Drop one holder of `key`, evicting the entry entirely once the last holder leaves so idle keys don't
     * accumulate.
     *
     * @param key the key to release a holder of
     * @return unit once the count is decremented (or the entry evicted)
     */
    private def release(key: K): UIO[Unit] =
      ref.update { map =>
        map.get(key) match
          case Some((s, 1)) => map - key            // last user → evict
          case Some((s, n)) => map.updated(key, (s, n - 1))
          case None         => map
      }
