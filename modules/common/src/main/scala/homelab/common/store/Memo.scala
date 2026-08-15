package homelab.common.store

import homelab.common.error.ApplicationError.AdapterError
import homelab.common.flow.KeyLock
import zio.*


/**
 * Port: memoised computation over keys `In` — ask for a `Result` and get either what was computed before or,
 * on a miss, the value `compute` produces, retained for next time. Where a [[KeyValueStore]] exposes the
 * slots (get / set / delete), a `Memo` exposes only the *derivation*: a caller can obtain a value but cannot
 * write, evict, or observe whether it was cached. Retention is the implementation's business — an adapter
 * may keep everything, expire, or bound the set — so nothing here promises a hit.
 *
 * The contract that matters is [[computed]]'s exactly-once guarantee; the free
 * [[KeyValueStore.computed]] is the same shape *without* it.
 *
 * @tparam In the key a result is computed for
 * @tparam Result the value computed and retained
 */
trait Memo[In, Result]:

  /**
   * Return the result for `in`, running `compute` only if there isn't one already. Concurrent callers asking
   * for the same `in` share a single computation — one runs it, the rest await its value — so `compute` may
   * be expensive or externally rate-limited without the caller coordinating. Distinct keys never block each
   * other.
   *
   * @param in the key to resolve
   * @param compute derives the result for a key that has none yet
   * @tparam R the environment `compute` needs
   * @tparam E the error `compute` may fail with
   * @return the retained or freshly computed result; fails with `E` if the computation fails, or
   *         `AdapterError` if the underlying storage does. A failed computation retains nothing, so the next
   *         caller retries it.
   */
  def computed[R, E](in: In)(compute: In => ZIO[R, E, Result]): ZIO[R, E | AdapterError, Result]


/** Companion of the [[Memo]] port, carrying the constructor that builds one over any [[KeyValueStore]]. */
object Memo:

  /**
   * A [[Memo]] retaining its results in `store`, serialised by a [[KeyLock]] it provisions itself.
   *
   * Reads take the fast path first — an already-computed key returns without touching the lock at all — and
   * only a miss acquires the permit, re-checks (a concurrent caller may have filled the slot while this one
   * waited), and computes. That second check is what turns [[KeyValueStore.computed]]'s last-write-wins into
   * exactly-once.
   *
   * @param store the slots results are retained in
   * @tparam In the key a result is computed for
   * @tparam Result the value computed and retained
   * @return the memo, with a fresh lock of its own
   */
  def make[In, Result](store: KeyValueStore[In, Result]): UIO[Memo[In, Result]] =
    KeyLock.make[In].map { lock =>
      new Memo[In, Result]:
        def computed[R, E](in: In)(compute: In => ZIO[R, E, Result]): ZIO[R, E | AdapterError, Result] =
          store.get(in).flatMap {
            case Some(result) => ZIO.succeed(result)
            case None         => lock.withPermit(in)(store.computed(in)(compute))
          }
    }
