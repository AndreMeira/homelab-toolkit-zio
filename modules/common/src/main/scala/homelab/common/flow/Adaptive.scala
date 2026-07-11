package homelab.common.flow


import zio.*


/**
 * A load-adaptive batcher. It tracks concurrent in-flight calls: below `threshold` it runs each request via
 * the inner batcher's unbatched `direct` path (single-call latency, no coordination); at or above `threshold`
 * it routes to `inner.run`, so the burst coalesces. Built via [[Batcher.adaptive]].
 *
 * The low-load path uses `direct` — no coalescing, and (over a deduplicating inner) no dedup. It optimizes
 * latency, not batching, below the threshold.
 *
 * @param threshold the in-flight concurrency at which batching switches on
 * @param inFlight  the live-call counter
 * @param inner     the wrapped batcher
 */
private[flow] final class Adaptive[R, E, In, Out](
  threshold: Int,
  inFlight: Ref[Int],
  inner: Batcher[R, E, In, Out],
) extends Batcher[R, E, In, Out] {

  /**
   * Route on live concurrency: below `threshold`, `inner.direct` (no coalescing); at or above it, `inner.run`.
   * `acquireReleaseWith` runs the decrement on every exit (incl. interrupt) so a leaked count can't pin the
   * batcher in "hot" mode. The count is a heuristic — the read-decide-act is racy, which only ever means an
   * occasionally-suboptimal route, never a wrong result.
   *
   * @return the inner's result; aborts as the inner does
   */
  override def run(in: In): ZIO[R, E, Out] =
    ZIO.acquireReleaseWith(inFlight.updateAndGet(_ + 1))(_ => inFlight.update(_ - 1)): live =>
      if live <= threshold then inner.direct(in) else inner.run(in)

  /**
   * Delegate the one-shot path straight to the inner (bypassing this wrapper's threshold).
   *
   * @return the inner's one-shot result; aborts as the inner does
   */
  override private[flow] def direct(in: In): ZIO[R, E, Out] = inner.direct(in)
}
