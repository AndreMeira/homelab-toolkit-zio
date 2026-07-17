package homelab.common.flow.batching

import homelab.common.flow.Batcher
import zio.*


/**
 * Spreads load across `shards.size` independent batchers, so up to that many bulk calls run in parallel (each
 * shard is single-flight). `shardOf` picks the shard for an input: hash of the input for a plain serial inner,
 * hash of the *key* for a deduplicating inner — so a key always lands on one shard and dedup is preserved
 * (parallel across keys, coalesced per key). Built via [[Batcher.distributed]].
 *
 * @param shards  the independent per-shard batchers
 * @param shardOf picks a shard for an input (floor-modded into range)
 */
private[flow] final class Distributed[E, In, Out](
  shards: Vector[Batcher[E, In, Out]],
  shardOf: In => Int,
) extends Batcher[E, In, Out] {

  /**
   * The shard owning `in`.
   *
   * @return the batcher for `in`'s shard
   */
  private def shard(in: In): Batcher[E, In, Out] = 
    shards(Math.floorMod(shardOf(in), shards.size))

  /**
   * Route the request to its shard's [[Batcher.run]].
   *
   * @return the shard's result; aborts as the shard does
   */
  override def run(in: In): IO[E, Out] = shard(in).run(in)

  /**
   * Route the request to its shard's `direct`.
   *
   * @return the shard's one-shot result; aborts as the shard does
   */
  override private[flow] def direct(in: In): IO[E, Out] = shard(in).direct(in)
}
