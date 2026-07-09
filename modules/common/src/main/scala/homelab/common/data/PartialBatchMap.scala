package homelab.common.data


import homelab.common.data.BatchMap.Lineage


/**
 * The `Map`-backed [[Batch.Partial]] implementation. Mirrors [[BatchMap]]'s delegation to [[BatchOps]], but
 * every operation stays a `Partial` — a `Partial` never returns to a complete [[Batch]] except by being
 * overlaid onto one via [[Batch.overlay]].
 *
 * @param lineage the lineage of the [[Batch]] this is a subset of
 * @param items   the retained slots, keyed by input position
 */
private[data] case class PartialBatchMap[+E, +A](
  lineage: Lineage,
  items: Map[Int, Either[E, A]],
) extends Batch.Partial[E, A], BatchOps[E, A] {

  override def map[B](fn: A => B): Batch.Partial[E, B] =
    PartialBatchMap(lineage, mapItems(fn))

  override def mapEither[E2 >: E, B](fn: A => Either[E2, B]): Batch.Partial[E2, B] =
    PartialBatchMap(lineage, mapEitherItems(fn))

  override def filter(fn: A => Boolean): Batch.Partial[E, A] =
    PartialBatchMap(lineage, filterItems(fn))

  override def collect[B](fn: PartialFunction[A, B]): Batch.Partial[E, B] =
    PartialBatchMap(lineage, collectItems(fn))

  override def partitionMap[A1, A2](fn: A => Either[A1, A2]): (Batch.Partial[Nothing, A1], Batch.Partial[Nothing, A2]) =
    val (lefts, rights) = partitionMapItems(fn)
    (PartialBatchMap(lineage, lefts), PartialBatchMap(lineage, rights))

  override def defaultError[E2](error: E2): Batch.Partial[E2, Nothing] =
    PartialBatchMap(lineage, fillItemsWithError(error))

  override def defaultValue[B](value: B): Batch.Partial[Nothing, B] =
    PartialBatchMap(lineage, fillItemsWithValue(value))

  override def replaceWith[E2 >: E, K, B](other: Map[K, B])(key: A => K, notFound: A => E2): Batch.Partial[E2, B] =
    PartialBatchMap(lineage, replaceItemsWith(other)(key, notFound))

  override def associateWith[E2 >: E, K, B](other: Map[K, B])(key: A => K, notFound: A => E2): Batch.Partial[E2, (A, B)] =
    PartialBatchMap(lineage, associateItemsWith(other)(key, notFound))
}
