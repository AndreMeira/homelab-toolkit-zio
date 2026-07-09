package homelab.common.data


import homelab.common.data.Batch.LineageMismatch
import homelab.common.data.BatchMap.Lineage


/**
 * The `Map`-backed [[Batch]] implementation. Every operation delegates its per-slot logic to [[BatchOps]]
 * and re-wraps the result — as a `BatchMap` when completeness is preserved, or a [[PartialBatchMap]] when
 * slots may be dropped.
 *
 * @param lineage the positional identity shared by all pieces derived from one [[Batch.make]]
 * @param items   the slots, keyed by input position
 */
private[data] case class BatchMap[+E, +A](
  lineage: Lineage,
  items: Map[Int, Either[E, A]],
) extends Batch[E, A], BatchOps[E, A] {

  override def partial: Batch.Partial[E, A] =
    PartialBatchMap(lineage, items)

  override def map[B](fn: A => B): Batch[E, B] =
    BatchMap(lineage, items = mapItems(fn))

  override def mapEither[E2 >: E, B](fn: A => Either[E2, B]): Batch[E2, B] =
    BatchMap(lineage, mapEitherItems(fn))

  override def filter(fn: A => Boolean): Batch.Partial[E, A] =
    PartialBatchMap(lineage, filterItems(fn))

  override def collect[B](fn: PartialFunction[A, B]): Batch.Partial[E, B] =
    PartialBatchMap(lineage, collectItems(fn))

  override def partitionMap[A1, A2](fn: A => Either[A1, A2]): (Batch.Partial[Nothing, A1], Batch.Partial[Nothing, A2]) =
    val (lefts, rights) = partitionMapItems(fn)
    (PartialBatchMap(lineage, lefts), PartialBatchMap(lineage, rights))

  override def defaultValue[B](value: B): Batch[Nothing, B] =
    BatchMap(lineage, fillItemsWithValue(value))

  override def defaultError[E2](error: E2): Batch[E2, Nothing] =
    BatchMap(lineage, fillItemsWithError(error))

  override def replaceWith[E2 >: E, K, B](other: Map[K, B])(key: A => K, notFound: A => E2): Batch[E2, B] =
    BatchMap(lineage, replaceItemsWith(other)(key, notFound))

  override def associateWith[E2 >: E, K, B](other: Map[K, B])(key: A => K, notFound: A => E2): Batch[E2, (A, B)] =
    BatchMap(lineage, associateItemsWith(other)(key, notFound))

  override def overlay[E2 >: E, A2 >: A](other: Batch.Partial[E2, A2]): Either[LineageMismatch, Batch[E2, A2]] =
    other match {
      case PartialBatchMap(parent, others) if parent == lineage => Right(BatchMap(lineage, items ++ others))
      case _                                                    => Left(LineageMismatch)
    }
}


private[data] object BatchMap:

  /** Per-[[Batch.make]] positional identity — reference equality distinguishes universes. */
  final class Lineage
