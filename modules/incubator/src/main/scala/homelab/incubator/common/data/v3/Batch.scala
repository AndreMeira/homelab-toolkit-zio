package homelab.incubator.common.data.v3


import homelab.common.error.ApplicationError
import homelab.incubator.common.data.v3.Batch.LineageMismatch


/**
 * An ordered set of indexed slots, each resolved to a success `A` or an error `E`. The `Int` index is a
 * slot's identity — assigned by [[Batch.make]] and preserved through every transform — so the partial
 * successes and failures of a bulk operation stay correlated to their original request positions.
 *
 * Backed by a `Map[Int, Either[E, A]]`, so indices are unique by construction and overlays are plain map
 * merges. Indices are only meaningful within a single [[Batch.make]] lineage: only overlay batches that
 * were derived from the same `make`.
 */
case class Batch[+E, +A] private (slots: Map[Int, Either[E, A]], lineage: Batch.Lineage):

  /** Transform the success channel, leaving errors untouched. */
  def map[B](fn: A => B): Batch[E, B] = Batch(slots.view.mapValues(_.map(fn)).toMap, lineage)

  /** Transform the error channel, leaving successes untouched. */
  def mapError[E2](fn: E => E2): Batch[E2, A] =
    Batch(slots.view.mapValues(_.left.map(fn)).toMap, lineage)

  /** The successful slots. */
  def success: Batch.Success[A] = Batch.Success(
    slots.collect { case (index, Right(value)) => index -> value },
    lineage,
  )

  /** The errored slots. */
  def error: Batch.Error[E] =
    Batch.Error(slots.collect { case (index, Left(value)) => index -> value }, lineage)

  /** Overlay `other`'s successes onto this batch; `other` wins on shared slots. */
  def overlay[A2 >: A](other: Batch.Success[A2]): Either[LineageMismatch, Batch[E, A2]] =
    if other.lineage != this.lineage then Left(LineageMismatch)
    else Right(merge(other.lift.slots))

  /** Overlay `other`'s errors onto this batch; `other` wins on shared slots. */
  def overlay[E2 >: E](other: Batch.Error[E2]): Either[LineageMismatch, Batch[E2, A]] =
    if other.lineage != this.lineage then Left(LineageMismatch)
    else Right(merge(other.lift.slots))

  /** Overlay `other` onto this batch; `other` wins on shared slots. */
  def overlay[E2 >: E, A2 >: A](other: Batch[E2, A2]): Either[LineageMismatch, Batch[E2, A2]] =
    if other.lineage != this.lineage then Left(LineageMismatch)
    else Right(merge(other.slots))

  /** Collapse every slot to `Right(value)` — the uniform success base for an overlay fold. */
  def defaultValue[B](value: B): Batch[Nothing, B] = Batch(slots.view.mapValues(_ => Right(value)).toMap, lineage)

  /** Collapse every slot to `Left(error)` — the uniform error base for an overlay fold. */
  def defaultError[E2](error: E2): Batch[E2, Nothing] = Batch(slots.view.mapValues(_ => Left(error)).toMap, lineage)

  /** Materialise the slots as `Either`s in index order. */
  def toList: List[Either[E, A]] = slots.toList.sortBy((k, _) => k).map((_, v) => v)

  def overlays[A2 >: A, E2 >: E](
    others: (Batch.Error[E2] | Batch.Success[A2] | Batch[E2, A2])*
  ): Either[LineageMismatch, Batch[E2, A2]] =
    others.foldLeft(Right(this): Either[LineageMismatch, Batch[E2, A2]]):
      case (Right(batch), other @ Batch.Success[A2](_, this.lineage)) => batch.overlay(other)
      case (Right(batch), other @ Batch.Error[E2](_, this.lineage))   => batch.overlay(other)
      case (Right(batch), other @ Batch[E2, A2](_, this.lineage))     => batch.overlay(other)
      case _                                                          => Left(LineageMismatch)

  private def merge[E2 >: E, A2 >: A](otherSlots: Map[Int, Either[E2, A2]]): Batch[E2, A2] =
    Batch(slots ++ otherSlots, lineage)


object Batch:
  final class Lineage

  type LineageMismatch = LineageMismatch.type
  case object LineageMismatch extends ApplicationError.InconsistentState:
    override def message: String = "Batch overlay failed: the two batches are from different lineages."

  /** A fresh batch of all-successful slots, indexed by position. */
  def make[A](values: List[A]): Batch[Nothing, A] =
    Batch(values.zipWithIndex.map { case (value, index) => index -> Right(value) }.toMap, new Lineage)

  /** The successful slots of a [[Batch]] — one `A` per index, no errors. */
  case class Success[+A](slots: Map[Int, A], lineage: Lineage):

    /** The values in index order. */
    def toList: List[A] = slots.toList.sortBy(_._1).map(_._2)

    /** Transform each value, keeping its slot. */
    def map[B](fn: A => B): Success[B] = Success(slots.view.mapValues(fn).toMap, lineage)

    /** Index the values by a business key, so external results can be re-associated by that key. */
    def indexBy[K](fn: A => K): Success.Indexed[K, A] =
      Success.Indexed(slots.map { case (index, value) => fn(value) -> (index, value) }, lineage)

    /** Apply a fallible transform per value, flipping failures into the error channel of a [[Batch]]. */
    def mapEither[E, B](fn: A => Either[E, B]): Batch[E, B] = Batch(slots.view.mapValues(fn).toMap, lineage)

    def lift[E]: Batch[E, A] = Batch(slots.map { case (index, value) => index -> Right(value) }, lineage)

  /** The errored slots of a [[Batch]] — one `E` per index, no successes. */
  case class Error[+E](slots: Map[Int, E], lineage: Lineage):

    /** The errors in index order. */
    def toList: List[E] = slots.toList.sortBy(_._1).map(_._2)

    /** Transform each error, keeping its slot. */
    def map[E2](fn: E => E2): Error[E2] = Error(slots.view.mapValues(fn).toMap, lineage)

    /** Index the errors by a business key. */
    def indexBy[K](fn: E => K): Error.Indexed[K, E] =
      Error.Indexed(slots.map { case (index, value) => fn(value) -> (index, value) }, lineage)

    def lift[A]: Batch[E, A] = Batch(
      slots.map { case (index, value) => index -> Left(value) },
      lineage,
    )

  object Success:

    /** [[Success]] values indexed by a business key `K`, each remembering its original slot. */
    case class Indexed[K, +A](byKey: Map[K, (Int, A)], lineage: Lineage):

      /**
       * Re-associate `replacements` into the original slots by key: each replacement matched to a key takes
       * that slot's index. Unmatched replacements and unmatched originals are dropped.
       */
      def replaceWith[B](replacements: List[B])(fn: B => K): Success[B] =
        Success(
          replacements.flatMap(replacement => byKey.get(fn(replacement)).map(_._1 -> replacement)).toMap,
          lineage,
        )

  object Error:

    /** [[Error]] values indexed by a business key `K`, each remembering its original slot. */
    case class Indexed[K, +E](byKey: Map[K, (Int, E)], lineage: Lineage):

      /** Re-associate `replacements` into the original slots by key (see [[Success.Indexed.replaceWith]]). */
      def replaceWith[E2](replacements: List[E2])(fn: E2 => K): Error[E2] =
        Error(
          replacements.flatMap(replacement => byKey.get(fn(replacement)).map(_._1 -> replacement)).toMap,
          lineage,
        )
