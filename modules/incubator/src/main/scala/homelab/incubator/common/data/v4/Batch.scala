package homelab.incubator.common.data.v4


import homelab.common.error.ApplicationError
import homelab.incubator.common.data.v4.Batch.LineageMismatch


/**
 * An ordered set of indexed slots, each resolved to a success `A` or an error `E`. The `Int` index is a
 * slot's identity — assigned by [[Batch.make]] and preserved through every transform — so the partial
 * successes and failures of a bulk operation stay correlated to their original request positions.
 *
 * Backed by a `Map[Int, Either[E, A]]`, so indices are unique by construction. A `Batch` is *partial*: it
 * carries no overlay operator. To recombine pieces you first fix a complete universe with [[defaultValue]]
 * / [[defaultError]], which yields a [[Batch.Default]] — the only thing you can overlay onto — so an
 * overlay fold always starts from, and stays over, a full universe.
 *
 * Indices are only meaningful within a single [[Batch.make]] lineage; overlaying across lineages fails with
 * [[Batch.LineageMismatch]]. Lineage is reference identity, so `make(xs)` differs from `make(xs)` — compare
 * `toList`, not batches.
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

  /** Fix a complete universe of all-`Right(value)` slots — a [[Batch.Default]] to overlay pieces onto. */
  def defaultValue[B](value: B): Batch.Default[Nothing, B] =
    Batch.Default(slots.view.mapValues(_ => Right(value)).toMap, lineage)

  /** Fix a complete universe of all-`Left(error)` slots — a [[Batch.Default]] to overlay pieces onto. */
  def defaultError[E2](error: E2): Batch.Default[E2, Nothing] =
    Batch.Default(slots.view.mapValues(_ => Left(error)).toMap, lineage)

  /** Materialise the slots as `Either`s in index order. */
  def toList: List[Either[E, A]] = slots.toList.sortBy((k, _) => k).map((_, v) => v)


object Batch:
  final class Lineage

  type LineageMismatch = LineageMismatch.type
  case object LineageMismatch extends ApplicationError.InconsistentState:
    override def message: String = "Batch overlay failed: the two batches are from different lineages."

  /** A fresh batch of all-successful slots, indexed by position. */
  def make[A](values: List[A]): Batch[Nothing, A] =
    Batch(values.zipWithIndex.map { case (value, index) => index -> Right(value) }.toMap, new Lineage)

  /**
   * A *complete* batch over its universe — every slot resolved — obtained from [[Batch.defaultValue]] /
   * [[Batch.defaultError]]. Only a `Default` carries `overlay`/`overlays`, so an overlay fold must start
   * from a full universe; because a same-lineage piece's slots are always a subset of it, the result stays
   * complete over that universe. Pieces from another lineage yield [[LineageMismatch]].
   */
  case class Default[+E, +A](slots: Map[Int, Either[E, A]], lineage: Lineage):

    /** Overlay `other`'s successes onto this default; `other` wins on shared slots. */
    def overlay[A2 >: A](other: Success[A2]): Either[LineageMismatch, Default[E, A2]] =
      if other.lineage != this.lineage then Left(LineageMismatch)
      else Right(merge(other.lift.slots))

    /** Overlay `other`'s errors onto this default; `other` wins on shared slots. */
    def overlay[E2 >: E](other: Error[E2]): Either[LineageMismatch, Default[E2, A]] =
      if other.lineage != this.lineage then Left(LineageMismatch)
      else Right(merge(other.lift.slots))

    /** Overlay `other` onto this default; `other` wins on shared slots. */
    def overlay[E2 >: E, A2 >: A](other: Batch[E2, A2]): Either[LineageMismatch, Default[E2, A2]] =
      if other.lineage != this.lineage then Left(LineageMismatch)
      else Right(merge(other.slots))

    /** Overlay each of `others` in turn (later wins); any lineage mismatch fails the whole fold. */
    def overlays[A2 >: A, E2 >: E](
      others: (Error[E2] | Success[A2] | Batch[E2, A2])*
    ): Either[LineageMismatch, Default[E2, A2]] =
      others.foldLeft(Right(this): Either[LineageMismatch, Default[E2, A2]]):
        case (Right(default), other @ Success[A2](_, this.lineage))   => default.overlay(other)
        case (Right(default), other @ Error[E2](_, this.lineage))     => default.overlay(other)
        case (Right(default), other @ Batch[E2, A2](_, this.lineage)) => default.overlay(other)
        case _                                                        => Left(LineageMismatch)

    /** Materialise the slots as `Either`s in index order. */
    def toList: List[Either[E, A]] = slots.toList.sortBy((k, _) => k).map((_, v) => v)

    private def merge[E2 >: E, A2 >: A](otherSlots: Map[Int, Either[E2, A2]]): Default[E2, A2] =
      Default(slots ++ otherSlots, lineage)

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
