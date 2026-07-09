package homelab.incubator.common.data.v2


/**
 * The identity of one [[Batch.make]] call. Every batch derived from that `make` — through any chain of
 * transforms and any fork/join — shares this `Lineage`. Because indices are positional, they are only
 * comparable within a single lineage, so overlays require a matching one.
 */
final class Lineage


/** Raised by an overlay when the two batches come from different [[Batch.make]] lineages. */
final class LineageMismatch extends RuntimeException("cannot overlay batches from different `make` lineages")


/**
 * An ordered set of indexed slots, each resolved to a success `A` or an error `E`, tagged with the
 * [[Lineage]] of the [[Batch.make]] it descends from.
 *
 * Backed by a `Map[Int, Either[E, A]]`. Only the overlay direction `<+` is provided ("incoming wins"), and
 * it is guarded by a runtime lineage check: combining batches from different `make`s throws
 * [[LineageMismatch]] rather than silently merging by meaningless positions. Since no transform ever
 * invents an index, a same-lineage overlay is always an index-subset of the base — it can never grow the
 * slot universe.
 */
case class Batch[+E, +A] private (lineage: Lineage, slots: Map[Int, Either[E, A]]):

  /** Transform the success channel, leaving errors untouched. */
  def map[B](fn: A => B): Batch[E, B] = Batch(lineage, slots.view.mapValues(_.map(fn)).toMap)

  /** Transform the error channel, leaving successes untouched. */
  def mapError[E2](fn: E => E2): Batch[E2, A] = Batch(lineage, slots.view.mapValues(_.left.map(fn)).toMap)

  /** The successful slots. */
  def success: Batch.Success[A] = Batch.Success(lineage, slots.collect { case (index, Right(value)) => index -> value })

  /** The errored slots. */
  def error: Batch.Error[E] = Batch.Error(lineage, slots.collect { case (index, Left(value)) => index -> value })

  /** Overlay `other`'s successes onto this batch (`other` wins); both must share a lineage. */
  def <+[A2 >: A](other: Batch.Success[A2]): Batch[E, A2] = overlaidWith[E, A2](other.lineage, other.asEither)

  /** Overlay `other`'s errors onto this batch (`other` wins); both must share a lineage. */
  def <+[E2 >: E](other: Batch.Error[E2]): Batch[E2, A] = overlaidWith[E2, A](other.lineage, other.asEither)

  /** Overlay `other` onto this batch (`other` wins); both must share a lineage. */
  def <+[E2 >: E, A2 >: A](other: Batch[E2, A2]): Batch[E2, A2] = overlaidWith[E2, A2](other.lineage, other.slots)

  /** Collapse every slot to `Right(value)` — the uniform success base for an overlay fold. */
  def defaultValue[B](value: B): Batch[E, B] = Batch(lineage, slots.view.mapValues(_ => Right(value)).toMap)

  /** Collapse every slot to `Left(error)` — the uniform error base for an overlay fold. */
  def defaultError[E2](error: E2): Batch[E2, A] = Batch(lineage, slots.view.mapValues(_ => Left(error)).toMap)

  /** Materialise the slots as `Either`s in index order. */
  def toList: List[Either[E, A]] = slots.toList.sortBy(_._1).map(_._2)

  /** Overlay `other` (incoming wins), enforcing a shared lineage. Throws [[LineageMismatch]] otherwise. */
  private def overlaidWith[E2 >: E, A2 >: A](otherLineage: Lineage, other: Map[Int, Either[E2, A2]]): Batch[E2, A2] =
    if lineage ne otherLineage then throw LineageMismatch()
    else Batch(lineage, slots ++ other)


object Batch:

  /** A fresh batch of all-successful slots, indexed by position — and a fresh [[Lineage]]. */
  def make[A](values: List[A]): Batch[Nothing, A] =
    Batch(new Lineage, values.zipWithIndex.map { case (value, index) => index -> Right(value) }.toMap)

  /** The successful slots of a [[Batch]] — one `A` per index, no errors, tagged with its lineage. */
  case class Success[+A](lineage: Lineage, slots: Map[Int, A]):

    /** The values in index order. */
    def toList: List[A] = slots.toList.sortBy(_._1).map(_._2)

    /** Transform each value, keeping its slot. */
    def map[B](fn: A => B): Success[B] = Success(lineage, slots.view.mapValues(fn).toMap)

    /** Index the values by a business key, so external results can be re-associated by that key. */
    def indexBy[K](fn: A => K): Success.Indexed[K, A] =
      Success.Indexed(lineage, slots.map { case (index, value) => fn(value) -> (index, value) })

    /** Apply a fallible transform per value, flipping failures into the error channel of a [[Batch]]. */
    def mapEither[E, B](fn: A => Either[E, B]): Batch[E, B] = Batch(lineage, slots.view.mapValues(fn).toMap)

    private[v2] def asEither: Map[Int, Either[Nothing, A]] = slots.view.mapValues(Right(_)).toMap

  /** The errored slots of a [[Batch]] — one `E` per index, no successes, tagged with its lineage. */
  case class Error[+E](lineage: Lineage, slots: Map[Int, E]):

    /** The errors in index order. */
    def toList: List[E] = slots.toList.sortBy(_._1).map(_._2)

    /** Transform each error, keeping its slot. */
    def map[E2](fn: E => E2): Error[E2] = Error(lineage, slots.view.mapValues(fn).toMap)

    /** Index the errors by a business key. */
    def indexBy[K](fn: E => K): Error.Indexed[K, E] =
      Error.Indexed(lineage, slots.map { case (index, value) => fn(value) -> (index, value) })

    private[v2] def asEither: Map[Int, Either[E, Nothing]] = slots.view.mapValues(Left(_)).toMap

  object Success:

    /** [[Success]] values indexed by a business key `K`, each remembering its slot; carries the lineage. */
    case class Indexed[K, +A](lineage: Lineage, byKey: Map[K, (Int, A)]):

      /**
       * Re-associate `replacements` into the original slots by key: each replacement matched to a key takes
       * that slot's index. Unmatched replacements and unmatched originals are dropped.
       */
      def replaceWith[B](replacements: List[B])(fn: B => K): Success[B] =
        Success(lineage, replacements.flatMap(replacement => byKey.get(fn(replacement)).map(_._1 -> replacement)).toMap)

  object Error:

    /** [[Error]] values indexed by a business key `K`, each remembering its slot; carries the lineage. */
    case class Indexed[K, +E](lineage: Lineage, byKey: Map[K, (Int, E)]):

      /** Re-associate `replacements` into the original slots by key (see [[Success.Indexed.replaceWith]]). */
      def replaceWith[E2](replacements: List[E2])(fn: E2 => K): Error[E2] =
        Error(lineage, replacements.flatMap(replacement => byKey.get(fn(replacement)).map(_._1 -> replacement)).toMap)
