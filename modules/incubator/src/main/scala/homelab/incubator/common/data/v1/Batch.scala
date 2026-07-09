package homelab.incubator.common.data.v1


/**
 * An ordered set of indexed slots, each resolved to a success `A` or an error `E`. The `Int` index is a
 * slot's identity — assigned by [[Batch.make]] and preserved through every transform — so the partial
 * successes and failures of a bulk operation stay correlated to their original request positions.
 *
 * Backed by a `Map[Int, Either[E, A]]`, so indices are unique by construction and overlays are plain map
 * merges. Indices are only meaningful within a single [[Batch.make]] lineage: only overlay batches that
 * were derived from the same `make`.
 */
case class Batch[+E, +A] private (slots: Map[Int, Either[E, A]]):

  /** Transform the success channel, leaving errors untouched. */
  def map[B](fn: A => B): Batch[E, B] = Batch(slots.view.mapValues(_.map(fn)).toMap)

  /** Transform the error channel, leaving successes untouched. */
  def mapError[E2](fn: E => E2): Batch[E2, A] = Batch(slots.view.mapValues(_.left.map(fn)).toMap)

  /** The successful slots. */
  def success: Batch.Success[A] = Batch.Success(slots.collect { case (index, Right(value)) => index -> value })

  /** The errored slots. */
  def error: Batch.Error[E] = Batch.Error(slots.collect { case (index, Left(value)) => index -> value })

  /** Overlay `other`'s successes onto this batch; `other` wins on shared slots. */
  def <+[A2 >: A](other: Batch.Success[A2]): Batch[E, A2] = overlaidWith[E, A2](other.asEither)

  /** Overlay this batch onto `other`'s successes; this batch wins on shared slots. */
  def +>[A2 >: A](other: Batch.Success[A2]): Batch[E, A2] = laidUnder[E, A2](other.asEither)

  /** Overlay `other`'s errors onto this batch; `other` wins on shared slots. */
  def <+[E2 >: E](other: Batch.Error[E2]): Batch[E2, A] = overlaidWith[E2, A](other.asEither)

  /** Overlay this batch onto `other`'s errors; this batch wins on shared slots. */
  def +>[E2 >: E](other: Batch.Error[E2]): Batch[E2, A] = laidUnder[E2, A](other.asEither)

  /** Overlay `other` onto this batch; `other` wins on shared slots. */
  def <+[E2 >: E, A2 >: A](other: Batch[E2, A2]): Batch[E2, A2] = overlaidWith[E2, A2](other.slots)

  /** Overlay this batch onto `other`; this batch wins on shared slots. */
  def +>[E2 >: E, A2 >: A](other: Batch[E2, A2]): Batch[E2, A2] = laidUnder[E2, A2](other.slots)

  /** Collapse every slot to `Right(value)` — the uniform success base for an overlay fold. */
  def defaultValue[B](value: B): Batch[E, B] = Batch(slots.view.mapValues(_ => Right(value)).toMap)

  /** Collapse every slot to `Left(error)` — the uniform error base for an overlay fold. */
  def defaultError[E2](error: E2): Batch[E2, A] = Batch(slots.view.mapValues(_ => Left(error)).toMap)

  /** Materialise the slots as `Either`s in index order. */
  def toList: List[Either[E, A]] = slots.toList.sortBy(_._1).map(_._2)

  private def overlaidWith[E2 >: E, A2 >: A](other: Map[Int, Either[E2, A2]]): Batch[E2, A2] =
    Batch(slots ++ other)

  private def laidUnder[E2 >: E, A2 >: A](other: Map[Int, Either[E2, A2]]): Batch[E2, A2] =
    Batch(other ++ slots)


object Batch:

  /** A fresh batch of all-successful slots, indexed by position. */
  def make[A](values: List[A]): Batch[Nothing, A] =
    Batch(values.zipWithIndex.map { case (value, index) => index -> Right(value) }.toMap)

  /** The successful slots of a [[Batch]] — one `A` per index, no errors. */
  case class Success[+A](slots: Map[Int, A]):

    /** The values in index order. */
    def toList: List[A] = slots.toList.sortBy(_._1).map(_._2)

    /** Transform each value, keeping its slot. */
    def map[B](fn: A => B): Success[B] = Success(slots.view.mapValues(fn).toMap)

    /** Index the values by a business key, so external results can be re-associated by that key. */
    def indexBy[K](fn: A => K): Success.Indexed[K, A] =
      Success.Indexed(slots.map { case (index, value) => fn(value) -> (index, value) })

    /** Apply a fallible transform per value, flipping failures into the error channel of a [[Batch]]. */
    def mapEither[E, B](fn: A => Either[E, B]): Batch[E, B] = Batch(slots.view.mapValues(fn).toMap)

    private[v1] def asEither: Map[Int, Either[Nothing, A]] = slots.view.mapValues(Right(_)).toMap

  /** The errored slots of a [[Batch]] — one `E` per index, no successes. */
  case class Error[+E](slots: Map[Int, E]):

    /** The errors in index order. */
    def toList: List[E] = slots.toList.sortBy(_._1).map(_._2)

    /** Transform each error, keeping its slot. */
    def map[E2](fn: E => E2): Error[E2] = Error(slots.view.mapValues(fn).toMap)

    /** Index the errors by a business key. */
    def indexBy[K](fn: E => K): Error.Indexed[K, E] =
      Error.Indexed(slots.map { case (index, value) => fn(value) -> (index, value) })

    private[v1] def asEither: Map[Int, Either[E, Nothing]] = slots.view.mapValues(Left(_)).toMap

  object Success:

    /** [[Success]] values indexed by a business key `K`, each remembering its original slot. */
    case class Indexed[K, +A](byKey: Map[K, (Int, A)]):

      /**
       * Re-associate `replacements` into the original slots by key: each replacement matched to a key takes
       * that slot's index. Unmatched replacements and unmatched originals are dropped.
       */
      def replaceWith[B](replacements: List[B])(fn: B => K): Success[B] =
        Success(replacements.flatMap(replacement => byKey.get(fn(replacement)).map(_._1 -> replacement)).toMap)

  object Error:

    /** [[Error]] values indexed by a business key `K`, each remembering its original slot. */
    case class Indexed[K, +E](byKey: Map[K, (Int, E)]):

      /** Re-associate `replacements` into the original slots by key (see [[Success.Indexed.replaceWith]]). */
      def replaceWith[E2](replacements: List[E2])(fn: E2 => K): Error[E2] =
        Error(replacements.flatMap(replacement => byKey.get(fn(replacement)).map(_._1 -> replacement)).toMap)
