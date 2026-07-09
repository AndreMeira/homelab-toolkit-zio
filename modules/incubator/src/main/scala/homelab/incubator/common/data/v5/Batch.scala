package homelab.incubator.common.data.v5


import homelab.common.error.ApplicationError
import homelab.incubator.common.data.v5.Batch.LineageMismatch


/**
 * A *complete* batch over its universe — every slot resolved to a success `A` or an error `E`. Only
 * [[Batch.make]], [[defaultValue]]/[[defaultError]] and [[overlay]]/[[overlays]] produce a `Batch`, and
 * each preserves the full slot set, so a `Batch` is always complete. Subsets and per-item mappings live in
 * [[Batch.Partial]] and can only be overlaid *onto* a `Batch`, never promoted back to one — which is what
 * keeps an overlay fold complete.
 *
 * Backed by a `Map[Int, Either[E, A]]`; the `Int` index is the per-make positional identity. Overlaying
 * across lineages fails with [[Batch.LineageMismatch]]. Lineage is reference identity, so `make(xs)` differs
 * from `make(xs)` — compare `toList`, not batches.
 */
case class Batch[+E, +A] private (slots: Map[Int, Either[E, A]], lineage: Batch.Lineage):

  /** Transform the success channel, leaving errors untouched. */
  def map[B](fn: A => B): Batch[E, B] = Batch(slots.view.mapValues(_.map(fn)).toMap, lineage)

  /** Transform the error channel, leaving successes untouched. */
  def mapError[E2](fn: E => E2): Batch[E2, A] =
    Batch(slots.view.mapValues(_.left.map(fn)).toMap, lineage)

  /** Filter-and-map the success channel into a [[Batch.Partial]]: matching successes are mapped,
    * non-matching are dropped, errors pass through unchanged. */
  def collect[B](pf: PartialFunction[A, B]): Batch.Partial[E, B] =
    Batch.Partial(
      slots.collect {
        case (index, Right(value)) if pf.isDefinedAt(value) => index -> (Right(pf(value)): Either[E, B])
        case (index, Left(error))                           => index -> (Left(error): Either[E, B])
      },
      lineage,
    )

  /** Split by a predicate on the success values: `left` holds the successes matching `p`, `right` the
    * remainder — non-matching successes and all errors. The two are disjoint and cover every slot, so
    * overlaying both back reconstructs the whole. */
  def partition(p: A => Boolean): (Batch.Partial[E, A], Batch.Partial[E, A]) =
    val (left, right) = slots.partition {
      case (_, Right(value)) => p(value)
      case (_, Left(_))      => false
    }
    (Batch.Partial(left, lineage), Batch.Partial(right, lineage))

  /** The successful slots. */
  def success: Batch.Success[A] = Batch.Success(
    slots.collect { case (index, Right(value)) => index -> value },
    lineage,
  )

  /** The errored slots. */
  def error: Batch.Error[E] =
    Batch.Error(slots.collect { case (index, Left(value)) => index -> value }, lineage)

  /** Split into both channels at once: `(errors, successes)`. */
  def separate: (Batch.Error[E], Batch.Success[A]) = (error, success)

  /** Overlay `other`'s successes onto this batch; `other` wins on shared slots. */
  def overlay[A2 >: A](other: Batch.Success[A2]): Either[LineageMismatch, Batch[E, A2]] =
    if other.lineage != this.lineage then Left(LineageMismatch)
    else Right(merge(other.lift.slots))

  /** Overlay `other`'s errors onto this batch; `other` wins on shared slots. */
  def overlay[E2 >: E](other: Batch.Error[E2]): Either[LineageMismatch, Batch[E2, A]] =
    if other.lineage != this.lineage then Left(LineageMismatch)
    else Right(merge(other.lift.slots))

  /** Overlay `other`'s partial slots onto this batch; `other` wins on shared slots. */
  def overlay[E2 >: E, A2 >: A](other: Batch.Partial[E2, A2]): Either[LineageMismatch, Batch[E2, A2]] =
    if other.lineage != this.lineage then Left(LineageMismatch)
    else Right(merge(other.slots))

  /** Overlay each of `others` in turn (later wins); any lineage mismatch fails the whole fold. */
  def overlays[A2 >: A, E2 >: E](
    others: (Batch.Error[E2] | Batch.Success[A2] | Batch.Partial[E2, A2])*
  ): Either[LineageMismatch, Batch[E2, A2]] =
    others.foldLeft(Right(this): Either[LineageMismatch, Batch[E2, A2]]):
      case (Right(batch), other: Batch.Success[A2])     => batch.overlay(other)
      case (Right(batch), other: Batch.Error[E2])       => batch.overlay(other)
      case (Right(batch), other: Batch.Partial[E2, A2]) => batch.overlay(other)
      case _                                            => Left(LineageMismatch)

  /** Fix a fallback of all-`Right(value)` — a complete base to overlay pieces onto. */
  def defaultValue[B](value: B): Batch[Nothing, B] = Batch(slots.view.mapValues(_ => Right(value)).toMap, lineage)

  /** Fix a fallback of all-`Left(error)` — a complete base to overlay pieces onto. */
  def defaultError[E2](error: E2): Batch[E2, Nothing] = Batch(slots.view.mapValues(_ => Left(error)).toMap, lineage)

  /** Materialise the slots as `Either`s in index order. */
  def toList: List[Either[E, A]] = slots.toList.sortBy((k, _) => k).map((_, v) => v)

  private def merge[E2 >: E, A2 >: A](otherSlots: Map[Int, Either[E2, A2]]): Batch[E2, A2] =
    Batch(slots ++ otherSlots, lineage)


object Batch:
  final class Lineage

  type LineageMismatch = LineageMismatch.type
  case object LineageMismatch extends ApplicationError.ImplementationError:
    override def message: String = "Batch overlay failed: the two batches are from different lineages."

  /** A fresh batch of all-successful slots, indexed by position. */
  def make[A](values: List[A]): Batch[Nothing, A] =
    Batch(values.zipWithIndex.map { case (value, index) => index -> Right(value) }.toMap, new Lineage)

  /**
   * A *partial* set of slots — a projection or per-item mapping over a subset of a [[Batch]]'s universe.
   * It carries no `overlay`/`defaultValue`/`defaultError`, and nothing promotes it back to a [[Batch]]: a
   * `Partial` can only be overlaid *onto* a `Batch`. That's what keeps a `Batch` provably complete.
   */
  case class Partial[+E, +A](slots: Map[Int, Either[E, A]], lineage: Lineage):

    /** Transform the success channel, leaving errors untouched. */
    def map[B](fn: A => B): Partial[E, B] = Partial(slots.view.mapValues(_.map(fn)).toMap, lineage)

    /** Transform the error channel, leaving successes untouched. */
    def mapError[E2](fn: E => E2): Partial[E2, A] =
      Partial(slots.view.mapValues(_.left.map(fn)).toMap, lineage)

    /** Filter-and-map the success channel (see [[Batch.collect]]): matching successes mapped, non-matching
      * dropped, errors passed through. */
    def collect[B](pf: PartialFunction[A, B]): Partial[E, B] =
      Partial(
        slots.collect {
          case (index, Right(value)) if pf.isDefinedAt(value) => index -> (Right(pf(value)): Either[E, B])
          case (index, Left(error))                           => index -> (Left(error): Either[E, B])
        },
        lineage,
      )

    /** Split by a predicate on the success values (see [[Batch.partition]]): matching successes `left`, the
      * remainder — non-matching successes and all errors — `right`. */
    def partition(p: A => Boolean): (Partial[E, A], Partial[E, A]) =
      val (left, right) = slots.partition {
        case (_, Right(value)) => p(value)
        case (_, Left(_))      => false
      }
      (Partial(left, lineage), Partial(right, lineage))

    /** The successful slots. */
    def success: Success[A] = Success(slots.collect { case (index, Right(value)) => index -> value }, lineage)

    /** The errored slots. */
    def error: Error[E] = Error(slots.collect { case (index, Left(value)) => index -> value }, lineage)

    /** Split into both channels at once: `(errors, successes)`. */
    def separate: (Error[E], Success[A]) = (error, success)

    /** Materialise the slots as `Either`s in index order. */
    def toList: List[Either[E, A]] = slots.toList.sortBy((k, _) => k).map((_, v) => v)

  /** The successful slots of a [[Batch]] — one `A` per index, no errors. */
  case class Success[+A](slots: Map[Int, A], lineage: Lineage):

    /** The values in index order. */
    def toList: List[A] = slots.toList.sortBy(_._1).map(_._2)

    /** Transform each value, keeping its slot. */
    def map[B](fn: A => B): Success[B] = Success(slots.view.mapValues(fn).toMap, lineage)

    /** Filter-and-map the values into a [[Partial]]: matching values are mapped, non-matching are dropped. */
    def collect[B](pf: PartialFunction[A, B]): Partial[Nothing, B] =
      Partial(slots.collect { case (index, value) if pf.isDefinedAt(value) => index -> (Right(pf(value)): Either[Nothing, B]) }, lineage)

    /** Index the values by a business key, so external results can be re-associated by that key. */
    def indexBy[K](fn: A => K): Success.Indexed[K, A] =
      Success.Indexed(slots.map { case (index, value) => fn(value) -> (index, value) }, lineage)

    /** Apply a fallible transform per value, flipping failures into the error channel — a [[Partial]]. */
    def mapEither[E, B](fn: A => Either[E, B]): Partial[E, B] = Partial(slots.view.mapValues(fn).toMap, lineage)

    /** Route each value into one of two typed groups by `f` — a *non-failing* split (both sides are
      * successes, unlike [[mapEither]] where `Left` is a failure). Slots and lineage are preserved. */
    def partitionMap[A1, A2](f: A => Either[A1, A2]): (Success[A1], Success[A2]) =
      val (lefts, rights) = slots.toList.partitionMap {
        case (index, value) =>
          f(value) match
            case Left(a1)  => Left(index -> a1)
            case Right(a2) => Right(index -> a2)
      }
      (Success(lefts.toMap, lineage), Success(rights.toMap, lineage))

    /** Lift these successes into a [[Partial]] (all `Right`) — a piece to overlay onto a [[Batch]]. */
    def lift[E]: Partial[E, A] = Partial(slots.map { case (index, value) => index -> Right(value) }, lineage)

  /** The errored slots of a [[Batch]] — one `E` per index, no successes. */
  case class Error[+E](slots: Map[Int, E], lineage: Lineage):

    /** The errors in index order. */
    def toList: List[E] = slots.toList.sortBy(_._1).map(_._2)

    /** Transform each error, keeping its slot. */
    def map[E2](fn: E => E2): Error[E2] = Error(slots.view.mapValues(fn).toMap, lineage)

    /** Lift these errors into a [[Partial]] (all `Left`) — a piece to overlay onto a [[Batch]]. */
    def lift[A]: Partial[E, A] = Partial(slots.map { case (index, value) => index -> Left(value) }, lineage)

  object Success:

    /** [[Success]] values indexed by a business key `K`, each remembering its original slot. */
    case class Indexed[K, +A](byKey: Map[K, (Int, A)], lineage: Lineage):

      /**
       * Left-join `replacements` onto the original slots by key: every original slot is covered — a matched
       * replacement becomes `Right(replacement)`, an original with no matching replacement becomes
       * `Left(orError)`. Replacements with no matching original are ignored. (This is [[associateWith]] keeping
       * only the match.)
       */
      def replaceWith[E, B](replacements: List[B], orError: E)(fn: B => K): Partial[E, B] =
        val byNewKey = replacements.map(replacement => fn(replacement) -> replacement).toMap
        Partial(byKey.map { case (key, (index, _)) => index -> byNewKey.get(key).toRight(orError) }, lineage)

      /**
       * Left-join `associations` onto the original slots by key, *pairing* each original with its match:
       * a matched slot becomes `Right((original, match))`, an original with no match becomes `Left(orError)`.
       * Associations with no matching original are ignored.
       */
      def associateWith[E, B](associations: List[B], orError: E)(fn: B => K): Partial[E, (A, B)] =
        val byNewKey = associations.map(replacement => fn(replacement) -> replacement).toMap
        Partial(byKey.map { case (key, (index, value)) => index -> byNewKey.get(key).map(value -> _).toRight(orError) }, lineage)
