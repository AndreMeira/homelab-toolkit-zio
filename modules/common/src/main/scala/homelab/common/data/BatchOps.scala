package homelab.common.data


/**
 * Shared `Map[Int, Either[E, A]]` machinery behind [[BatchMap]] and [[PartialBatchMap]]. The two impls
 * differ only in how they wrap a result (complete [[Batch]] vs [[Batch.Partial]]), so the per-slot logic
 * lives here once and each caller re-wraps the returned map. Every operation keys off the positional `Int`
 * index, and materialisation is index-ordered regardless of `Map` iteration order.
 *
 * @tparam E the error type of a failed slot
 * @tparam A the value type of a successful slot
 */
private[data] trait BatchOps[+E, +A] {

  /** The backing slots, keyed by input position. */
  def items: Map[Int, Either[E, A]]
  
  def zipItems[E2 >: E, B](other: Map[Int, Either[E2, B]]): Map[Int, Either[E2, (A, B)]] =
    items.flatMap:
      case (index, Left(err))    => Some(index -> Left(err))
      case (index, Right(value)) => other.get(index).map(otherEither => index -> otherEither.map(b => (value, b)))

  /**
   * The slots as `Either`s in index order — the single source of ordering for the materialisers below.
   *
   * @return every slot sorted by its `Int` index, the `Map` iteration order discarded
   */
  private def ordered: List[Either[E, A]] = items.toList.sortBy((index, _) => index).map((_, either) => either)

  /**
   * The successful values, in index order.
   *
   * @return the value of every `Right` slot, ordered by index
   */
  def values: List[A] = ordered.collect { case Right(value) => value }

  /**
   * The errors, in index order.
   *
   * @return the error of every `Left` slot, ordered by index
   */
  def errors: List[E] = ordered.collect { case Left(error) => error }

  /**
   * Every slot, in index order.
   *
   * @return each slot as `Right(value)` or `Left(error)`, ordered by index
   */
  def toList: List[Either[E, A]] = ordered

  /**
   * Map the value channel, keeping errors and positions.
   *
   * @param fn the mapping applied to each successful value
   * @tparam B the mapped value type
   * @return the slots with each `Right` mapped by `fn`, `Left`s untouched
   */
  def mapItems[B](fn: A => B): Map[Int, Either[E, B]] =
    items.map { case (index, either) => index -> either.map(fn) }

  /**
   * Map each value into an `Either`, folding `Left`s into the error channel.
   *
   * @param fn the fallible mapping applied to each successful value
   * @tparam E2 the widened error type, admitting `fn`'s failures
   * @tparam B  the mapped value type
   * @return the slots with each `Right` replaced by `fn`'s result, existing `Left`s untouched
   */
  def mapEitherItems[E2 >: E, B](fn: A => Either[E2, B]): Map[Int, Either[E2, B]] =
    items.map { case (index, either) => index -> either.flatMap(fn) }

  /**
   * Keep the value slots satisfying `fn`; errors always pass.
   *
   * @param fn the predicate applied to each successful value
   * @return the slots retaining matching `Right`s and all `Left`s
   */
  def filterItems(fn: A => Boolean): Map[Int, Either[E, A]] =
    items.filter { case (index, either) => either.fold(_ => true, value => fn(value)) }

  /**
   * Set every slot to `Left(error)` — a constant, value-free error base.
   *
   * @param error the error placed in every slot
   * @tparam E2 the error type of the base
   * @return the slots, each an `Left(error)`, positions preserved
   */
  def fillItemsWithError[E2](error: E2): Map[Int, Either[E2, Nothing]] =
    items.map { case index -> _ => index -> Left(error) }

  /**
   * Set every slot to `Right(value)` — a constant, error-free success base.
   *
   * @param value the value placed in every slot
   * @tparam B the value type of the base
   * @return the slots, each a `Right(value)`, positions preserved
   */
  def fillItemsWithValue[B](value: B): Map[Int, Either[Nothing, B]] =
    items.map { case index -> _ => index -> Right(value) }

  /**
   * Map the values `fn` is defined on and drop the rest; errors pass through.
   *
   * @param fn the partial mapping applied to successful values
   * @tparam B the mapped value type
   * @return the slots with defined `Right`s mapped, undefined `Right`s dropped, and all `Left`s kept
   */
  def collectItems[B](fn: PartialFunction[A, B]): Map[Int, Either[E, B]] = items.collect:
    case (index, Right(value)) if fn.isDefinedAt(value) => index -> Right(fn(value))
    case (index, Left(error))                           => index -> Left(error)

  /**
   * Route value slots into two disjoint index-keyed maps by `fn`; error slots go to neither half.
   *
   * @param fn routes each value into the left (`Left`) or right (`Right`) map
   * @tparam A1 the value type of the left half
   * @tparam A2 the value type of the right half
   * @return `(lefts, rights)` — two error-free slot maps sharing the source indices; error slots in neither
   */
  def partitionMapItems[A1, A2](fn: A => Either[A1, A2]): (Map[Int, Either[Nothing, A1]], Map[Int, Either[Nothing, A2]]) =
    val routed = items.collect { case (index, Right(value)) => index -> fn(value) }
    val lefts  = routed.collect { case (index, Left(a1)) => index -> Right(a1) }
    val rights = routed.collect { case (index, Right(a2)) => index -> Right(a2) }
    (lefts, rights)

  /**
   * Replace each value with its `key` match in `other`; errors pass through.
   *
   * @param other    the lookup table, already keyed
   * @param key      the join key extracted from a value
   * @param notFound the error for a value absent from `other`, evaluated only on a miss
   * @tparam E2 the widened error type, admitting `notFound`'s failures
   * @tparam K  the join key
   * @tparam B  the replacement value
   * @return the slots with each `Right` replaced by its match, misses turned into `notFound` errors, `Left`s
   *         untouched
   */
  def replaceItemsWith[E2 >: E, K, B](other: Map[K, B])(key: A => K, notFound: A => E2): Map[Int, Either[E2, B]] =
    items.map:
      case (index, Left(err))    => index -> Left(err)
      case (index, Right(value)) => index -> other.get(key(value)).toRight(notFound(value))

  /**
   * As [[replaceItemsWith]], pairing each value with its match.
   *
   * @param other    the lookup table, already keyed
   * @param key      the join key extracted from a value
   * @param notFound the error for a value absent from `other`, evaluated only on a miss
   * @tparam E2 the widened error type, admitting `notFound`'s failures
   * @tparam K  the join key
   * @tparam B  the matched value
   * @return the slots with each `Right` paired with its match, misses turned into `notFound` errors, `Left`s
   *         untouched
   */
  def associateItemsWith[E2 >: E, K, B](other: Map[K, B])(key: A => K, notFound: A => E2): Map[Int, Either[E2, (A, B)]] =
    items.map:
      case (index, Left(err))    => index -> Left(err)
      case (index, Right(value)) => index -> other.get(key(value)).map(b => (value, b)).toRight(notFound(value))
}
