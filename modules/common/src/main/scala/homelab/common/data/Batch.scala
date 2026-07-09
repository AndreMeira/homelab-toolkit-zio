package homelab.common.data


import homelab.common.error.ApplicationError
import homelab.common.data.Batch.LineageMismatch


/**
 * An order-preserving result of a bulk operation: every element of the original input resolves to a
 * success `A` or an error `E`, keyed by its input position, so partial failure never loses an element or
 * reorders the rest.
 *
 * A `Batch` is *complete* — it always covers its whole input universe. Only [[Batch.make]], the
 * value-preserving transforms ([[map]], [[mapEither]], [[replaceWith]], [[associateWith]]), the constant
 * bases ([[defaultValue]], [[defaultError]]) and [[overlay]] produce one, and each keeps the full slot set.
 * Operations that may drop elements ([[filter]], [[collect]], [[partitionMap]]) return a [[Batch.Partial]]
 * — a subset that can only be overlaid *onto* a `Batch`, never promoted to one. That asymmetry is what makes
 * completeness a type-level guarantee rather than a convention.
 *
 * Slots carry a per-[[make]] positional identity (their *lineage*). [[overlay]] merges only same-lineage
 * pieces; mixing lineages is a programming error, reported as [[LineageMismatch]]. Lineage is reference
 * identity, so `make(xs)` and `make(xs)` are distinct universes — compare [[toList]], not batches.
 *
 * @tparam E the error type of a failed slot
 * @tparam A the value type of a successful slot
 */
trait Batch[+E, +A] {

  /**
   * The successful values, in input order.
   *
   * @return the value of every successful slot, ordered by input position
   */
  def values: List[A]

  /**
   * The errors, in input order.
   *
   * @return the error of every failed slot, ordered by input position
   */
  def errors: List[E]

  /**
   * Every slot as an `Either`, one entry per input element.
   *
   * @return each slot as `Right(value)` or `Left(error)`, ordered by input position
   */
  def toList: List[Either[E, A]]

  /**
   * View this batch as a same-lineage [[Batch.Partial]], forgetting completeness.
   *
   * @return the partial over exactly these slots, sharing this batch's lineage — e.g. to [[overlay]] this
   *         batch onto another
   */
  def partial: Batch.Partial[E, A]

  /**
   * Transform the value channel, leaving errors and positions untouched.
   *
   * @param fn the mapping applied to each successful value
   * @tparam B the mapped value type
   * @return a batch with each success mapped by `fn`, errors and positions unchanged
   */
  def map[B](fn: A => B): Batch[E, B]

  /**
   * Transform each value into an `Either`, routing `Left`s into the error channel.
   *
   * @param fn the fallible mapping applied to each successful value
   * @tparam E2 the widened error type, admitting `fn`'s failures
   * @tparam B  the mapped value type
   * @return a batch where each success became `fn`'s result — a new value or a new error — positions unchanged
   */
  def mapEither[E2 >: E, B](fn: A => Either[E2, B]): Batch[E2, B]

  /**
   * Keep the value slots satisfying `fn`, dropping the rest; errors always pass.
   *
   * @param fn the predicate applied to each successful value
   * @return a partial retaining the matching successes and all errors, dropping non-matching successes
   */
  def filter(fn: A => Boolean): Batch.Partial[E, A]

  /**
   * Map the values `fn` is defined on and drop the rest; errors pass through.
   *
   * @param fn the partial mapping applied to successful values
   * @tparam B the mapped value type
   * @return a partial with defined successes mapped, undefined successes dropped, and all errors kept
   */
  def collect[B](fn: PartialFunction[A, B]): Batch.Partial[E, B]

  /**
   * Split the value channel into two typed, disjoint halves by `fn`: `Left`s land in the first, `Right`s in
   * the second. Error slots go to neither — they remain in this batch, the base you overlay the processed
   * halves back onto. The halves are therefore error-free (`Partial[Nothing, _]`): each is a set of pure
   * successes to process, and carrying the batch's errors along would wrongly invite mapping over slots that
   * have already failed and belong to no branch. Useful to route a heterogeneous batch (e.g.
   * `Create | Update`) into its branches for separate processing, then recombine via
   * [[defaultError]]/[[defaultValue]] + [[overlays]].
   *
   * @param fn routes each value into the left (`Left`) or right (`Right`) half
   * @tparam A1 the value type of the left half
   * @tparam A2 the value type of the right half
   * @return `(lefts, rights)` — two error-free partials of this lineage; error slots appear in neither
   */
  def partitionMap[A1, A2](fn: A => Either[A1, A2]): (Batch.Partial[Nothing, A1], Batch.Partial[Nothing, A2])

  /**
   * A same-lineage base with every slot set to `value` — a complete, error-free success base to [[overlay]]
   * pieces onto.
   *
   * @param value the value placed in every slot
   * @tparam B the value type of the base
   * @return a complete batch of this lineage with every position `Right(value)`
   */
  def defaultValue[B](value: B): Batch[Nothing, B]

  /**
   * A same-lineage base with every slot set to `error` — a complete, value-free error base to [[overlay]]
   * pieces onto; slots left uncovered by the overlays keep `error`.
   *
   * @param error the error placed in every slot
   * @tparam E2 the error type of the base
   * @return a complete batch of this lineage with every position `Left(error)`
   */
  def defaultError[E2](error: E2): Batch[E2, Nothing]

  /**
   * Replace each value with its match in `other`, looked up by `key`; errors pass through, positions are
   * preserved. This is the keyed join for threading an external lookup table (e.g. rows fetched in one
   * query) back into the batch.
   *
   * @param other    the lookup table, already keyed
   * @param key      the join key extracted from a value
   * @param notFound the error for a value absent from `other`, evaluated only on a miss
   * @tparam E2 the widened error type, admitting `notFound`'s failures
   * @tparam K  the join key
   * @tparam B  the replacement value
   * @return a batch with each value replaced by its match, misses turned into `notFound` errors, positions
   *         preserved
   */
  def replaceWith[E2 >: E, K, B](other: Map[K, B])(key: A => K, notFound: A => E2): Batch[E2, B]

  /**
   * As [[replaceWith]], but pairs each value with its match as `(value, match)` instead of discarding it.
   *
   * @param other    the lookup table, already keyed
   * @param key      the join key extracted from a value
   * @param notFound the error for a value absent from `other`, evaluated only on a miss
   * @tparam E2 the widened error type, admitting `notFound`'s failures
   * @tparam K  the join key
   * @tparam B  the matched value
   * @return a batch with each value paired with its match, misses turned into `notFound` errors, positions
   *         preserved
   */
  def associateWith[E2 >: E, K, B](other: Map[K, B])(key: A => K, notFound: A => E2): Batch[E2, (A, B)]

  /**
   * Overlay `other`'s slots onto this batch, `other` winning on shared slots. Completeness is preserved
   * (`other` is a subset of the same universe).
   *
   * @param other the same-lineage subset to overlay
   * @tparam E2 the widened error type
   * @tparam A2 the widened value type
   * @return the merged batch; aborts with [[LineageMismatch]] (as a `Left`) if `other` is from a different
   *         lineage
   */
  def overlay[E2 >: E, A2 >: A](other: Batch.Partial[E2, A2]): Either[LineageMismatch, Batch[E2, A2]]

  /**
   * Overlay each of `others` left-to-right (later wins), short-circuiting on the first lineage mismatch.
   *
   * @param others the same-lineage subsets to overlay, in order
   * @tparam E2 the widened error type
   * @tparam A2 the widened value type
   * @return the merged batch; aborts with [[LineageMismatch]] (as a `Left`) if any piece is from a different
   *         lineage
   */
  def overlays[E2 >: E, A2 >: A](others: Batch.Partial[E2, A2]*): Either[LineageMismatch, Batch[E2, A2]] =
    others.foldLeft(Right(this): Either[LineageMismatch, Batch[E2, A2]]):
      case Left(err) -> _        => Left(err)
      case Right(batch) -> other => batch.overlay(other)
}


object Batch {

  /**
   * A fresh, all-successful batch indexed by input position, under a new lineage.
   *
   * @param items the input elements, each becoming a successful slot at its position
   * @tparam A the value type
   * @return a complete batch of `items` carrying a brand-new lineage
   */
  def make[A](items: List[A]): Batch[Nothing, A] = BatchMap(
    new BatchMap.Lineage,
    items.zipWithIndex.map((value, index) => index -> Right(value)).toMap,
  )

  type LineageMismatch = LineageMismatch.type

  /** Overlaying pieces from different [[make]] universes — a defect in the calling code, not a domain
    * failure, hence an [[ApplicationError.ImplementationError]]. */
  object LineageMismatch extends ApplicationError.ImplementationError:
    override def message: String = "Lineage mismatch between batches"

  /**
   * A *subset* of a [[Batch]]'s slots — the result of an operation that may drop elements ([[Batch.filter]],
   * [[Batch.collect]], [[Batch.partitionMap]]), or a per-element mapping thereof. It carries no [[overlay]]
   * and nothing promotes it back to a [[Batch]]; a `Partial` can only be overlaid *onto* a `Batch` of its
   * lineage. That is what keeps a `Batch` provably complete.
   *
   * @tparam E the error type of a failed slot
   * @tparam A the value type of a successful slot
   */
  trait Partial[+E, +A] {

    /**
     * The successful values, in input order.
     *
     * @return the value of every successful retained slot, ordered by input position
     */
    def values: List[A]

    /**
     * The errors, in input order.
     *
     * @return the error of every failed retained slot, ordered by input position
     */
    def errors: List[E]

    /**
     * Every retained slot as an `Either`, in input order.
     *
     * @return each retained slot as `Right(value)` or `Left(error)`, ordered by input position
     */
    def toList: List[Either[E, A]]

    /**
     * Transform the value channel, leaving errors and positions untouched.
     *
     * @param fn the mapping applied to each successful value
     * @tparam B the mapped value type
     * @return a partial with each success mapped by `fn`, errors and positions unchanged
     */
    def map[B](fn: A => B): Batch.Partial[E, B]

    /**
     * Transform each value into an `Either`, routing `Left`s into the error channel.
     *
     * @param fn the fallible mapping applied to each successful value
     * @tparam E2 the widened error type, admitting `fn`'s failures
     * @tparam B  the mapped value type
     * @return a partial where each success became `fn`'s result — a new value or a new error
     */
    def mapEither[E2 >: E, B](fn: A => Either[E2, B]): Batch.Partial[E2, B]

    /**
     * Keep the value slots satisfying `fn`, dropping the rest; errors always pass.
     *
     * @param fn the predicate applied to each successful value
     * @return a partial retaining the matching successes and all errors, dropping non-matching successes
     */
    def filter(fn: A => Boolean): Batch.Partial[E, A]

    /**
     * Map the values `fn` is defined on and drop the rest; errors pass through.
     *
     * @param fn the partial mapping applied to successful values
     * @tparam B the mapped value type
     * @return a partial with defined successes mapped, undefined successes dropped, and all errors kept
     */
    def collect[B](fn: PartialFunction[A, B]): Batch.Partial[E, B]

    /**
     * Split the value channel into two typed, disjoint halves by `fn`; error slots go to neither (see
     * [[Batch.partitionMap]] for the rationale).
     *
     * @param fn routes each value into the left (`Left`) or right (`Right`) half
     * @tparam A1 the value type of the left half
     * @tparam A2 the value type of the right half
     * @return `(lefts, rights)` — two error-free partials of this lineage; error slots appear in neither
     */
    def partitionMap[A1, A2](fn: A => Either[A1, A2]): (Batch.Partial[Nothing, A1], Batch.Partial[Nothing, A2])

    /**
     * Set every retained slot to `value`.
     *
     * @param value the value placed in every retained slot
     * @tparam B the value type
     * @return a partial of this lineage with every retained position `Right(value)`
     */
    def defaultValue[B](value: B): Batch.Partial[Nothing, B]

    /**
     * Set every retained slot to `error`.
     *
     * @param error the error placed in every retained slot
     * @tparam E2 the error type
     * @return a partial of this lineage with every retained position `Left(error)`
     */
    def defaultError[E2](error: E2): Batch.Partial[E2, Nothing]

    /**
     * Replace each value with its match in `other`, looked up by `key`; errors pass through.
     *
     * @param other    the lookup table, already keyed
     * @param key      the join key extracted from a value
     * @param notFound the error for a value absent from `other`, evaluated only on a miss
     * @tparam E2 the widened error type, admitting `notFound`'s failures
     * @tparam K  the join key
     * @tparam B  the replacement value
     * @return a partial with each value replaced by its match, misses turned into `notFound` errors
     */
    def replaceWith[E2 >: E, K, B](other: Map[K, B])(key: A => K, notFound: A => E2): Batch.Partial[E2, B]

    /**
     * As [[replaceWith]], pairing each value with its match as `(value, match)`.
     *
     * @param other    the lookup table, already keyed
     * @param key      the join key extracted from a value
     * @param notFound the error for a value absent from `other`, evaluated only on a miss
     * @tparam E2 the widened error type, admitting `notFound`'s failures
     * @tparam K  the join key
     * @tparam B  the matched value
     * @return a partial with each value paired with its match, misses turned into `notFound` errors
     */
    def associateWith[E2 >: E, K, B](other: Map[K, B])(key: A => K, notFound: A => E2): Batch.Partial[E2, (A, B)]
  }
}
