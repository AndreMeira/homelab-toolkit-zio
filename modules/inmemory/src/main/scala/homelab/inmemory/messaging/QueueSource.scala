package homelab.inmemory.messaging

import zio.{ Chunk, Queue, Ref, UIO, ZIO }


/**
 * The take-side of an in-memory channel: a tree of leaf queues that a [[QueueConsumer]] reads from.
 * A `Pure` wraps one raw [[Queue]]; a `Mapped` transforms every element; a `Merged` fairly interleaves
 * several sources behind one buffer.
 *
 * @tparam A the element delivered
 */
sealed trait QueueSource[A] {

  /**
   * Take one element if immediately available, without blocking.
   *
   * @return `Some(element)` if one is ready, else `None`
   */
  def poll: UIO[Option[A]]

  /**
   * Take one element, blocking until one is available.
   *
   * @return the next element
   */
  def take: UIO[A]

  /**
   * Take up to `n` elements, blocking until at least one is available.
   *
   * @param n the maximum number to take
   * @return between one and `n` elements, in take order
   */
  def takeUpTo(n: Int): UIO[List[A]]

  /**
   * Transform every element via `f`. Fluent shorthand for `QueueSource.Mapped(this, f)`.
   *
   * @param f the per-element mapping
   * @tparam B the mapped element type
   * @return a source delivering `f(a)` for each `a` of this source
   */
  def map[B](f: A => B): QueueSource[B] = QueueSource.Mapped(this, f)

  /**
   * Flatten this tree into its leaf sources, removing every `Merged` layer — each result is a `Pure`
   * or a `Mapped` (never a `Merged`). `Mapped` distributes through any nested `Merged`; a `Merged`'s
   * `destination`/`rotation`/`batching` are discarded. The element set is preserved up to per-source
   * order; the round-robin routing of intermediate `Merged` nodes is not.
   *
   * @return the leaf sources (`Pure` or `Mapped`) of this tree
   */
  def unnest: List[QueueSource[A]] = this match
    case QueueSource.Merged(_, sources, _, _) => sources.flatMap(_.unnest)
    case QueueSource.Mapped(source, fn)       => source.unnest.map(_.map(fn))
    case other                                => List(other)

  /**
   * Non-blocking batched poll — up to `n` elements currently available, possibly empty. Internal
   * primitive used by `Merged` to drain sources without per-element looping.
   *
   * @param n the maximum number to poll
   * @return up to `n` currently-available elements
   */
  private[messaging] def pollUpTo(n: Int): UIO[Chunk[A]]
}


object QueueSource {

  /**
   * A leaf source wrapping one raw [[Queue]].
   *
   * @param queue the backing queue
   */
  final case class Pure[A](queue: Queue[A]) extends QueueSource[A] {
    def poll: UIO[Option[A]]           = queue.poll
    def take: UIO[A]                   = queue.take
    def takeUpTo(n: Int): UIO[List[A]] = queue.takeBetween(1, n).map(_.toList)

    private[messaging] def pollUpTo(n: Int): UIO[Chunk[A]] = queue.takeUpTo(n)
  }

  /**
   * A source that applies `f` to every element of an underlying source. Composes over `Pure`,
   * `Mapped`, or `Merged` uniformly through the [[QueueSource]] interface.
   *
   * @param source the underlying source
   * @param f      the per-element mapping
   */
  final case class Mapped[A, B](source: QueueSource[B], f: B => A) extends QueueSource[A] {
    def poll: UIO[Option[A]]           = source.poll.map(_.map(f))
    def take: UIO[A]                   = source.take.map(f)
    def takeUpTo(n: Int): UIO[List[A]] = source.takeUpTo(n).map(_.map(f))

    private[messaging] def pollUpTo(n: Int): UIO[Chunk[A]] = source.pollUpTo(n).map(_.map(f))
  }

  /**
   * A source that fairly interleaves several `sources` behind a `destination` buffer, polling them
   * round-robin. `rotation` advances one step per poll (cross-call fairness); within a call, polling
   * is greedy up to `batching`.
   *
   * @param destination the buffer that drained elements land in
   * @param sources     the interleaved sources
   * @param rotation    the round-robin cursor
   * @param batching    the maximum elements pulled from sources per round
   */
  final case class Merged[A](
    destination: Queue[A],
    sources: List[QueueSource[A]],
    rotation: Ref[Int],
    batching: Int = 100,
  ) extends QueueSource[A] {

    /**
     * The size of one source-polling round: the smaller of `destination`'s remaining capacity and
     * `batching`, so a round never over-fetches beyond what the buffer can hold.
     *
     * @return the effective batch size for the next round
     */
    private def batchSize: UIO[Int] =
      destination.remainingCapacity.map(_.min(batching))

    /**
     * Poll: take from `destination` if buffered, else poll sources round-robin and buffer the
     * remainder.
     *
     * @return `Some(element)` if any is available, else `None`
     */
    def poll: UIO[Option[A]] =
      destination.poll.flatMap:
        case Some(a) => ZIO.succeed(Some(a))
        case None    =>
          pollSources.flatMap { chunk =>
            if chunk.isEmpty then ZIO.succeed(None)
            else destination.offerAll(chunk.tail) *> ZIO.succeed(Some(chunk.head))
          }

    /**
     * Take: `poll` fast-path, else `forkAndWait` — one forwarder fiber per source races the first
     * element into `destination`.
     *
     * @return the next element
     */
    def take: UIO[A] =
      poll.flatMap:
        case Some(a) => ZIO.succeed(a)
        case None    => forkAndWait

    /**
     * Take up to `n`: drain what's buffered/available, else block for one via `forkAndWait`.
     *
     * @param n the maximum number to take
     * @return between one and `n` elements
     */
    def takeUpTo(n: Int): UIO[List[A]] =
      pollUpTo(n).flatMap { chunk =>
        if chunk.isEmpty then forkAndWait.map(List(_))
        else ZIO.succeed(chunk.toList)
      }

    /**
     * Non-blocking drain: everything buffered in `destination`, then the remainder polled from
     * sources round-robin, buffering any overshoot.
     *
     * @param n the maximum number to poll
     * @return up to `n` currently-available elements
     */
    private[messaging] def pollUpTo(n: Int): UIO[Chunk[A]] =
      destination.takeUpTo(n).flatMap { fromDest =>
        val remaining = n - fromDest.size
        if remaining <= 0 then ZIO.succeed(fromDest)
        else
          ZIO.uninterruptible:
            pollSources.flatMap: chunk =>
              destination.offerAll(chunk.drop(remaining)) *>
                ZIO.succeed(fromDest ++ chunk.take(remaining))
      }

    /**
     * Poll sources round-robin from the rotation cursor, up to `batchSize`. The cursor advances
     * exactly once per call (cross-call fairness); within a call iteration is greedy (earlier sources
     * may exhaust the budget), which the next call's advanced start corrects.
     *
     * @return the polled elements in take order
     */
    private def pollSources: UIO[Chunk[A]] =
      if sources.isEmpty then ZIO.succeed(Chunk.empty)
      else
        for {
          start  <- rotation.getAndUpdate(index => (index + 1) % sources.size)
          count  <- batchSize
          result <- pollSourcesFrom(start, count)
        } yield result

    /**
     * Iterate sources from `start`, accumulating up to `batchSize`. Does not touch `rotation` —
     * called only from `pollSources`, which advances the cursor once per call.
     *
     * @param start     the source index to begin at
     * @param batchSize the remaining element budget
     * @param initial   the elements accumulated so far
     * @param visited   how many sources have been visited
     * @return the accumulated elements
     */
    private def pollSourcesFrom(
      start: Int,
      batchSize: Int,
      initial: Chunk[A] = Chunk.empty,
      visited: Int = 0,
    ): UIO[Chunk[A]] =
      if batchSize <= 0 || visited >= sources.size then ZIO.succeed(initial)
      else
        sources((start + visited) % sources.size)
          .pollUpTo(batchSize)
          .flatMap: chunk =>
            pollSourcesFrom(start, batchSize - chunk.size, initial ++ chunk, visited + 1)

    /**
     * Slow path: fork one forwarder per source, each racing (uninterruptibly-masked, take restorable)
     * to deliver the first element into `destination`, then take from `destination`.
     *
     * @return the first element any source yields
     */
    private def forkAndWait: UIO[A] = ZIO.scoped {
      ZIO.foreach(sources) { src =>
        ZIO
          .uninterruptibleMask(restore => restore(src.take).flatMap(destination.offer))
          .forkScoped
      } *> destination.take
    }
  }

  object Merged {

    /**
     * Merge `sources` behind a fresh unbounded buffer with a zeroed rotation cursor.
     *
     * @param sources the sources to interleave
     * @return the merged source
     */
    def make[A](sources: List[QueueSource[A]]): UIO[Merged[A]] =
      for {
        destination <- Queue.unbounded[A]
        rotation    <- Ref.make(0)
      } yield Merged(destination, sources, rotation)
  }

  extension [A](queue: Queue[A]) {

    /** Remaining capacity = declared capacity minus current size. */
    private def remainingCapacity: UIO[Int] = queue.size.map(queue.capacity - _)
  }
}
