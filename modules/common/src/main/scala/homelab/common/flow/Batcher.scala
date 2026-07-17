package homelab.common.flow


import homelab.common.data.Batch
import homelab.common.data.Batch.LineageMismatch
import homelab.common.error.ApplicationError
import homelab.common.flow.batching.{ Adaptive, DeduplicatedSerial, Distributed, Serial }
import zio.*


/**
 * Coalesces many single-item [[run]] calls into fewer bulk [[Batcher.Logic]] calls — the DataLoader /
 * request-batching pattern. Build one with the [[Batcher.serial]], [[Batcher.deduplicated]],
 * [[Batcher.keyed]], [[Batcher.distributed]], or [[Batcher.adaptive]] factories; they compose freely (e.g.
 * adaptive over distributed over deduplicated). Every factory is scoped — closing the scope drops pending work
 * and interrupts in-flight callers, so no caller ever hangs.
 *
 * There is deliberately no environment type: a batcher's drain is shared across many callers, so a per-call
 * environment would be captured from whichever caller happened to fork the drain and reused for everyone. The
 * `Logic`'s dependencies belong to *construction* — provide them when you build the batcher (a closure, or
 * `ZIO.serviceWith`). `E` is covariant, so the error widens freely — e.g. to a common `ApplicationError`.
 *
 * @tparam E   the caller-facing error (typically a [[Batcher.Failure]])
 * @tparam In  a single request
 * @tparam Out its result
 */
trait Batcher[+E, In, Out]:

  /**
   * Submit one request; it is coalesced with concurrent submissions into a bulk [[Batcher.Logic]] call.
   *
   * @param in the request
   * @return its result; aborts with `E` (a whole-batch failure, a per-item failure, or a [[LineageMismatch]]),
   *         or is interrupted if the batcher's scope closes while it is waiting
   */
  def run(in: In): IO[E, Out]

  /**
   * Run one request immediately, *without* coalescing — the low-load path [[Batcher.adaptive]] routes to.
   * Defaults to [[run]]; the concrete batchers override it with a true one-shot call. Internal to the batcher
   * family (a batcher's public contract is just [[run]]); a batcher that does not override this loses the
   * adaptive fast path — it merely coalesces.
   *
   * @param in the request
   * @return its result, computed on its own; aborts as [[run]] does
   */
  private[flow] def direct(in: In): IO[E, Out] = run(in)


object Batcher:

  /**
   * The batcher's caller-facing error: a [[LineageMismatch]] (a `Logic` returned a non-derived batch — an
   * implementation defect the caller may log, retry, or escalate), a whole-batch `E`, or a per-item `BE`. All
   * three are `ApplicationError`s, so the union widens to `ApplicationError` at any call site that does not
   * branch on them.
   *
   * @tparam E  a whole-batch failure
   * @tparam BE a per-item failure
   */
  type Failure[+E, +BE] = LineageMismatch | E | BE

  /** Construction-time misconfiguration — `batchSize` must be at least 1. A defect in the calling code, hence
    * an [[ApplicationError.ImplementationError]], but surfaced (not thrown) so the caller decides how to react. */
  final case class InvalidBatchSize(size: Int) extends ApplicationError.ImplementationError:
    override def message: String = s"Batcher batchSize must be >= 1, got $size"

  /** Construction-time misconfiguration — `parallelism` must be at least 1. Same category as [[InvalidBatchSize]]. */
  final case class InvalidParallelism(parallelism: Int) extends ApplicationError.ImplementationError:
    override def message: String = s"Batcher parallelism must be >= 1, got $parallelism"

  /**
   * The bulk operation a batcher coalesces onto. It carries no environment — bake dependencies in when
   * building the batcher (see [[Batcher]]).
   *
   * @tparam E  a whole-batch failure (the bulk call itself failed)
   * @tparam BE a per-item failure (routed to just that item's caller)
   */
  trait Logic[E, BE, In, Out]:
    /**
     * Run the bulk operation over `input`, producing a result **derived from that batch** — via
     * `map`/`mapEither`/`replaceWith`/… — so results line up with inputs by lineage. Returning a fresh
     * `Batch.make` breaks that alignment and surfaces to callers as [[LineageMismatch]].
     *
     * @param input the batch of distinct inputs
     * @return a lineage-derived result batch; aborts with `E` on a whole-batch failure
     */
    def run(input: Batch.Success[In]): IO[E, Batch[BE, Out]]

  object Logic:
    /**
     * Wrap a function as [[Logic]], preserving the same lineage contract as [[Logic.run]] (the returned
     * batch must be derived from the provided `input`).
     *
     * @param fn the bulk function to adapt into [[Logic]]
     * @return a [[Logic]] that delegates to `in`
     */
    def fromFunction[E, BE, In, Out](
      fn: Batch.Success[In] => IO[E, Batch[BE, Out]]
    ): Logic[E, BE, In, Out] = fn(_)

  /**
   * A FIFO batcher: concurrent [[Batcher.run]] calls coalesce into bulk `logic` calls, one batch in flight at
   * a time.
   *
   * @param batchSize the maximum inputs per bulk call (must be `>= 1`)
   * @param logic     the bulk operation
   * @tparam E  a whole-batch failure from `logic`
   * @tparam BE a per-item failure from `logic`
   * @return a scoped batcher; aborts with [[InvalidBatchSize]] if `batchSize < 1`
   */
  def serial[E, BE, In, Out](
    batchSize: Int,
    logic: Logic[E, BE, In, Out],
  ): ZIO[Scope, InvalidBatchSize, Batcher[Failure[E, BE], In, Out]] =
    for
      _      <- guard(batchSize)
      scope  <- ZIO.scope
      ref    <- Ref.make[Serial.State[Failure[E, BE], In, Out]](Serial.State.Idle())
      batcher = Serial[E, BE, In, Out](batchSize, scope, ref, logic)
      _      <- ZIO.addFinalizer(batcher.abandon)
    yield batcher

  /**
   * As [[serial]], but also coalesces by key: requests sharing a `key` collapse to a single downstream
   * computation and a single shared promise, so a duplicate allocates nothing and enqueues nothing.
   *
   * @param batchSize the maximum inputs per bulk call (must be `>= 1`)
   * @param key       extracts the coalescing key; callers with equal keys share a result, so it must capture
   *                  everything that determines the result (a cache key)
   * @param logic     the bulk operation
   * @return a scoped batcher; aborts with [[InvalidBatchSize]] if `batchSize < 1`
   */
  def deduplicated[E, BE, Key, In, Out](
    batchSize: Int,
    key: In => Key,
    logic: Logic[E, BE, In, Out],
  ): ZIO[Scope, InvalidBatchSize, Batcher[Failure[E, BE], In, Out]] =
    for
      _     <- guard(batchSize)
      scope <- ZIO.scope
      ref   <- Ref.make[DeduplicatedSerial.State[Key, Failure[E, BE], In, Out]](DeduplicatedSerial.State.Idle())
      b      = DeduplicatedSerial[E, BE, Key, In, Out](batchSize, scope, ref, key, logic)
      _     <- ZIO.addFinalizer(b.abandon)
    yield b

  /**
   * A keyed loader: a batcher whose input *is* the key, [[deduplicated]] by that key. Concurrent requests
   * coalesce into one bulk `fetch` over the distinct keys; its result `Map` is re-associated back to each
   * caller, and a key absent from the map becomes `notFound`. This is the shape you wrap behind a named domain
   * port (a `*Loader`/`*Lookup`).
   *
   * @param batchSize the maximum distinct keys per `fetch` (must be `>= 1`)
   * @param fetch     the bulk lookup — always given a non-empty set of distinct keys
   * @param notFound  the per-item error for a key `fetch` did not return
   * @return a scoped batcher; aborts with [[InvalidBatchSize]] if `batchSize < 1`
   */
  def keyed[E, BE, Key, Out](
    batchSize: Int,
    fetch: NonEmptyChunk[Key] => IO[E, Map[Key, Out]],
    notFound: Key => BE,
  ): ZIO[Scope, InvalidBatchSize, Batcher[Failure[E, BE], Key, Out]] =
    deduplicated(batchSize, identity, keyedLogic(fetch, notFound))

  /**
   * The [[Logic]] behind [[keyed]]: run `fetch` over the batch's keys and re-associate its result `Map` by key
   * via `Batch.replaceWith` (lineage-preserving), turning a missing key into `notFound`. Exposed so a keyed
   * loader can compose with any strategy — e.g. `distributed(bs, n, identity, Batcher.keyedLogic(fetch, nf))`.
   *
   * @param fetch    the bulk lookup
   * @param notFound the per-item error for a key `fetch` did not return
   * @return a bulk operation over a batch of distinct keys
   */
  def keyedLogic[E, BE, Key, Out](
    fetch: NonEmptyChunk[Key] => IO[E, Map[Key, Out]],
    notFound: Key => BE,
  ): Logic[E, BE, Key, Out] = input =>
    NonEmptyChunk.fromIterableOption(input.values) match
      case Some(keys) => fetch(keys).map(found => input.replaceWith(found)(identity, notFound))
      case None       => ZIO.succeed(input.replaceWith(Map.empty[Key, Out])(identity, notFound)) // never empty

  /**
   * Build [[Logic]] from a bulk `fetch` that may miss keys, re-associating by key and returning
   * `Option[Out]` per item (`Some` when found, `None` when missing).
   *
   * Uses `Map.get` over the fetched map and maps each input slot to its optional match, preserving lineage.
   * Exposed so callers can compose this keyed-optional behavior with any batching strategy.
   *
   * @param fetch the bulk lookup — always given a non-empty set of distinct keys
   * @return a bulk operation over keys that yields optional per-key results
   */
  def fetchLogic[E, Key, Out](
    fetch: NonEmptyChunk[Key] => IO[E, Map[Key, Out]]
  ): Logic[E, Nothing, Key, Option[Out]] = input =>
    NonEmptyChunk.fromIterableOption(input.values) match
      case Some(keys) => fetch(keys).map(found => input.map(found.get))
      case None       => ZIO.succeed(input.defaultValue(None)) // never empty

  /**
   * `parallelism` independent [[serial]] shards, so up to that many bulk calls run at once. Inputs are
   * hash-partitioned across shards.
   *
   * @param batchSize   the maximum inputs per bulk call, per shard (must be `>= 1`)
   * @param parallelism the number of shards (must be `>= 1`)
   * @param logic       the bulk operation
   * @return a scoped batcher; aborts with [[InvalidBatchSize]] or [[InvalidParallelism]] if either is `< 1`
   */
  def distributed[E, BE, In, Out](
    batchSize: Int,
    parallelism: Int,
    logic: Logic[E, BE, In, Out],
  ): ZIO[Scope, InvalidBatchSize | InvalidParallelism, Batcher[Failure[E, BE], In, Out]] =
    distribute(parallelism, _.hashCode, serial(batchSize, logic))

  /**
   * As [[distributed]], but over [[deduplicated]] shards, sharded by the *same* `key` so a key always lands on
   * one shard and dedup is preserved — parallel across keys, coalesced per key.
   *
   * @param key the coalescing *and* sharding key
   */
  def distributed[E, BE, Key, In, Out](
    batchSize: Int,
    parallelism: Int,
    key: In => Key,
    logic: Logic[E, BE, In, Out],
  ): ZIO[Scope, InvalidBatchSize | InvalidParallelism, Batcher[Failure[E, BE], In, Out]] =
    distribute(parallelism, key(_).hashCode, deduplicated(batchSize, key, logic))

  /**
   * Wraps any batcher to make it load-adaptive: while fewer than `threshold` calls are in flight, each runs
   * via the inner batcher's unbatched `direct` path (single-call latency, no coordination); at or above
   * `threshold`, calls route to the inner's `run` and coalesce. Composes over any inner — e.g.
   * `adaptive(4, serial(…))`, `adaptive(4, deduplicated(…))`, `adaptive(4, distributed(…))`.
   *
   * The low-load path does not coalesce (nor deduplicate) — it trades batching for latency.
   *
   * @param threshold the in-flight concurrency at which batching switches on
   * @param inner     the batcher to wrap (as a scoped recipe)
   * @tparam CErr the inner's construction error, propagated unchanged
   * @return a scoped batcher; aborts with `CErr` if the inner does
   */
  def adaptive[E, CErr, In, Out](
    threshold: Int,
    inner: ZIO[Scope, CErr, Batcher[E, In, Out]],
  ): ZIO[Scope, CErr, Batcher[E, In, Out]] =
    for
      batcher  <- inner
      inFlight <- Ref.make(0)
    yield Adaptive[E, In, Out](threshold, inFlight, batcher)

  /**
   * Build `parallelism` shards from `shard` and wrap them in a [[Distributed]] routed by `shardOf`.
   *
   * @param parallelism the number of shards
   * @param shardOf     picks a shard for an input (floor-modded into range)
   * @param shard       the per-shard recipe, re-run once per shard
   * @tparam CErr the shard's construction error, propagated unchanged
   * @return a scoped distributed batcher; aborts with [[InvalidParallelism]] (if `< 1`) or the shard's `CErr`
   */
  private def distribute[E, CErr, In, Out](
    parallelism: Int,
    shardOf: In => Int,
    shard: ZIO[Scope, CErr, Batcher[E, In, Out]],
  ): ZIO[Scope, InvalidParallelism | CErr, Batcher[E, In, Out]] =
    for
      _      <- ZIO.fail(InvalidParallelism(parallelism)).when(parallelism < 1)
      shards <- ZIO.foreach(1 to parallelism)(_ => shard)
    yield Distributed[E, In, Out](shards.toVector, shardOf)

  /**
   * Reject a non-positive `batchSize` at construction.
   *
   * @param batchSize the configured size
   * @return unit; aborts with [[InvalidBatchSize]] if `batchSize < 1`
   */
  private def guard(batchSize: Int): IO[InvalidBatchSize, Unit] =
    ZIO.fail(InvalidBatchSize(batchSize)).when(batchSize < 1).unit
