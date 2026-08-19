package homelab.incubator.processing.pool.v2


import homelab.common.messaging.Consumer
import zio.*


/**
 * A consumer for queues that have no client — a store you must ask, rather than a broker that tells you.
 *
 * '''When to reach for this.''' A table used as a work queue is the motivating case: rows claimed with
 * `SELECT … FOR UPDATE SKIP LOCKED LIMIT n`, a `claimed_until` column for the lease, a `visible_at` column for
 * the retry delay. Nothing calls you when a row appears, so somebody has to ask — and asking well is the whole
 * problem this solves. The abstraction stays generic (anything that can claim, ack and nack without blocking
 * fits [[Source]]), but the design is shaped by that case, and the trade-offs below are priced for it.
 *
 * '''When not to.''' If the substrate ships a consumer client that pushes or long-polls — NATS, Kafka, SQS —
 * use it. Those already solve delivery, flow control and liveness better than a polling loop can, and wrapping
 * them here would buy nothing but a second layer of queues.
 *
 * '''The mechanism.''' A worker ready to run something offers a *demand* token; the fetcher claims from the
 * source only as much as it holds demand for, and hands back what the source did not fill. That splits the two
 * things a puller waits on, which a naive loop conflates:
 *
 *   - '''capacity''' — the fetcher blocks on `demand`, and a worker finishing is what frees it. Nothing is
 *     ever claimed that there is no capacity to run, so no row is marked claimed while its handler queues.
 *   - '''data''' — the wake-up, which means one thing only: the source may be non-empty.
 *
 * Capacity is therefore a token held, not a number computed: no in-flight counter, no allowance arithmetic, no
 * reserve-then-return-the-remainder, and no query issued for zero rows. The pool's own concurrency is the
 * capacity bound, and `pollSize` means what it says — the `LIMIT`.
 *
 * '''Liveness.''' [[Worker.wakeUp]] is how anything says work exists, and it is '''process-local''': it wakes
 * this pool only. Rows inserted by another instance, or by anything outside this JVM, will not wake it — so a
 * periodic tick is the mechanism that makes the pool correct, and `wakeUp` is the latency optimisation on top
 * for writers in the same process. Ticking is cheap by construction: a saturated pool never reaches the
 * wake-up, so it issues no queries at all while its workers are busy, and the dropping-of-one means a long
 * idle spell wakes to a single query rather than a burst.
 *
 * '''The cost.''' `supply` briefly holds *claimed* elements — a lease with its clock running, between the
 * claim and the worker taking it. It is a handoff rather than a buffer (its depth is bounded by the number of
 * idle workers, so every element in it has a worker already reaching for it), and against a lease measured in
 * seconds or minutes the exposure is negligible. It is still why the [[Fetcher]] drains and nacks on the way
 * out, and it is why this shape would be wrong for a substrate whose lease is measured in milliseconds.
 */
object DemandDriven:

  /**
   * A store that hands out leases: claiming never blocks, and every claim is settled exactly once.
   *
   * Over a table the three methods are one statement each — claim with `SELECT … FOR UPDATE SKIP LOCKED`
   * setting a `claimed_until`, ack by deleting or marking done, nack by pushing `visible_at` forward. The
   * lease is what makes a crashed pool recoverable: an expired claim must become available again, so the
   * claiming query has to treat a stale `claimed_until` as free. Nothing here enforces that — it is the one
   * obligation an implementor carries alone.
   *
   * @tparam E the error claiming or settling aborts with
   * @tparam A the element claimed
   */
  trait Source[E, A]:

    /**
     * Claim up to `upTo` elements without blocking. An empty list means nothing is available *now*, not that
     * the source is exhausted.
     *
     * '''Must return at most `upTo`.''' Each element handed back is one a worker is already waiting for; an
     * extra would sit in `supply` with no demand paying for it, its lease ticking with nobody to settle it.
     *
     * '''Must be cheap when empty''', because that is the common case: an idle pool asks once per tick, and
     * this is the query it runs. Index for it.
     *
     * @param upTo the ceiling — always positive, since the caller holds that much demand
     * @return the claimed elements, at most `upTo` of them
     */
    def tryAcquire(upTo: Int): IO[E, List[A]]

    /**
     * Retire elements: they will not be handed out again.
     *
     * Batch-shaped to mirror [[tryAcquire]] — claim n, settle n — and because settling is the one thing a
     * store-backed source does per element rather than per poll, so it is where the statement count lands.
     * `DELETE … WHERE id = ANY($1)` collapses a batch into one round trip. Today the pool calls this one
     * element at a time; the shape is here so that grouping later is a change inside the pool rather than a
     * break for every implementor.
     *
     * @param elements the claimed elements, never empty
     * @return noop once retired
     */
    def ack(elements: List[A]): IO[E, Unit]

    /**
     * Return elements to the store, available again after `wait`.
     *
     * The delay is uniform across the batch, which is what keeps it a single statement. Per-element backoff
     * belongs in the store instead — computed from an attempt counter as part of the same update — rather than
     * as a parameter that would force one statement per element.
     *
     * @param elements the claimed elements, never empty
     * @param wait how long before they may be handed out again
     * @return noop once returned
     */
    def nack(elements: List[A], wait: Duration): IO[E, Unit]

  /**
   * The wake-up: a queue of one, dropping — a token that persists until taken but never banks a second.
   *
   * Opaque so it can only be built by [[Signal.make]]: the capacity and the drop policy *are* the contract. It
   * says "the source may be non-empty" and nothing else; capacity is `demand`'s job.
   *
   * Internal: the pool builds its own, and exposes the raising end as [[Worker.wakeUp]]. Callers never hold
   * one — whoever knows that work arrived, be it a subscription callback, a scheduled tick, or a write on the
   * other side of the table, reaches it through the worker.
   */
  private[v2] opaque type Signal = Signal.Type

  private[v2] object Signal:
    opaque type Type <: Queue[Unit] = Queue[Unit]

    /**
     * A fresh signal, holding at most one untaken token.
     *
     * @return the signal; never fails
     */
    def make: UIO[Signal] = Queue.dropping(1)

    extension (signal: Signal)

      /**
       * Tell the pool the source may have work now. Never blocks and never fails: a token already pending
       * absorbs this one, which is the whole reason for the drop policy.
       *
       * @return noop
       */
      def raise: UIO[Unit] = signal.offer(()).unit

  /**
   * The channel between the fetcher and its workers: demand flowing one way, claimed elements the other.
   *
   * Both queues are sized to the same `capacity`, and that is what keeps the offers non-blocking. Every token
   * in `demand`, every element in `supply`, and every token the fetcher currently holds belongs to exactly one
   * waiting worker, so the total can never exceed the number of workers — '''build it with a capacity at
   * least as large as the pool's concurrency'''.
   *
   * @param demand capacity offered by workers — one token is the right to claim one element
   * @param supply elements already claimed, waiting for the worker whose demand paid for them
   * @param signal raised when the source may have become non-empty
   * @param failure the fetcher's death certificate — see [[Fetcher.make]]
   * @tparam E the error the fetcher aborts with
   * @tparam A the element carried
   */
  final private[v2] case class Channel[E, A](
    demand: Queue[Unit],
    supply: Queue[A],
    signal: Signal,
    failure: Promise[E, Nothing],
  )

  private[v2] object Channel:

    /**
     * A channel sized for `capacity` concurrent workers, around a signal its caller already holds.
     *
     * @param capacity the pool's concurrency — see the class doc for why both queues take it
     * @param signal the wake-up the fetcher parks on, owned by the caller so it can be raised from outside
     * @tparam E the error the fetcher aborts with
     * @tparam A the element carried
     * @return the channel; never fails
     */
    def make[E, A](capacity: Int, signal: Signal): UIO[Channel[E, A]] =
      for
        demand  <- Queue.bounded[Unit](capacity)
        supply  <- Queue.bounded[A](capacity)
        failure <- Promise.make[E, Nothing]
      yield Channel(demand, supply, signal, failure)

  /**
   * Claims from the source on behalf of waiting workers, and never more than they asked for. One per pool,
   * running as its own fiber for as long as the pool lives.
   *
   * @param source the leased source to claim from
   * @param channel the demand it serves and the supply it fills
   * @param pollSize the most it will claim in one call
   * @param nackDelay how long an element returned at shutdown stays unavailable
   * @tparam E the error claiming aborts with
   * @tparam A the element claimed
   */
  final private[v2] class Fetcher[E, A] private (
    source: Source[E, A],
    channel: Channel[E, A],
    pollSize: Int,
    nackDelay: Duration,
  ):

    /**
     * Fetch until interrupted. Says nothing about shutdown: stopping and giving back are separate steps, and
     * [[Fetcher.make]] is what sequences them.
     *
     * @return never completes successfully; aborts with `E` if the source fails
     */
    private def run: IO[E, Nothing] = step.forever

    /**
     * One cycle: wait for demand, claim at most that much, and place what arrived.
     *
     * Demand is taken *before* the source is touched, so nothing is ever claimed without a worker waiting for
     * it — the same reservation discipline as v1's permit, but as a value rather than a counter. `takeBetween`
     * blocks for the first token and sweeps up whatever else is there, so a busy pool batches and an idle one
     * still makes progress with a single waiter.
     *
     * Tokens the source did not fill go straight back, so capacity is never lost to a poll that came up short.
     * On an empty poll every token is returned before parking: a parked fetcher holds nothing, exactly as a
     * parked v1 consumer held no permits.
     *
     * @return noop once the batch is placed; aborts with `E` if the source fails
     */
    private def step: IO[E, Unit] =
      channel.demand.takeBetween(1, pollSize).flatMap { tokens =>
        source.tryAcquire(upTo = tokens.size).flatMap {
          case Nil    => channel.demand.offerAll(tokens) *> channel.signal.take.unit
          case claims => channel.supply.offerAll(claims) *> channel.demand.offerAll(tokens.drop(claims.size)).unit
        }
      }

    /**
     * Return everything claimed but not yet handed to a worker.
     *
     * Without this, a closing pool leaves those elements held until their lease expires — the one cost of
     * putting a queue between the claim and the work. Settlement failures are ignored: the pool is already on
     * its way out, and the lease expiring is the fallback.
     *
     * '''Only correct once [[run]] has stopped.''' Draining a live fetcher empties a queue it is still
     * filling: some elements come back, and whatever is placed a moment later is stranded. [[Fetcher.make]]
     * arranges for the fetcher to be interrupted first.
     *
     * @return noop once the supply is empty
     */
    private def drain: UIO[Unit] =
      channel.supply.takeAll
        .flatMap(stranded => source.nack(stranded.toList, nackDelay).ignore.unless(stranded.isEmpty))
        .unit
        .uninterruptible

  private[v2] object Fetcher:

    /**
     * A fetcher as an acquired resource: built and started on acquisition, stopped and emptied on release.
     *
     * Shutdown is two steps in a fixed order — '''stop fetching, then give back what is queued''' — and this
     * is where that is stated. The release is only [[drain]]; the stopping comes from the fork being tied to
     * the same scope, and the ordering between them is the subject of the warning below.
     *
     * Not public: a started fetcher is only useful to the workers on the other end of its channel, so
     * [[Worker.make]] is the one thing that calls this, and it does so with a channel nobody else holds. There
     * is no way to end up with a channel whose fetcher was never started.
     *
     * Starting eagerly is free: `step` opens on `demand.takeBetween`, so a fetcher with no workers behind it
     * has claimed nothing and touched the source not at all. It costs a parked fiber, and nothing else.
     *
     * The fork is why the failure promise exists. A forked `run` that dies takes its error with it, and the
     * workers behind it would park on an empty `supply` forever; routing the cause into `channel.failure` is
     * what lets [[Worker.consume]] abort instead of hang. Interruption is not a failure and is not routed —
     * the scope closing interrupts the fetcher and the workers alike. Note that a fetcher which *fails* is not
     * drained on the spot: its supply waits for the release, which the aborting workers bring on promptly.
     *
     * '''The fork must stay in the `tap`, outside the acquire.''' Two separate things break if it moves in,
     * and neither announces itself:
     *
     *   1. `acquireRelease` runs its acquire uninterruptibly, and a fiber inherits the interrupt status of its
     *      parent at fork time. A fetcher forked there is born uninterruptible, so the scope can never stop it
     *      and closing '''deadlocks'''.
     *   1. Scope finalizers run last-registered-first. Forking inside the acquire registers the fetcher's
     *      interrupt-on-close *before* the release, so the drain would run while the fetcher was still
     *      stepping — emptying a queue it is still filling, and stranding whatever lands next. Keeping the
     *      fork in the `tap` registers it second, so it is interrupted first.
     *
     * Both are pinned by `ScopeOrderingSpec`, which exists because neither is visible in the code.
     *
     * @param source the leased source to claim from
     * @param channel the demand it serves, the supply it fills, and the promise it dies into
     * @param pollSize the most it will claim in one call
     * @param nackDelay how long an element returned at shutdown stays unavailable
     * @tparam E the error the source aborts with
     * @tparam A the element claimed
     * @return the running fetcher; never fails — its own failure lands in the channel instead
     */
    def make[E, A](
      source: Source[E, A],
      channel: Channel[E, A],
      pollSize: Int,
      nackDelay: Duration,
    ): ZIO[Scope, Nothing, Fetcher[E, A]] =
      ZIO
        .acquireRelease(ZIO.succeed(Fetcher(source, channel, pollSize, nackDelay)))(_.drain)
        .tap(_.run.catchAllCause(channel.failure.failCause(_).unit).forkScoped)

  /**
   * The pool's public face, and the only type here anyone outside builds: one `consume` call takes one element,
   * runs `logic`, and settles it. A run loop calls it repeatedly, and several such loops run concurrently — up
   * to the `concurrency` it was made with.
   *
   * Offering demand *before* taking supply is the whole protocol: the fetcher claims only what has been asked
   * for, so nothing is leased speculatively.
   *
   * It is a `Consumer` plus one thing a `Consumer` cannot express: [[wakeUp]], the producer-facing end. Both
   * halves come from one object so the caller never has to build, hold, or match up a separate wake-up.
   *
   * @param source the leased source, used only to settle
   * @param channel the demand it offers and the supply it takes from
   * @param nackDelay how long a failed element stays unavailable
   * @tparam E the error settling aborts with
   * @tparam A the element handled
   */
  final class Worker[E, A] private (source: Source[E, A], channel: Channel[E, A], nackDelay: Duration) extends Consumer[E, A]:

    /**
     * Tell this pool its source may have work now. Never blocks and never fails — a wake-up already pending
     * absorbs this one, so a chatty producer costs at most one extra poll rather than a poll per call.
     *
     * '''It wakes this pool only.''' Work created by another instance, or by anything outside this process,
     * cannot reach it — so a periodic tick is what keeps the pool live, and this is the shortcut for writers
     * that happen to share the process (call it after the insert commits, not before). A pool with neither
     * polls once, finds nothing, and parks forever.
     *
     * Raising while every worker is busy is free: the fetcher is waiting on demand, not on the wake-up, so a
     * saturated pool never even reads it.
     *
     * @return noop
     */
    def wakeUp: UIO[Unit] = channel.signal.raise

    /**
     * Take one element and run `logic` on it, settling by the outcome.
     *
     * The wait for supply races the fetcher's death: without that, a fetcher that has failed leaves every
     * worker parked on a queue nothing will ever fill again, and the pool hangs instead of reporting.
     *
     * @param logic processes one element
     * @tparam E2 the widened error, admitting `logic`'s failures
     * @return noop once the element is settled; aborts with `E2` if `logic` failed or the fetcher died
     */
    override def consume[E2 >: E](logic: A => IO[E2, Unit]): IO[E2, Unit] =
      channel.demand.offer(()) *>
        channel.supply.take.raceFirst(channel.failure.await).flatMap(settle(_, logic))

    /**
     * Run `logic` and settle, whatever happens.
     *
     * Only `logic` is interruptible: settlement runs to completion even as the pool shuts down, so an element
     * is never left claimed with nobody to answer for it. The failure is re-raised after the nack rather than
     * swallowed, so a run loop sees it.
     *
     * Settling one element at a time is deliberate for now — a batch of one, over a batch-shaped port. Workers
     * finish independently, so grouping them means an accumulator between here and the store, and that buys a
     * fiber whose flush has to be sequenced against shutdown. Worth it when the statement count justifies it,
     * and the port shape is what keeps that a change here rather than everywhere.
     *
     * @param element the claimed element
     * @param logic processes it
     * @tparam E2 the widened error
     * @return noop once settled; aborts with `E2` if `logic` failed
     */
    private def settle[E2 >: E](element: A, logic: A => IO[E2, Unit]): IO[E2, Unit] =
      ZIO.uninterruptibleMask: restore =>
        restore(logic(element)).exit.flatMap:
          case Exit.Success(_)           => source.ack(List(element))
          case failure @ Exit.Failure(_) => source.nack(List(element), nackDelay) *> failure

  object Worker:

    /**
     * Build a pool over `source` and start it, for the life of the scope: the channel, the fetcher fiber
     * behind it, and the consumer in front of it.
     *
     * This is the only entry point, because the channel and the fetcher are useless apart and dangerous
     * apart — a channel whose fetcher was never started is a consumer that blocks forever, and nothing in a
     * type signature would have warned you. Building all three together removes the ordering from the API
     * surface: there is no unstarted state to hold.
     *
     * The returned consumer is safe to call from `concurrency` fibers at once, and '''that is the bound it was
     * sized for''' — more concurrent callers than that and the demand queue backs up, so a caller can block in
     * `consume` before any element is claimed for it. Fewer is merely idle capacity.
     *
     * The wake-up is built here rather than taken, and handed back as [[Worker.wakeUp]] — which is why this
     * returns the worker rather than a bare `Consumer`. A pool that is never woken polls once, finds nothing,
     * and parks, so give `wakeUp` to whatever knows work arrived.
     *
     * `Consumer`'s combinators (`map`, `mapZIO`, …) return a plain `Consumer`, so a composed pool no longer
     * carries `wakeUp`. That costs nothing: `wakeUp` is an ordinary effect value, so lift it out first and
     * compose the consumer freely afterwards.
     *
     * @param source the leased source to claim from and settle against
     * @param concurrency how many fibers will call `consume` at once — the channel's size
     * @param pollSize the most the fetcher will claim from the source in one call
     * @param nackDelay how long an element returned unprocessed stays unavailable
     * @tparam E the error the source aborts with
     * @tparam A the element handled
     * @return the worker, with its fetcher already running; never fails
     */
    def make[E, A](
      source: Source[E, A],
      concurrency: Int,
      pollSize: Int,
      nackDelay: Duration,
    ): ZIO[Scope, Nothing, Worker[E, A]] =
      for
        signal  <- Signal.make
        channel <- Channel.make[E, A](concurrency, signal)
        _       <- Fetcher.make(source, channel, pollSize, nackDelay)
      yield Worker(source, channel, nackDelay)
