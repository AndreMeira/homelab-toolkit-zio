package homelab.incubator.processing.pool.v3


import homelab.common.messaging.Consumer
import zio.*


/**
 * The v2 pool with settlement moved onto a queue of its own, so both directions of store traffic are batched.
 *
 * v2 batched the claim (`LIMIT n`, one query per poll) but settled one element per statement, which is where
 * the query count lands at steady state. Here a worker's outcome goes onto a third queue and a dedicated
 * settler drains it, so `ack`/`nack` see whatever finished while the previous write was in flight — batching
 * that tightens exactly when it needs to and disappears when the pool is quiet.
 *
 * The shape that falls out is worth more than the query count. Every touch of the store now belongs to one
 * fiber, and the worker touches it not at all:
 *
 *   - '''fetcher''' — takes demand, claims, fills supply. Owns the read.
 *   - '''worker''' — takes supply, runs `logic`, files a verdict. Owns nothing but the work; it does not even
 *     hold a [[Source]].
 *   - '''settler''' — takes verdicts, writes them. Owns the write.
 *
 * '''Capacity is still one token per caller, and still end-to-end.''' A worker does not return from `consume`
 * until its element is durably settled, so a caller cannot claim a second element while its first is only
 * recorded in memory. That keeps v2's invariant — outstanding leases never exceed `concurrency` — which a
 * fire-and-forget settlement queue would have quietly given up. Under load the cost is nil: several callers
 * finishing together are settled by one statement and released together, which is the batching.
 *
 * '''Teardown is ordered, and the order is load-bearing.''' [[Worker.make]] registers the settler before the
 * fetcher, so scope finalizers (last-registered-first) run: workers interrupted → fetcher stopped → supply
 * drained → settler stopped → settlement flushed. The settler outliving the workers is what lets a worker
 * interrupted mid-await still have its verdict written, and it is why the registration order in `make` is not
 * cosmetic. See `ScopeOrderingSpec` in v2 for the finalizer mechanics this rests on.
 *
 * Everything else — the wake-up, the demand/supply protocol, the drain — is v2's, unchanged.
 */
object DemandDriven:

  /**
   * A store that hands out leases: claiming never blocks, and every claim is settled exactly once.
   *
   * Identical to v2's port. Over a table the three methods are one statement each — claim with
   * `SELECT … FOR UPDATE SKIP LOCKED` setting a `claimed_until`, ack by deleting or marking done, nack by
   * pushing `visible_at` forward. An expired claim must become available again, which the claiming query has
   * to arrange by treating a stale `claimed_until` as free; nothing here enforces it.
   *
   * @tparam E the error claiming or settling aborts with
   * @tparam A the element claimed
   */
  trait Source[E, A]:

    /**
     * Claim up to `upTo` elements without blocking. An empty list means nothing is available *now*, not that
     * the source is exhausted.
     *
     * '''Must return at most `upTo`''', and '''must be cheap when empty''' — on an idle pool this is the query
     * run once per wake-up.
     *
     * @param upTo the ceiling — always positive, since the caller holds that much demand
     * @return the claimed elements, at most `upTo` of them
     */
    def tryAcquire(upTo: Int): IO[E, List[A]]

    /**
     * Retire elements: they will not be handed out again.
     *
     * @param elements the claimed elements, never empty
     * @return noop once retired
     */
    def ack(elements: List[A]): IO[E, Unit]

    /**
     * Return elements to the store, available again after `wait`.
     *
     * The delay is uniform across the batch, which is what keeps it one statement. Per-element backoff belongs
     * in the store, computed from an attempt counter as part of the same update.
     *
     * @param elements the claimed elements, never empty
     * @param wait how long before they may be handed out again
     * @return noop once returned
     */
    def nack(elements: List[A], wait: Duration): IO[E, Unit]

  /**
   * The wake-up: a queue of one, dropping — a token that persists until taken but never banks a second.
   *
   * Internal; the pool builds its own and exposes the raising end as [[Worker.wakeUp]].
   */
  opaque private[v3] type Signal = Signal.Type

  private[v3] object Signal:
    opaque type Type <: Queue[Unit] = Queue[Unit]

    /**
     * A fresh signal, holding at most one untaken token.
     *
     * @return the signal; never fails
     */
    def make: UIO[Signal] = Queue.dropping(1)

    extension (signal: Signal)

      /**
       * Raise the wake-up. Never blocks, never fails: a pending token absorbs this one.
       *
       * @return noop
       */
      def raise: UIO[Unit] = signal.offer(()).unit

  /** What a worker decided about an element: whether the store should retire it or hand it out again. */
  private[v3] enum Verdict:
    case Done, Failed

  /**
   * One element's verdict, in transit to the settler, with the promise the worker is waiting on.
   *
   * The promise is what makes settlement synchronous from the caller's side without making it unbatched: the
   * settler completes a whole batch's worth at once.
   *
   * It '''only ever succeeds'''. A write that fails kills the settler, and its cause lands in the channel's
   * failure promise, which [[Worker.consume]] is already racing — so failing this one too would tell the
   * worker something it is about to be told anyway. Keeping it `Promise[Nothing, Unit]` is what lets this type
   * and [[Settlement]] be covariant, which in turn is what lets `Closed` be a parameterless case.
   *
   * @param element the claimed element
   * @param verdict what the worker decided
   * @param settled completed once the store has recorded it
   * @tparam A the element settled
   */
  final private[v3] case class Pending[+A](element: A, verdict: Verdict, settled: Promise[Nothing, Unit])

  /**
   * What travels the settlement queue: verdicts, and the one message that ends the settler.
   *
   * The settler is stopped through its own queue rather than by interruption, because interrupting a fiber
   * parked on a take can destroy the very item it was being handed — see [[Settler.close]].
   *
   * @tparam A the element settled
   */
  private[v3] enum Settlement[+A]:
    case Filed(pending: Pending[A])
    case Closed

  /**
   * The three queues, plus the wake-up and the pool's death certificate.
   *
   * All three are sized to `concurrency`, and the same argument covers all of them: one token exists per
   * caller, and at any instant it is in exactly one place — the demand queue, the fetcher's hand, the supply
   * queue, a worker's hand, the settlement queue, or the settler's hand. So no queue can be asked to hold
   * more than `concurrency`, and no offer to any of them can block.
   *
   * @param demand capacity offered by callers — one token is the right to claim one element
   * @param supply elements claimed and waiting for the worker whose demand paid for them
   * @param settlement verdicts waiting to be written
   * @param signal raised when the source may have become non-empty
   * @param failure filled when the fetcher or the settler dies — see [[Settler.make]]
   * @tparam E the error the store aborts with
   * @tparam A the element carried
   */
  final private[v3] case class Channel[E, A](
    demand: Queue[Unit],
    supply: Queue[A],
    settlement: Queue[Settlement[A]],
    signal: Signal,
    failure: Promise[E, Nothing],
  )

  private[v3] object Channel:

    /**
     * A channel sized for `concurrency` concurrent callers.
     *
     * @param concurrency the pool's concurrency — see the class doc for why all three queues take it
     * @param signal the wake-up the fetcher parks on
     * @tparam E the error the store aborts with
     * @tparam A the element carried
     * @return the channel; never fails
     */
    def make[E, A](concurrency: Int, signal: Signal): UIO[Channel[E, A]] =
      for
        demand     <- Queue.bounded[Unit](concurrency)
        supply     <- Queue.bounded[A](concurrency)
        settlement <- Queue.bounded[Settlement[A]](concurrency + 1) // +1 so the Closed message always fits
        failure    <- Promise.make[E, Nothing]
      yield Channel(demand, supply, settlement, signal, failure)

  /**
   * Claims from the store on behalf of waiting workers, and never more than they asked for. Unchanged from v2.
   *
   * @param source the leased store to claim from
   * @param channel the demand it serves and the supply it fills
   * @param pollSize the most it will claim in one call
   * @param nackDelay how long an element returned at shutdown stays unavailable
   * @tparam E the error claiming aborts with
   * @tparam A the element claimed
   */
  final private[v3] class Fetcher[E, A] private (
    source: Source[E, A],
    channel: Channel[E, A],
    pollSize: Int,
    nackDelay: Duration,
  ):

    /**
     * Fetch until interrupted. Says nothing about shutdown; [[Fetcher.make]] sequences that.
     *
     * @return never completes successfully; aborts with `E` if the store fails
     */
    private def run: IO[E, Nothing] = step.forever

    /**
     * One cycle: wait for demand, claim at most that much, and place what arrived.
     *
     * Demand is taken before the store is touched, so nothing is claimed without a caller waiting for it.
     * Tokens the store did not fill go straight back, and on an empty poll every token is returned before
     * parking, so a parked fetcher holds nothing.
     *
     * @return noop once the batch is placed; aborts with `E` if the store fails
     */
    private def step: IO[E, Unit] =
      channel.demand
        .takeBetween(1, pollSize)
        .flatMap: tokens =>
          source
            .tryAcquire(upTo = tokens.size)
            .flatMap:
              case Nil    => channel.demand.offerAll(tokens) *> channel.signal.take.unit
              case claims => channel.supply.offerAll(claims) *> channel.demand.offerAll(tokens.drop(claims.size)).unit

    /**
     * Return everything claimed but never handed to a worker.
     *
     * This goes straight to the store rather than through the settlement queue. It could go through it — the
     * settler is still alive at this point in the teardown — but these elements have no worker awaiting a
     * promise, so routing them would mean a [[Pending]] whose `settled` nobody reads. One batched `nack` is
     * simpler and has no ordering dependency on the settler.
     *
     * '''Only correct once [[run]] has stopped'''; [[Fetcher.make]] interrupts first.
     *
     * @return noop once the supply is empty
     */
    private def drain: UIO[Unit] =
      channel.supply.takeAll
        .flatMap(stranded => source.nack(stranded.toList, nackDelay).ignore.unless(stranded.isEmpty))
        .unit
        .uninterruptible

  private[v3] object Fetcher:

    /**
     * A fetcher as an acquired resource: built and started on acquisition, stopped and drained on release.
     *
     * '''The fork must stay in the `tap`, outside the acquire.''' `acquireRelease` runs its acquire
     * uninterruptibly and a fiber inherits its parent's interrupt status, so a fetcher forked there could
     * never be stopped and closing would deadlock. Keeping it here also registers its interrupt *after* the
     * drain, and finalizers run last-registered-first, so the fetcher stops before the drain empties a queue
     * it would otherwise still be filling.
     *
     * @param source the leased store to claim from
     * @param channel the demand it serves, the supply it fills, and the promise it dies into
     * @param pollSize the most it will claim in one call
     * @param nackDelay how long an element returned at shutdown stays unavailable
     * @tparam E the error the store aborts with
     * @tparam A the element claimed
     * @return the running fetcher; never fails — its own failure lands in the channel
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
   * Writes verdicts to the store, as many at a time as have accumulated.
   *
   * `takeBetween(1, batchSize)` is the whole batching policy: block for the first verdict, sweep up whatever
   * else is already there. No timer and no flush interval — a quiet pool writes one element per statement with
   * no added latency, and a busy one batches exactly as hard as it is being pushed, because the batch is
   * whatever finished while the previous write was in flight.
   *
   * @param source the leased store to write to
   * @param channel the settlement queue it drains
   * @param batchSize the most it will write in one statement
   * @param nackDelay how long a failed element stays unavailable
   * @tparam E the error writing aborts with
   * @tparam A the element settled
   */
  final private[v3] class Settler[E, A] private (
    source: Source[E, A],
    channel: Channel[E, A],
    batchSize: Int,
    nackDelay: Duration,
  ):

    /**
     * Settle until told to stop, then return.
     *
     * It '''ends by itself''', on reading [[Settlement.Closed]] — nothing ever interrupts it. That is not
     * fastidiousness: a queue hands an offered item directly to a parked taker, so between the taker being
     * completed and `takeBetween` returning, the item exists only inside an interruptible frame. Interrupt
     * there and it is gone — not in the queue, never written, and invisible until the lease expires. Since
     * `Closed` arrives through the same FIFO as the verdicts, everything filed before it is written first.
     *
     * The write is still uninterruptible, which costs nothing now and keeps the loop honest if a future
     * caller does interrupt it.
     *
     * @return noop once `Closed` is read; aborts with `E` if the store fails
     */
    private def run: IO[E, Unit] = step.repeatWhile(identity).unit

    /**
     * Take a batch, write the verdicts in it, and report whether to keep going.
     *
     * @return true unless the batch contained [[Settlement.Closed]]; aborts with `E` if the store fails
     */
    private def step: IO[E, Boolean] =
      ZIO.uninterruptibleMask: restore =>
        restore(channel.settlement.takeBetween(1, batchSize)).flatMap: batch =>
          val verdicts = batch.collect { case Settlement.Filed(pending) => pending }
          val closed   = batch.exists:
            case Settlement.Closed => true
            case _                   => false
          write(verdicts).unless(verdicts.isEmpty).as(!closed)

    /**
     * Write one batch — at most one `ack` and one `nack` — and release everyone waiting on it.
     *
     * The promises are completed only after the write lands, which is what makes `consume` mean "recorded"
     * rather than "attempted". A failed write completes none of them and simply propagates: the settler dies,
     * its cause reaches the channel, and every waiting worker aborts on the race in [[Worker.consume]]. The
     * batch is told once, through one path, rather than twice through two.
     *
     * @param batch the verdicts to write, never empty
     * @return noop once written and released; aborts with `E` if the store fails
     */
    private def write(batch: Chunk[Pending[A]]): IO[E, Unit] =
      val (done, failed) = batch.partition(_.verdict == Verdict.Done)
      source.ack(done.map(_.element).toList).unless(done.isEmpty)
        *> source.nack(failed.map(_.element).toList, nackDelay).unless(failed.isEmpty)
        *> ZIO.foreachDiscard(batch)(_.settled.succeed(()))

    /**
     * End the settler by asking it to stop, and wait for it to finish.
     *
     * The last step of teardown, and it works entirely through the data path: post [[Settlement.Closed]],
     * then await the fiber. Because the queue is FIFO, every verdict already filed is written before the
     * settler sees the message, and because nothing is interrupted, none can be lost in the hand-off window
     * described on [[run]].
     *
     * The post cannot block: the settlement queue is sized one larger than the pool's concurrency precisely so
     * that this message always fits. Awaiting cannot hang either — a settler that has already died is a
     * completed fiber, so `await` returns at once and its unwritten verdicts fall back on lease expiry.
     *
     * @param fiber the running settler
     * @return noop once the settler has stopped
     */
    private[v3] def close(fiber: Fiber[Nothing, Unit]): UIO[Unit] =
      channel.settlement.offer(Settlement.Closed) *> fiber.await.unit

  private[v3] object Settler:

    /**
     * Start a settler, and arrange for it to finish its work before it is stopped.
     *
     * '''The settler is never interrupted''', unlike every other fiber here. It is a consumer, and a consumer
     * interrupted while a queue is handing it an item loses that item — see [[Settler.run]]. So it is closed
     * through its own queue instead, and the finalizer that does it is registered *after* the fork so that
     * last-registered-first runs it while the fiber is still alive. The scope's own interrupt then lands on an
     * already-completed fiber and does nothing.
     *
     * '''A dead settler must take the pool with it''', which is why its cause is routed into
     * `channel.failure` exactly as the fetcher's is — and it matters more here. A dead fetcher stops the flow,
     * which is loud. A dead settler is silent: work keeps being processed and nothing is ever recorded, so
     * every element redelivers on lease expiry and is done again, forever.
     *
     * @param source the leased store to write to
     * @param channel the settlement queue it drains and the promise it dies into
     * @param batchSize the most it will write in one statement
     * @param nackDelay how long a failed element stays unavailable
     * @tparam E the error the store aborts with
     * @tparam A the element settled
     * @return the running settler; never fails — its own failure lands in the channel
     */
    def make[E, A](
      source: Source[E, A],
      channel: Channel[E, A],
      batchSize: Int,
      nackDelay: Duration,
    ): ZIO[Scope, Nothing, Settler[E, A]] =
      val settler = Settler(source, channel, batchSize, nackDelay)
      for
        fiber <- settler.run.catchAllCause(channel.failure.failCause(_).unit).forkScoped
        _     <- ZIO.addFinalizer(settler.close(fiber))
      yield settler

  /**
   * The pool's public face: one `consume` call takes one element, runs `logic`, and returns once the store has
   * recorded the outcome.
   *
   * '''It holds no [[Source]].''' Claiming belongs to the fetcher and writing to the settler, so a worker is
   * only the work — take, run, file a verdict, wait for it to land. That is the structural point of the third
   * queue; the batching is the payment it collects.
   *
   * @param channel the three queues, the wake-up, and the failure promise
   * @tparam E the error the store aborts with
   * @tparam A the element handled
   */
  final class Worker[E, A] private (channel: Channel[E, A]) extends Consumer[E, A]:

    /**
     * Tell this pool its store may have work now. Never blocks and never fails — a wake-up already pending
     * absorbs this one.
     *
     * '''It wakes this pool only.''' Work created by another instance, or by anything outside this process,
     * cannot reach it, so a periodic tick is what keeps the pool live and this is the shortcut for writers
     * sharing the process (call it after the insert commits). A pool with neither polls once and parks.
     *
     * @return noop
     */
    def wakeUp: UIO[Unit] = channel.signal.raise

    /**
     * Take one element, run `logic` on it, and return once its outcome is durably recorded.
     *
     * Both waits race the pool's failure promise: a fetcher that has died leaves `supply` never to be filled
     * again, and a settler that has died leaves the verdict never to be written. Without the races either one
     * would show up as a hang rather than as an error.
     *
     * @param logic processes one element
     * @tparam E2 the widened error, admitting `logic`'s failures
     * @return noop once the element is settled; aborts with `E2` if `logic` failed, or if the pool died
     */
    override def consume[E2 >: E](logic: A => IO[E2, Unit]): IO[E2, Unit] =
      channel.demand.offer(())
        *> channel.supply.take.raceFirst(channel.failure.await).flatMap(process(_, logic))

    /**
     * Run `logic`, file the verdict, and wait for it to land.
     *
     * Only `logic` and the wait are interruptible. Filing is not: an element whose verdict was dropped on the
     * way out would sit claimed until its lease expired. The wait deliberately *is* interruptible — holding it
     * uninterruptibly would mean a worker being torn down could not proceed until the settler wrote, and the
     * settler is stopped later in the teardown than the workers. The verdict is already on the queue by then,
     * so the flush still records it; only the waiting is abandoned.
     *
     * @param element the claimed element
     * @param logic processes it
     * @tparam E2 the widened error
     * @return noop once settled; aborts with `E2` if `logic` failed
     */
    private def process[E2 >: E](element: A, logic: A => IO[E2, Unit]): IO[E2, Unit] =
      ZIO.uninterruptibleMask: restore =>
        for
          settled <- Promise.make[Nothing, Unit]
          exit    <- restore(logic(element)).exit
          verdict  = if exit.isSuccess then Verdict.Done else Verdict.Failed
          _       <- channel.settlement.offer(Settlement.Filed(Pending(element, verdict, settled)))
          _       <- restore(settled.await.raceFirst(channel.failure.await))
          done    <- exit
        yield done

  object Worker:

    /**
     * Build a pool over `source` and start it, for the life of the scope: the three queues, the settler and
     * fetcher behind them, and the worker in front.
     *
     * '''The settler is registered before the fetcher on purpose.''' Finalizers run last-registered-first, so
     * teardown goes: callers' own fibers interrupted → fetcher stopped → supply drained → settler stopped →
     * settlement flushed. Every stage that can still produce a verdict is stopped before the stage that writes
     * them, and the settler outlives the workers, which is what lets a worker interrupted mid-wait still have
     * its verdict recorded. Swap these two lines and verdicts filed during teardown are lost until their
     * leases expire.
     *
     * The wake-up is built here and handed back as [[Worker.wakeUp]], which is why this returns the worker
     * rather than a bare `Consumer`. `Consumer`'s combinators return a plain `Consumer`, so lift `wakeUp` out
     * first if you intend to compose.
     *
     * @param source the leased store to claim from and write to
     * @param concurrency how many fibers will call `consume` at once — the size of all three queues, and the
     *                    ceiling on outstanding leases
     * @param pollSize the most the fetcher will claim in one query
     * @param nackDelay how long an element returned unprocessed stays unavailable
     * @tparam E the error the store aborts with
     * @tparam A the element handled
     * @return the worker, with its fetcher and settler already running; never fails
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
        _       <- Settler.make(source, channel, concurrency, nackDelay)
        _       <- Fetcher.make(source, channel, pollSize, nackDelay)
      yield Worker(channel)
