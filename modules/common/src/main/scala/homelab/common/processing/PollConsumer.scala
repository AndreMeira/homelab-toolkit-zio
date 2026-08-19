package homelab.common.processing


import homelab.common.messaging.Consumer
import zio.*


/**
 * A `Consumer` over a store that will not tell you when it has work: one `consume` call claims one element,
 * runs `logic` on it, and returns once the store has durably recorded the outcome.
 *
 * Build it with [[PollConsumer.make]], which is where the shape and the trade-offs are documented. Call
 * `consume` from up to `concurrency` fibers at once, and give [[wakeUp]] to whatever knows work has arrived.
 *
 * '''It holds no [[PollConsumer.Source]].''' Claiming belongs to the fetcher behind it and writing to the
 * settler, so a caller is only the work — take, run, file a verdict, wait for it to land. Both directions of
 * store traffic are therefore batched without any caller having to know.
 *
 * Because `consume` returns only once the outcome is recorded, '''outstanding leases never exceed
 * `concurrency`''': nobody can claim a second element while their first is recorded only in memory. Under
 * load that costs nothing — callers finishing together are written by one statement and released together,
 * which is precisely where the batching comes from.
 *
 * @param channel the three queues, the wake-up, and the failure promise
 * @tparam E the error the store aborts with
 * @tparam A the element handled
 */
final class PollConsumer[E, A] private (channel: PollConsumer.Channel[E, A]) extends Consumer[E, A]:

  import PollConsumer.{ Pending, Settlement, Verdict }


  /**
   * Tell this consumer its store may have work now. Never blocks and never fails — a wake-up already pending
   * absorbs this one.
   *
   * '''It wakes this consumer only.''' Rows created by another instance, or by anything outside this process,
   * cannot reach it — so a periodic tick is what keeps the consumer correct, and this is the latency shortcut
   * for writers sharing the process (call it after the insert commits, not before). With neither, the
   * consumer polls once, finds nothing, and parks forever.
   *
   * @return noop
   */
  def wakeUp: UIO[Unit] = channel.signal.raise

  /**
   * Take one element, run `logic` on it, and return once its outcome is durably recorded.
   *
   * Both waits race the failure promise: a fetcher that has died leaves `supply` never to be filled
   * again, and a settler that has died leaves the verdict never to be written. Without the races either one
   * would show up as a hang rather than as an error.
   *
   * @param logic processes one element
   * @tparam E2 the widened error, admitting `logic`'s failures
   * @return noop once the element is settled; aborts with `E2` if `logic` failed, or if the fetcher or settler died
   */
  override def consume[E2 >: E](logic: A => IO[E2, Unit]): IO[E2, Unit] =
    channel.demand.offer(())
      *> channel.supply.take.raceFirst(channel.failure.await).flatMap(process(_, logic))

  /**
   * Run `logic`, file the verdict, and wait for it to land.
   *
   * Only `logic` and the wait are interruptible. Filing is not: an element whose verdict was dropped on the
   * way out would sit claimed until its lease expired. The wait deliberately *is* interruptible — holding it
   * uninterruptibly would mean a caller being torn down could not proceed until the settler wrote, and the
   * settler is closed later in the teardown than the callers. The verdict is already on the queue by then, so
   * it is still written; only the waiting is abandoned.
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

/**
 * How a [[PollConsumer]] is built, and what a store has to provide to be consumed from.
 *
 * '''When to reach for this.''' A table used as a work queue is the motivating case: rows claimed with
 * `SELECT … FOR UPDATE SKIP LOCKED LIMIT n`, a `claimed_until` column carrying the lease, a `visible_at`
 * column carrying the retry delay. Nothing calls you when a row appears, so somebody has to ask — and asking
 * well is the whole problem this solves. [[Source]] is generic (anything that claims, acks and nacks without
 * blocking fits), but the design is shaped by that case and priced for it.
 *
 * '''When not to.''' If the substrate ships a consumer client that pushes or long-polls — NATS, Kafka, SQS —
 * use it. Those already solve delivery, flow control and liveness better than a polling loop can, and
 * wrapping them here would buy a second layer of queues and nothing else.
 *
 * '''Three queues, three owners.''' Every touch of the store belongs to exactly one fiber, and the consumer
 * itself touches it not at all:
 *
 *   - '''fetcher''' — takes demand, claims, fills supply. Owns the read, batched by `LIMIT`.
 *   - '''caller''' — takes supply, runs `logic`, files a verdict. Owns nothing but the work.
 *   - '''settler''' — takes verdicts, writes them. Owns the write, batched by `WHERE id = ANY`.
 *
 * A caller ready to run something offers a *demand* token, and the fetcher claims only as much as it holds
 * demand for. Nothing is ever claimed that there is no capacity to run, so no row sits marked claimed while
 * its handler queues: capacity is a token held rather than a number computed.
 *
 * '''Teardown is ordered, and the order is load-bearing.''' [[make]] registers the settler before the fetcher,
 * so finalizers (last-registered-first) run: callers interrupted → fetcher stopped → supply drained → settler
 * closed. Every stage that can still produce a verdict stops before the stage that writes them, and the
 * settler outliving the callers is what lets one interrupted mid-wait still have its verdict recorded. See
 * `ScopeOrderingSpec` for the finalizer mechanics this rests on, and [[Settler.close]] for why the settler is
 * the one fiber here that is never interrupted.
 */
object PollConsumer:

  /**
   * A store that hands out leases: claiming never blocks, and every claim is settled exactly once.
   *
   * Over a table the three methods are one statement each — claim with
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
     * '''Must return at most `upTo`''', and '''must be cheap when empty''' — on an idle consumer this is the query
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
   * Internal; the consumer builds its own and exposes the raising end as [[PollConsumer.wakeUp]].
   */
  opaque private[PollConsumer] type Signal = Signal.Type

  private[PollConsumer] object Signal:
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
  private[PollConsumer] enum Verdict:
    case Done, Failed

  /**
   * One element's verdict, in transit to the settler, with the promise the worker is waiting on.
   *
   * The promise is what makes settlement synchronous from the caller's side without making it unbatched: the
   * settler completes a whole batch's worth at once.
   *
   * It '''only ever succeeds'''. A write that fails kills the settler, and its cause lands in the channel's
   * failure promise, which [[PollConsumer.consume]] is already racing — so failing this one too would tell the
   * worker something it is about to be told anyway. Keeping it `Promise[Nothing, Unit]` is what lets this type
   * and [[Settlement]] be covariant, which in turn is what lets `Closed` be a parameterless case.
   *
   * @param element the claimed element
   * @param verdict what the worker decided
   * @param settled completed once the store has recorded it
   * @tparam A the element settled
   */
  final private[PollConsumer] case class Pending[+A](element: A, verdict: Verdict, settled: Promise[Nothing, Unit])

  /**
   * What travels the settlement queue: verdicts, and the one message that ends the settler.
   *
   * The settler is stopped through its own queue rather than by interruption, because interrupting a fiber
   * parked on a take can destroy the very item it was being handed — see [[Settler.close]].
   *
   * @tparam A the element settled
   */
  private[PollConsumer] enum Settlement[+A]:
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
  final private[PollConsumer] case class Channel[E, A](
    demand: Queue[Unit],
    supply: Queue[A],
    settlement: Queue[Settlement[A]],
    signal: Signal,
    failure: Promise[E, Nothing],
  )

  private[PollConsumer] object Channel:

    /**
     * A channel sized for `concurrency` concurrent callers.
     *
     * @param concurrency the caller concurrency — see the class doc for why all three queues take it
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
   * Claims from the store on behalf of waiting workers, and never more than they asked for. 
   *
   * @param source the leased store to claim from
   * @param channel the demand it serves and the supply it fills
   * @param pollSize the most it will claim in one call
   * @param nackDelay how long an element returned at shutdown stays unavailable
   * @tparam E the error claiming aborts with
   * @tparam A the element claimed
   */
  final private[PollConsumer] class Fetcher[E, A] private (
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

  private[PollConsumer] object Fetcher:

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
   * else is already there. No timer and no flush interval — a quiet consumer writes one element per statement with
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
  final private[PollConsumer] class Settler[E, A] private (
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
     * its cause reaches the channel, and every waiting worker aborts on the race in [[PollConsumer.consume]]. The
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
     * The post cannot block: the settlement queue is sized one larger than the caller concurrency precisely so
     * that this message always fits. Awaiting cannot hang either — a settler that has already died is a
     * completed fiber, so `await` returns at once and its unwritten verdicts fall back on lease expiry.
     *
     * @param fiber the running settler
     * @return noop once the settler has stopped
     */
    private[PollConsumer] def close(fiber: Fiber[Nothing, Unit]): UIO[Unit] =
      channel.settlement.offer(Settlement.Closed) *> fiber.await.unit

  private[PollConsumer] object Settler:

    /**
     * Start a settler, and arrange for it to finish its work before it is stopped.
     *
     * '''The settler is never interrupted''', unlike every other fiber here. It is a consumer, and a consumer
     * interrupted while a queue is handing it an item loses that item — see [[Settler.run]]. So it is closed
     * through its own queue instead, and the finalizer that does it is registered *after* the fork so that
     * last-registered-first runs it while the fiber is still alive. The scope's own interrupt then lands on an
     * already-completed fiber and does nothing.
     *
     * '''A dead settler must take the consumer down with it''', which is why its cause is routed into
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
   * Build a consumer over `source` and start it, for the life of the scope: the three queues, the settler and
   * fetcher behind them, and the worker in front.
   *
   * '''The settler is registered before the fetcher on purpose.''' Finalizers run last-registered-first, so
   * teardown goes: callers' own fibers interrupted → fetcher stopped → supply drained → settler stopped →
   * settlement flushed. Every stage that can still produce a verdict is stopped before the stage that writes
   * them, and the settler outlives the workers, which is what lets a worker interrupted mid-wait still have
   * its verdict recorded. Swap these two lines and verdicts filed during teardown are lost until their
   * leases expire.
   *
   * The wake-up is built here and handed back as [[PollConsumer.wakeUp]], which is why this returns the worker
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
  ): ZIO[Scope, Nothing, PollConsumer[E, A]] =
    for
      signal  <- Signal.make
      channel <- Channel.make[E, A](concurrency, signal)
      _       <- Settler.make(source, channel, concurrency, nackDelay)
      _       <- Fetcher.make(source, channel, pollSize, nackDelay)
    yield PollConsumer(channel)
