---
title: "Workflow executor — a ZIO implementation sketch"
type: research
status: draft
updated: 2026-07-14
tags: [workflow, durable-execution, zio, fibers, postgres, sketch]
---

# Workflow executor — a ZIO implementation sketch

Code companion to [`workflow-executor.md`](./workflow-executor.md): a syntax-plausible ZIO 2.x sketch of the
executor that answers the runtime questions the design note left open — **where the fibers live**, whether
they're **fire-and-forget**, **what finalizers** run, and **how errors are handled**. It is a *sketch*: the
`WorkflowStore` port is abstract (the Postgres adapter sits behind it), a couple of helpers are elided, and it
hasn't been compiled — but the control flow, fiber structure, and error policy are real.

---

## The four questions, answered

- **Where do the fibers live?** In a **fixed pool** of worker fibers, plus one heartbeat fiber, all
  `forkScoped` into the executor's scope (which is the run-mode's lifetime in `Main`). They are *not*
  daemons — closing the scope interrupts them, so shutdown is clean.
- **Fired and forgot?** No. There's **no fork-per-instance** to orphan. A worker claims one instance, steps it
  to completion, then claims the next — so a fixed pool of `maxConcurrency` workers *is* the concurrency cap
  (no semaphore juggling), and there's nothing unsupervised to leak. In-flight external calls = live workers =
  the bulkhead; the backlog waits as rows.
- **Finalizers?** **One** — `ensuring(releaseLease)`, so any exit (done, failed, interrupted) hands the key
  lease back immediately for fast failover (correctness still holds via TTL expiry if it never runs).
  **State is *not* persisted in a finalizer.** The step loop persists explicitly at each checkpoint; persisting
  on interruption would be a lie (the in-flight side effect may not have landed), and the last durable
  checkpoint + idempotent resume already cover a mid-step stop.
- **Errors?** Classify the step's `ApplicationError` by marker: a `TransientError` with retries left →
  **durable reschedule with backoff** (`available_at`; the fiber is freed, the work waits as a row); anything
  else → **mark failed**. Defects are logged and failed. Store faults die and are reclaimed via lease expiry.

---

## Ports and types

```scala
package homelab.flow.executor // sketch — package tbd


import homelab.common.error.ApplicationError
import homelab.common.flow.Workflow
import zio.*

import java.time.Instant


type NodeId       = String
type WorkflowId   = String
type PartitionKey = String
type LeaseToken   = Long

/** A leased, runnable instance handed to a worker: identity, the key it serialises on, its type, the
 *  serialized state, the fencing token proving current ownership, and the durable attempt count. */
final case class Claimed(
  id:           WorkflowId,
  key:          PartitionKey,
  workflowType: String,
  state:        Array[Byte],
  token:        LeaseToken,
  attempt:      Int,
)

/** The durable store (Postgres behind it). Every method is a short transaction; none is held across a step.
 *  Failures are `ApplicationError.AdapterError` (like the NATS adapter's `NatsError`). */
trait WorkflowStore:

  /** Claim the next runnable instance whose key is free, leasing that key to `node` for `ttl` — the
   *  `SELECT … FOR UPDATE SKIP LOCKED` + lease upsert. `None` if there's no runnable, un-leased work. */
  def claim(node: NodeId, ttl: Duration): IO[ApplicationError, Option[Claimed]]

  /** Persist a checkpoint and renew the lease in one fenced write. `false` ⇒ the lease was stolen (token
   *  stale) ⇒ the caller has lost the key and must stop. */
  def persist(work: Claimed, state: Array[Byte]): IO[ApplicationError, Boolean]

  /** Mark the instance done with its output (fenced). */
  def complete(work: Claimed, output: Array[Byte]): IO[ApplicationError, Unit]

  /** Reschedule to run again no earlier than `at`, bumping the attempt count — a durable retry. */
  def reschedule(work: Claimed, at: Instant): IO[ApplicationError, Unit]

  /** Mark the instance permanently failed. */
  def fail(work: Claimed, reason: String): IO[ApplicationError, Unit]

  /** Renew every lease this node owns, in one batch UPDATE (the heartbeat). */
  def renewLeases(node: NodeId, ttl: Duration): IO[ApplicationError, Unit]

  /** Release a key lease (fast hand-off; TTL expiry is the correctness backstop). */
  def releaseLease(work: Claimed): IO[ApplicationError, Unit]

  /** The soonest `available_at` among pending work, for a precise idle sleep. */
  def nextDeadline: IO[ApplicationError, Option[Instant]]

  /** Block until a `workflow_ready` NOTIFY arrives (LISTEN). */
  def awaitReady: IO[ApplicationError, Unit]

/** Type-erased driver for one workflow type: decode `S`, run one `Workflow.next`, re-encode. The registry
 *  builds one per `workflowType` from a typed [[Workflow.Simple]] + its `Serde`s, hiding `S`/`O`. */
trait WorkflowRunner:
  def step(state: Array[Byte]): IO[ApplicationError, WorkflowRunner.Step]

object WorkflowRunner:
  enum Step:
    case Continue(state: Array[Byte])
    case Done(output: Array[Byte])
```

---

## The executor

```scala
final class Executor(
  store:   WorkflowStore,
  runners: String => WorkflowRunner, // the registry: workflowType → runner
  node:    NodeId,
  config:  Executor.Config,
):

  /** Run until the enclosing scope closes: a fixed pool of worker fibers + a heartbeat, all `forkScoped` so
   *  shutdown interrupts them cleanly. `maxConcurrency` fibers ⇒ at most that many in-flight external calls —
   *  the bulkhead, sized to the provider. The backlog waits as rows, not fibers. */
  def run: ZIO[Scope, Nothing, Unit] =
    for
      _ <- heartbeat.forkScoped
      _ <- ZIO.foreachDiscard(1 to config.maxConcurrency)(_ => worker.forkScoped)
      _ <- ZIO.never
    yield ()

  /** One worker: claim a runnable instance and step it, or sleep until woken. A pool of these *is* the cap —
   *  no semaphore, nothing forked per instance, so nothing to orphan. */
  private def worker: UIO[Unit] =
    claim.flatMap {
      case Some(work) => process(work)
      case None       => idle
    }.forever

  private def claim: UIO[Option[Claimed]] =
    store.claim(node, config.leaseTtl).orDie // a real impl retries transient store faults; here it dies → reclaimed

  /** Step an instance to completion (or a retry/failure), always releasing its key lease on exit — the one
   *  finalizer. State is deliberately NOT persisted on interruption: the last checkpoint + idempotent resume
   *  cover a mid-step stop, and writing a half-done step (whose side effect may not have landed) would lie. */
  private def process(work: Claimed): UIO[Unit] =
    stepLoop(work, work.state)
      .catchAll(onError(work, _))
      .catchAllDefect(onDefect(work, _))
      .ensuring(store.releaseLease(work).ignore)

  /** The checkpoint loop: run one step, persist (fenced), advance — or finish. `persist` returning `false`
   *  means the lease was stolen (we ran too slow, another node reclaimed the key); we stop and let the new
   *  owner resume from our last checkpoint. */
  private def stepLoop(work: Claimed, state: Array[Byte]): IO[ApplicationError, Unit] =
    runners(work.workflowType).step(state).flatMap {
      case WorkflowRunner.Step.Continue(next) =>
        store.persist(work, next).flatMap {
          case true  => stepLoop(work, next)
          case false => ZIO.unit // lease lost — stop; the new owner continues
        }
      case WorkflowRunner.Step.Done(output) =>
        store.complete(work, output)
    }

  /** Error policy: a transient error with retries left ⇒ durable reschedule with backoff (fiber freed, work
   *  waits as a row); anything else ⇒ mark failed. Store cleanups are best-effort (`ignore`). */
  private def onError(work: Claimed, error: ApplicationError): UIO[Unit] =
    error match
      case _: ApplicationError.TransientError if work.attempt < config.maxAttempts =>
        Clock.instant.flatMap(now => store.reschedule(work, now.plus(backoff(work.attempt)))).ignore
      case _ =>
        store.fail(work, error.message).ignore

  private def onDefect(work: Claimed, defect: Throwable): UIO[Unit] =
    ZIO.logErrorCause("workflow step defect", Cause.die(defect)) *> store.fail(work, s"defect: $defect").ignore

  /** Renew all this node's leases on a cadence comfortably tighter than the TTL. */
  private def heartbeat: UIO[Unit] =
    store.renewLeases(node, config.leaseTtl).ignore.repeat(Schedule.spaced(config.heartbeat)).unit

  /** Idle: block on the LISTEN nudge, but never past the next scheduled `available_at` — the precise wake. */
  private def idle: UIO[Unit] =
    store.nextDeadline.orDie.flatMap {
      case Some(at) => Clock.instant.flatMap(now => store.awaitReady.race(ZIO.sleep(java.time.Duration.between(now, at))))
      case None     => store.awaitReady // nothing pending → sleep on LISTEN until an enqueue wakes us
    }.ignore

  /** Exponential backoff for retry N. */
  private def backoff(attempt: Int): Duration =
    config.baseBackoff * math.pow(2, attempt.toDouble)


object Executor:

  /**
   * @param maxConcurrency max in-flight external calls per node — the bulkhead, sized to the provider
   * @param leaseTtl       how long a key lease survives without renewal
   * @param heartbeat      lease-renewal cadence (must be `< leaseTtl`)
   * @param maxAttempts    durable retry ceiling before an instance is failed
   * @param baseBackoff    first retry delay (doubled per attempt)
   */
  final case class Config(
    maxConcurrency: Int      = 200,
    leaseTtl:       Duration = 30.seconds,
    heartbeat:      Duration = 10.seconds,
    maxAttempts:    Int      = 5,
    baseBackoff:    Duration = 1.second,
  )
```

Wiring in `Main` is then a run-mode: `ZIO.scoped(executor.run)` under the process's lifetime, so `Ctrl-C` /
SIGTERM closes the scope → workers + heartbeat interrupt → each in-flight worker's `ensuring` releases its
lease → another node reclaims instantly (no TTL wait).

---

## Using it — placing work and launching the executor

Two remaining ports: a **submit** side for whoever places work, and the **runner factory** that erases a
typed `Workflow` to the byte-level `WorkflowRunner` the executor drives (the one bit the sketch elided).

```scala
/** The producer side — place a new workflow. Encodes `initial`, inserts a pending instance, fires the
 *  `workflow_ready` NOTIFY, returns the id. Fire-and-forget: the workflow then runs to completion on whatever
 *  node owns its key. (`Serde` = the shared codec seam, zio-json-derived in practice.) */
trait WorkflowClient:
  def submit[S: Serde](workflowType: String, key: PartitionKey, initial: S): IO[ApplicationError, WorkflowId]

// on WorkflowRunner's companion — decode S, run one `next`, re-encode:
def of[S, O](workflow: Workflow.Simple[S, O])(using in: Serde[S], out: Serde[O]): WorkflowRunner =
  bytes =>
    ZIO.fromEither(in.decode(bytes)).mapError(WorkflowError.CorruptState(_)).flatMap: state =>
      workflow.next(state).map:
        case Loop.Next.Continue(next)    => WorkflowRunner.Step.Continue(in.encode(next))
        case Loop.Next.Done((_, output)) => WorkflowRunner.Step.Done(out.encode(output))

enum WorkflowError extends ApplicationError:
  case CorruptState(reason: String) extends WorkflowError, ApplicationError.DecodingError
  override def message: String = this match
    case CorruptState(reason) => s"corrupt stored workflow state: $reason"
```

**1 — Define a workflow** (plain domain code; the state `S` is the checkpoint, `O` the terminal output):

```scala
enum OrderState:
  case Received(orderId: String, cart: Cart)
  case Reserved(orderId: String, reservationId: String)
  case Charged(orderId: String, chargeId: String)

final case class OrderReceipt(orderId: String, chargeId: String)

object OrderFulfilment extends Workflow.Simple[OrderState, OrderReceipt]:
  def next =
    case OrderState.Received(id, cart) =>
      reserveStock(id, cart).map(res => Loop.continue(OrderState.Reserved(id, res)))  // external call, idempotency-keyed
    case OrderState.Reserved(id, res) =>
      charge(id, res).map(chg => Loop.continue(OrderState.Charged(id, chg)))          // external call, idempotency-keyed
    case charged @ OrderState.Charged(id, chg) =>
      Loop.succeed((charged, OrderReceipt(id, chg)))                                   // done → (finalState, output)
  // reserveStock / charge: IO[ApplicationError, String] — the OpenRouter/API calls (elided)
```

**2 — Register it** (needs a `Serde` for the state and the output):

```scala
given Serde[OrderState]   = Serde.derived
given Serde[OrderReceipt] = Serde.derived

val registry: Map[String, WorkflowRunner] =
  Map("order-fulfilment" -> WorkflowRunner.of(OrderFulfilment))
```

**3 — A client places work** (e.g. inside an HTTP route). It supplies only the *initial state* and the *key*;
the executor runs the rest:

```scala
def placeOrder(client: WorkflowClient)(orderId: String, cart: Cart): IO[ApplicationError, WorkflowId] =
  client.submit("order-fulfilment", key = orderId, initial = OrderState.Received(orderId, cart))
  // key = orderId ⇒ all steps for one order run serially on one node; different orders run in parallel.
  // Returns the id immediately (a 202-Accepted shape); the workflow proceeds asynchronously.
```

**4 — Launch the executor as a fiber**, in `Main`, alongside the submit API:

```scala
object Main extends ZIOAppDefault:
  def run =
    ZIO.scoped:
      for
        store   <- PostgresWorkflowStore.make    // scoped: pooled connections + the LISTEN connection
        client  <- PostgresWorkflowClient.make
        executor = Executor(store, registry, node = System.getenv("HOSTNAME"), Executor.Config())
        _       <- executor.run.forkScoped        // ← the worker pool + heartbeat, as a background fiber
        _       <- submitApi(client)              // the HTTP submit surface, in the foreground (blocks)
      yield ()
```

`executor.run.forkScoped` is the launch: it forks the pool into the app scope and returns, so the HTTP API
runs concurrently. On SIGTERM, `ZIOAppDefault` interrupts `run` → the scope closes → the executor fiber (and
its workers/heartbeat) are interrupted → each `ensuring` releases its lease → a peer reclaims instantly. A
**pure worker node** (no API) is simpler still — just `_ <- executor.run` with no fork; its internal
`ZIO.never` holds the app open until interrupted.

---

## Variant — in-memory serial (partition-owned)

The pool executor above claims per **instance** and leases per **key** (fork #2's DB-serial branch). The
alternative — lighter on the DB, which was the original worry — is **partition-owned with in-memory serial**:
a node leases whole *partitions* (coarse), and inside a partition it serialises keys with an in-memory
running-set rather than a per-key DB lease. The DB holds only pending/done state (the queue + recovery log);
the per-key serial hot-path never touches it. The client side (`submit`) is unchanged; only the node's run
loop differs.

Same `WorkflowStore` / `WorkflowRunner` / `Claimed` (its per-key `token` is unused here — the *partition*
token fences); add a partition id and swap the core:

```scala
type PartitionId = Int

/** One dispatcher per owned partition. Serial-per-key is the `running` set — authoritative because this node
 *  is the partition's *single* owner (the partition lease). `slots` is the node-wide concurrency cap. Every
 *  write is fenced by the partition-lease `token`: if the partition is stolen, in-flight persists fail → stop. */
final class PartitionDispatcher(
  partition: PartitionId,
  token:     LeaseToken,
  store:     WorkflowStore,
  runners:   String => WorkflowRunner,
  slots:     Semaphore,
  running:   Ref[Set[PartitionKey]],
):

  /** Poll the partition and dispatch runnable keys, forever — until interrupted (the node lost the partition). */
  def run: ZIO[Scope, Nothing, Unit] =
    round.forever

  private def round: ZIO[Scope, Nothing, Unit] =
    for
      free       <- slots.available.map(_.toInt)
      candidates <- if free <= 0 then ZIO.succeed(List.empty[Claimed])
                    else store.pending(partition, limit = free * 2).orDie // earliest pending per key (DISTINCT ON key)
      fired      <- ZIO.foreach(candidates)(dispatch).map(_.count(identity))
      _          <- ZIO.when(fired == 0)(idle)                             // nothing dispatchable → wait
    yield ()

  /** Claim a key into the running-set (the serial gate) and fork its worker, which holds a slot while it steps
   *  and frees the key on exit. `false` ⇒ the key was already running → skip this instance; a later poll (once
   *  the key frees) picks up its next one in `seq` order. */
  private def dispatch(work: Claimed): ZIO[Scope, Nothing, Boolean] =
    running.modify(set => if set.contains(work.key) then (false, set) else (true, set + work.key)).flatMap {
      case false => ZIO.succeed(false)
      case true  =>
        slots
          .withPermit(worker(work))
          .ensuring(running.update(_ - work.key)) // release the key when the instance finishes
          .forkScoped
          .as(true)
    }

  /** Same `stepLoop` / `onError` / `onDefect` as the pool worker — but `store.persist(work, token, …)` is
   *  fenced by the *partition* token: a `false` return means the partition was reclaimed, so stop. */
  private def worker(work: Claimed): UIO[Unit] = ??? // as in the pool sketch, fenced on `token`
```

The node owns a shifting set of partitions and runs a dispatcher per owned one:

```scala
def runNode: ZIO[Scope, Nothing, Unit] =
  for
    running <- Ref.make(Set.empty[PartitionKey])        // shared across this node's dispatchers
    slots   <- Semaphore.make(config.maxConcurrency.toLong)
    _       <- renewPartitionLeases.repeat(Schedule.spaced(config.heartbeat)).forkScoped
    _       <- rebalance(running, slots).forever          // claim/lose partitions → start/stop dispatchers
  yield ()
```

`rebalance` claims free partitions (`partition_lease`, `SKIP LOCKED` — coarse, few rows), forks a
`PartitionDispatcher(...).run.forkScoped` per newly owned one, and interrupts the dispatcher of any partition
it loses (lease stolen on a peer's takeover). That partition-membership churn is the **added complexity of
this variant** — the pool executor has none of it.

Two things fall out cleanly:

- **Recovery needs no reset.** Serial lives in memory and the DB marks nothing "running," so a crashed owner
  leaves only `pending` rows. A peer takes the partition (lease TTL), starts a fresh dispatcher (empty
  `running`), and re-dispatches those instances — the same idempotent + fenced re-run that already covers a
  mid-step crash. Nothing to clean up.
- **DB load is minimal.** No per-key lease, no per-instance `SKIP LOCKED` claim, no `running`-status writes —
  just the coarse partition-lease renew and the unavoidable per-step persists. The serial hot-path is a `Ref`
  update.

The trade, restated: a partition moves as a unit (hot-partition risk, chunkier failover), and you carry the
in-memory dispatcher + partition rebalancing. In exchange the database does the least work of any option —
which is why it's the one to pick when DB load is the binding constraint.

---

## What the sketch elides (and would tighten for real)

- **Checkpoint granularity.** `stepLoop` persists after *every* `next`. The `workflow-executor.md` §2
  optimisation — checkpoint only around IO, batching cheap in-memory steps — moves inside `WorkflowRunner`
  (let `step` run several `next`s and return one checkpoint), not the executor.
- **Store-fault handling.** `claim`/`nextDeadline` `.orDie` here; a real impl retries transient
  `AdapterError`s (with backoff) rather than dying, reserving death for truly broken states.
- **Key draining.** A worker releases the lease after one instance; owning a key and draining *all* its ready
  instances before releasing would cut lease churn (at the cost of a worker holding a hot key longer). A
  fairness knob.
- **Rate limiting.** The bulkhead (`maxConcurrency`) is a concurrency cap, not an RPM/TPM limiter; a provider
  token-bucket and 429-`Retry-After` → `reschedule(available_at)` (the global valve from `workflow-executor.md`
  §7/§8) would layer on top of `onError`.
- **The Postgres adapter and codec.** `PostgresWorkflowStore` / `PostgresWorkflowClient` (the SQL from
  `workflow-executor.md` §§2–4 behind the port methods) and `Serde` derivation are assumed — everything above
  the storage line is shown; the SQL itself is not.

---

## Related

- [`workflow-executor.md`](./workflow-executor.md) — the Postgres design this drives (state store, leases,
  scheduling, the strains).
- [`workflow-executor-store-projection.md`](./workflow-executor-store-projection.md) — an alternative
  decomposition: treat `WorkflowStore` as the sole port and project it onto the messaging topology
  (`Consumer`/`Pipe`/`Processor`) rather than building a bespoke pool.
- `homelab.common.flow.Workflow` — the stepper the `WorkflowRunner` wraps.
- [`error-modeling.md`](./error-modeling.md) — the `ApplicationError` markers (`TransientError`, …) the error
  policy classifies on.
