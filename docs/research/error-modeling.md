---
title: "Error Modeling for Hexagonal Services (Scala 3 + Kyo)"
type: research
status: current
updated: 2026-07-07
tags: [error-modeling, hexagonal, kyo, scala, domain-errors]
---

# Error Modeling for Hexagonal Services (Scala 3 + Kyo)

> **Kyo provenance, ZIO reframe pending.** This note predates the move to ZIO: the *thinking* is current and
> is what `homelab-common`'s `ApplicationError` layer implements, but every code sample below is Kyo
> (`Abort[E]`, checked against `1.0.0-RC4`). Read the mechanics as illustration and the argument as live.
> Re-typing it to `ZIO[R, E, A]` is a promotion job that has not been done.

A design note for the homelab's application services (registration-service and
its siblings). It captures *how to model errors* in a DDD + hexagonal codebase
where errors are **typed values** in a `Kyo` effect (`Abort[E]`), not exceptions —
and *why* the design ends up where it does. The journey runs from "a port can't
list its errors, so do we give up and type everything as the root?" to a concrete,
compile-verified port pattern. Read top to bottom; each section answers the
objection the previous one raises.

All Kyo mechanics below were checked against kyo `1.0.0-RC4` on Scala `3.8.2`.

---

## The why, in one paragraph

Typed errors are only worth their weight when the type *says something the caller
acts on*. The failure mode to avoid is `Abort[ApplicationError]` everywhere —
`ApplicationError` is the root of the hierarchy, so that's the typed-error
equivalent of `throws Exception`: technically typed, practically useless. But the
opposite extreme — every adapter listing every concrete failure on every port — is
impossible, because a *port* is an abstraction over adapters that fail in different,
unknowable ways. The resolution is to stop typing errors *per service* and type
them **per operation**, to keep the **global** layer as nothing but behavioral
**markers**, and to be deliberate about which failures are *domain outcomes*
(typed, named, acted on) versus *infrastructure trouble* (typed but widened, or —
rarely — a defect).

---

## 1. Two kinds of failure, and only one is "interesting"

- **Domain errors** — *expected* outcomes that are part of the contract:
  `AlreadyExists`, `NotFound`, `Unauthorised`, validation failures. The caller
  *branches* on these. High signal. → **typed and named**.
- **Infrastructure failures** — the DB connection dropped, the k8s API is
  unreachable, a stored secret's JSON is corrupt. The caller can't do anything
  domain-meaningful with these except fail the request. Low signal, and
  *adapter-specific*.

The premise "a port can't enumerate its errors" is **only true of the infra
failures**, and those mostly shouldn't drive domain branching anyway. The domain
outcomes a port produces *are* enumerable and **adapter-independent**: `save`
either succeeds or hits a uniqueness conflict whether it's Postgres or in-memory.
Adapters differ only in *how* they fail at the infra level — which is exactly the
part you don't want callers branching on.

## 2. The global layer is just markers — on two axes

The only thing that is genuinely global is the **marker hierarchy** on
`ApplicationError`. The trap is conflating **two orthogonal axes**:

- **Source** — *where* it came from. The domain-facing one is **`AdapterError`**:
  "the thing behind my port (its adapter) failed." It's the single **opaque
  umbrella** the domain carries in signatures without branching on.
  `VendorError`/`PersistenceError`/`NetworkError` are refinements of it.
- **Recoverability** — *what to do about it*: `TransientError` ("retry might help")
  vs permanent (`InconsistentState` — a bug / corruption).

```
ApplicationError                 // root — never a typed-error channel on purpose
 ├─ DomainError                  // expected business outcomes (the caller branches)
 │   ├─ ConflictError            // 409
 │   ├─ UnauthorisedError        // 401
 │   ├─ NotFoundError            // 404
 │   ├─ ValidationError          // 422
 │   └─ InconsistentState        // 500 — internal state is wrong (bug / corruption)
 ├─ AdapterError                 // SOURCE: a port's adapter failed (opaque umbrella) → 5xx
 │   ├─ VendorError              //   a named third-party / upstream → 502
 │   ├─ PersistenceError         //   our own datastore
 │   └─ NetworkError             //   transport
 └─ TransientError               // RECOVERABILITY (orthogonal): retry might help → 503
```

The two axes **coexist** — a concrete error picks one source marker and, when it
matters, a recoverability marker:

```scala
case class Unreachable(cause: Throwable) extends AdapterError, TransientError      // → 503
case class Corrupted(reason: String)     extends AdapterError, InconsistentState   // → 500
```

> Naming caveat: **`AdapterError`, not `VendorError`, is the umbrella.** "Vendor"
> means a *third-party supplier* — wrong for your own in-cluster Postgres or the
> k8s API, which are infra but not vendors. Keep `VendorError` for genuine
> upstreams (→ 502); anything-an-adapter-can-throw is `AdapterError`. The domain
> depends on *ports*, so "an adapter failed" is the honest view from the boundary.

Markers are the **cross-cutting categories** that drive behavior — retry policy
and HTTP status. Critically, **the HTTP layer maps on markers, not on concrete
types**, and prefers the *recoverability* axis where present (order matters):

```scala
def from(error: ApplicationError): HttpError = error match
  case e: ApplicationError.UnauthorisedError => …  // 401
  case e: ApplicationError.NotFoundError     => …  // 404
  case e: ValidationError                    => …  // 422 / 409
  case e: ApplicationError.TransientError    => …  // 503 (retry might help)
  case e: ApplicationError.VendorError       => …  // 502 (named upstream)
  case e: ApplicationError.AdapterError      => …  // 500 (opaque infra default)
  case e                                     => …  // 500
```

The payoff is **open/closed**: a brand-new concrete error, as long as it extends
the right marker(s), gets the correct status **without touching `HttpError.from`**.
Concrete errors *self-classify* by the markers they pick.

## 3. Panic vs widen — the distinction that dissolves the tension

When an infra failure can't be named at a boundary, there are two very different
escape hatches, and they are easy to conflate:

- **Panic** (`Abort.panic`, or just an uncaught exception in `Sync`). A Kyo panic
  is captured as `Result.Panic` and propagates until handled — **it does *not*
  crash the JVM**; at the edge the server turns it into a 500 and the process lives.
  Its *real* drawback is that **it is invisible in the signature**. If you value a
  method's type documenting its failure modes, panic erases that.
- **Widen.** Keep the error typed and let it ascend to its least upper bound. An
  adapter method stays precise:

  ```scala
  def fetch(): List[Key] < Abort[KeyStore.Error]   // named, visible — full stop
  ```

  A use case that touches two adapters naturally gets
  `Abort[KeyStore.Error | RegistrationRepository.Error]`, and because both arms
  are `<: ApplicationError`, the whole thing is `<: Abort[ApplicationError]`. At
  the edge: `Abort.run[ApplicationError](program)` → `Result.Failure[ApplicationError]`
  → match on markers. **No panic was ever thrown, so there is nothing to
  "resurrect."** The error was a typed `Failure` the entire way; it only *widened*
  where many sources converge — which is exactly the point where you stop caring
  about the name and start acting on the marker.

> The annoyance of "errors disappear from the adapter signature" is an artifact of
> the *panic* approach. Drop panics for infra errors; keep typing; the signature
> stays precise and widening ≠ vanishing. **Reserve panic for impossible-state
> bugs**, not for "the database is down."

`Abort.runPartial[E]` is the tool that keeps the two channels honest: it handles
declared failures (`E`) and **leaves panics as defects**. So `runPartial` at the
handler + a single top-level panic handler = "domain errors → precise status,
genuine defects → 500/logged," with no `Result.Panic` branch to forget.

## 4. The granularity insight: type errors *per operation*, not per service

A *service's* total error set is hard to enumerate; a *method's* is not. So the
unit of typed-error precision is the **operation**, not the port:

```scala
def doStuff(): Unit < Abort[Error.SomeError]
def fetchStuff(id: Long): Stuff < Abort[Error.OtherStuff]
```

A caller of both gets the precise union `Error.SomeError | Error.OtherStuff` —
exactly what it can hit, nothing more — which then widens to `ApplicationError`
at the edge. This is the same spirit as "one class per use case": precision lives
at the operation.

## 5. The recommended port pattern (compile-verified)

```scala
trait PortWhateverService:
  import PortWhateverService.Error

  // Shared base: the ADAPTER channel — "any of my I/O can fail opaquely." NOTE you
  // intersect *effects*: an error type must be wrapped in Abort[...]; you cannot
  // write `Sync & ApplicationError.AdapterError`.
  type Effect = Sync & Abort[ApplicationError.AdapterError]

  // Each method adds its own DOMAIN error on top of the shared infra channel.
  def doStuff(): Unit < (Effect & Abort[Error.SomeError])
  def fetchStuff(id: Long): Stuff < (Effect & Abort[Error.OtherStuff])

object PortWhateverService:
  // The port OWNS its error ADT (in the domain). Enum while all cases share one
  // marker; switch to `sealed trait` the moment they need DIFFERENT markers.
  enum Error extends ApplicationError.DomainError:
    case SomeError(message: String)
    case OtherStuff(id: Long, message: String)
```

This is a clean realization of the two-tier model in a single signature: the
shared `Effect` carries the **adapter** channel (`Abort[AdapterError]`), each
method adds its **domain** error, so every method is
`Abort[AdapterError | ItsOwnError]` — both tiers typed, no panics.

Verified facts behind it:
- `Abort[Error.SomeError]` compiles — a **parameterized enum case is a real
  case-class type**, usable as a type argument. (A *parameterless* case is a value;
  you'd need `Error.X.type`.)
- `type Effect = Sync & Abort[ApplicationError.AdapterError]` is the fix for the
  common slip `Sync & ApplicationError.AdapterError` (a bare error type is not an
  effect).
- Errors widen: `Abort[SubError]` flows into a context wanting
  `Abort[ApplicationError]` because each arm is a subtype. `Abort.run[ApplicationError]`
  at the edge collapses the union.

Trade-offs to weigh per port:
- **`Abort[AdapterError]` in the shared `Effect`** is the honest "this port does
  I/O and that I/O can fail opaquely" admission — it names no specific adapter, so
  it's no real leak (much cleaner than the older `VendorError`, which falsely
  implied a third party). If you want the port pure-domain, drop it and let the
  adapter error widen in at the use-case layer instead.
- **The enum shares one marker** for all cases. Fine when they're all
  `DomainError`; when `SomeError` should be a `ConflictError` and `OtherStuff` a
  `NotFoundError`, drop to a `sealed trait` with per-case markers (next section).

## 6. The adapter is the translator

Adapters implement ports, and their job at every failure site is to translate the
infra world into the domain's vocabulary — *that decision is the hexagonal
boundary*:

- infra failure that **is** domain-meaningful → a port-declared domain error
  (Postgres unique-violation → `AlreadyExists`);
- infra failure that **isn't** → surface it as an `AdapterError` (plus a
  recoverability marker — `TransientError` if a retry could help), typed and
  visible; or — only for truly impossible states — a defect.

The adapter's *own* methods keep their precise named error type (`KeyStore.Error`);
that is never lost by widening, only by panicking.

## 7. Choosing the port's error representation — the menu

When a boundary must represent "an error I can't fully name," there are four
honest options, roughly in order of preference for this codebase:

1. **Per-method `Abort[SpecificError]`** (§5) — best granularity, precise and
   enumerable per operation. **Default.**
2. **`sealed trait Error extends ApplicationError` + per-case markers** — when a
   port's cases need *different* markers, or you want a named type + smart
   constructors + sealed exhaustiveness:
   ```scala
   sealed trait Error extends ApplicationError.AdapterError
   case class Unreachable(cause: Throwable) extends Error, ApplicationError.TransientError
   case class Corrupted(reason: String)     extends Error, ApplicationError.InconsistentState
   ```
3. **Sealed error + an `Infrastructure(cause)` catch-all** — keeps a port error
   *closed and exhaustive* despite unbounded adapter failures; the adapter wraps
   whatever it can't name into `Infrastructure(...)`. Most ceremony.
4. **`Abort[ApplicationError]` (root)** — simplest; the adapter's own definition
   still shows the named type, only the port *consumer* sees `ApplicationError`
   and matches markers. Acceptable, but the least informative port signature.

Avoid the **abstract type member** trick (`type Error >: X <: ApplicationError`,
refined per adapter): it keeps the port typed-but-abstract, but path-dependent
`port.Error` becomes awkward the moment a use case composes several ports.

## 8. Worked example — `KeyStore` (the 503-vs-500 line)

`KeyStore` (a k8s-Secret-backed adapter) originally had
`enum Error extends ApplicationError.VendorError` — *one* shared, and *wrong*,
marker for both cases (the k8s API is not a "vendor"). The fix illustrates the
whole note. Both cases share the **source** umbrella `AdapterError`; they differ on
the **recoverability** axis — apply **"will a retry plausibly succeed?"**:

- `Unreachable(cause)` — API blip / RBAC / network → *yes, later* →
  `AdapterError, TransientError` → **503**.
- `Corrupted(reason)` — the stored JSON won't parse → *no, the same read returns
  the same garbage* → `AdapterError, InconsistentState` → **500**.

So the two cases share the umbrella but want **different recoverability markers** →
option #2 (sealed trait `extends AdapterError`, per-case behavioral marker) over the
enum. And because the HTTP layer maps on markers, that's the *only* change needed —
`HttpError.from` is untouched.

Deeper still: a corrupted *signing-key* store means the service can't issue tokens
*at all*. That is arguably a **readiness/health** failure (don't serve) rather than
a per-request 500. Hold that thought until the key-manager layer exists — error
modeling and health modeling overlap at the "this instance is broken" boundary.

## 9. Kyo mechanics cheat-sheet (verified, RC4 / Scala 3.8.2)

- **Wrap errors in `Abort`**: effects intersect with `&`; `Abort[E]` is the
  effect, `E` alone is not.
- **Enum case as a type**: parameterized cases are case-class types
  (`Abort[Error.SomeError]` ✓); parameterless cases need `.type`.
- **Widening**: `Abort[Sub]` is usable where `Abort[ApplicationError]` is expected;
  unions of error arms collapse under `Abort.run[ApplicationError]`.
- **`Abort.run[E]`** handles failures *and* panics into a `Result` (match needs a
  `Panic` branch); **`Abort.runPartial[E]`** handles only declared failures and
  leaves panics as defects (match is just `Success | Failure`). Prefer
  `runPartial` at the handler so panics flow to a single top-level handler.
- **Named tuples** (`(id: Long, name: String)`) compile on 3.8 — fine for
  ephemeral shapes; prefer a `case class` for a *returned domain value*.

## Takeaways

1. **Global = markers only.** They drive HTTP/retry, and the HTTP mapper switches
   on them — so new errors with the right marker are correct for free (open/closed).
2. **Type errors per operation, not per service.** A method's failure set is
   knowable; a service's isn't.
3. **Domain errors are typed & named; infra failures are typed & widened.** Don't
   reach for panic just to get an error "out of the way" — widening keeps it in
   the signature and needs no resurrection.
4. **Panic is for impossible-state bugs**, not for "a dependency is down." It's
   safe (no crash) but invisible — wrong tool for documented failure modes.
5. **The adapter is the translator**: infra failure → a domain error (if
   meaningful) or an `AdapterError` (+ `TransientError` if retryable). Its own
   signature keeps the precise named type.
6. **Pick the port representation by need**: per-method specific (default) →
   sealed-trait-with-markers (cases diverge) → sealed+catch-all (closed) →
   `ApplicationError` (simplest). Skip abstract type members.
7. **503 vs 500 = "will a retry help?"** `TransientError` vs `InconsistentState`.
8. **Two axes, don't conflate them.** *Source* = `AdapterError` (the opaque
   domain-facing umbrella; `VendorError` is the narrower "named upstream → 502").
   *Recoverability* = `TransientError` (503) vs permanent (500). A concrete error
   picks one of each; the HTTP mapper prefers the recoverability axis.
