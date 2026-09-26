---
title: "2026-09-26 — what Dependabot's first run found, and the coverage it walked into"
type: session
status: current
updated: 2026-09-26
tags: [dependabot, dependencies, fabric8, kubernetes, testing, coverage, auth, task]
---

# What Dependabot's first run found

Dependabot was configured today and opened five PRs within the hour. Four were routine. One failed, and
what it failed on is worth keeping — along with the gap it walked into, which is the part still open.

## The one that failed

`io.fabric8:kubernetes-client` 6.0.0 → 7.9.0 would not compile:

```
value withToken is not a member of …TokenReviewFluent#SpecNested
```

`withToken(String)` is present in 7.9.0. It is unreachable. fabric8 7 changed the nested fluent builders
from static interfaces to inner classes:

| | `TokenReviewFluent$SpecNested<N>` |
|---|---|
| 6.0.0 | interface extending `TokenReviewSpecFluent<TokenReviewFluent$SpecNested<N>>` |
| 7.9.0 | class extending `TokenReviewSpecFluent<TokenReviewFluent<A>.SpecNested<N>>` |

The 7.x supertype names `A`, the *enclosing* builder's type parameter, which is not bound in `SpecNested`'s
own signature. Scala resolves no inherited member through that, so every setter on the nested builder
disappears and the failure reads as a missing method.

This is not specific to one call site: any Scala caller of fabric8's nested builder chain breaks the same
way. The fix is to use the top-level `TokenReviewSpecBuilder` and hand the spec to `withSpec` — both of
which exist in 6.0.0 and 7.x, so the fix is version-agnostic and was landed as its own commit ahead of the
bump (#18).

**Worth remembering for the next fabric8 major:** the error a Java generics interop break produces in Scala
is "this method does not exist", which sends you looking for a rename. `javap` on the jar is what settles
it — the method was there the whole time.

## The gap it walked into

`K8sTokenReviewer` has no test. The suite was green, and green meant *it compiles* — which is how a
major-version API break in the Kubernetes client reached a release. The request had to be checked by
building one by hand.

It is not one class. Seven of the fifteen types in `auth` are named in no test anywhere:

| untested | what it would take |
|---|---|
| `K8sTokenReviewer` | a stand-in `KubernetesClient` — it takes one as a constructor argument, and fabric8 publishes a mock-server artifact |
| `K8sJwksSource` | a stand-in `HttpClient`, which it also takes as a constructor argument |
| `HttpJwksSource` | the same, through the `protected def client` its subclasses override |
| `CachedJwksSource` | a stub `JwksSource` and a clock — no I/O at all |
| `CachedTokenProvider` | the same |
| `JwtProvider` | nothing; it is signing |
| `PublicKeyDecoder` | nothing; it is parsing |

Nothing here is blocked on a seam: every one of them already takes its collaborator as an argument. The
last four need no fixture whatever. So the shape of this is not "these are hard to test" — the ones that
would want a fixture were skipped, and the ones that want nothing were skipped alongside them.

The one real cost sits with the two JWKS sources: they speak over the JDK's `java.net.http.HttpClient`,
which is an abstract class rather than an interface, so a stand-in means subclassing it or pointing them at
a local server. That is more setup than sttp's `BackendStub` buys the LLM adapters — a reason to start
elsewhere, not a reason to stop.

`auth` is also the module whose ports are already promoted into `common`, which makes it the module a
service types its own code against.

## The task

Cover the four that need no fixture first — they are cheap, and a silent break in them is quietest of all.
Then decide what the other three are worth: a fabric8 mock server for the TokenReview path, and a local
`HttpServer` the two JWKS sources can be pointed at.

Nothing here argues for a coverage target. The argument is narrower: a dependency bump is a claim that the
suite would notice if the dependency changed under it, and for these seven that claim is false.
