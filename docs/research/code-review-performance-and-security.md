---
title: "Toolkit code review — performance and security"
type: research
status: current
updated: 2026-09-26
tags: [review, performance, security, auth, llm, tokens, logging, blocking]
---

# Performance and security

> One of three: [guideline adherence and style drift](code-review-guidelines-and-style.md) · [readability](code-review-readability.md).
>
> Measured at `3582a7e`, 2026-09-26. Counts are reproducible with the greps described; like every
> note here they record a point in time and are not edited to stay true.

A review of the eight published modules — `common`, `auth`, `nats`, `postgres`, `telemetry`, `llm`,
`llm-openai`, `llm-anthropic`. 128 main source files, ~6,900 lines of code under ~7,000 lines of comment.
The incubator is out of scope: it is `publish / skip := true`.

Findings are ordered by what they would cost if left. Each names a file and line so it can be checked
rather than taken on trust; where a concern turned out to be already handled, that is recorded too, because
knowing what was checked is half the value of a review.

## Security

### Bearer tokens are held in memory as map keys

`auth/CachedTokenVerifier.scala:34`

```scala
cache: Ref[Map[SignedToken, CachedTokenVerifier.Entry]]
```

The cache is keyed on the raw token. For the length of the TTL, the process holds every token it has
recently verified, in a map, in plaintext. A heap dump — a crash artefact, a debugger, an OOM dump collected
by a platform — contains usable credentials for every caller seen in that window.

Keying on a digest of the token instead (`SHA-256`, hex) removes the credential from memory while keeping
the cache exactly as useful: the lookup key is derived from what the caller presented, and nothing in the
map is replayable.

What is already right here, and worth not breaking:

- **Failures are not cached.** `verifyAndStore` only writes after `inner.verify` succeeds, so a flood of
  distinct invalid tokens cannot grow the map. That closes the obvious amplification.
- **An entry cannot outlive its token.** `expirationFrom` caps the entry at the earlier of `now + ttl` and
  the token's own `exp`, so a cached verification never authorises a token past its expiry.

There is no timing concern: the map lookup is on a value the caller supplied, not a comparison against a
secret.

### Response bodies reach error values, and errors reach the log

`llm-openai/HttpChatCompletionClient.scala:77,90` and `llm-anthropic/HttpAnthropicClient.scala:91,104`

```scala
val detail = body.fromJson[FailureResponse].map(_.error.message).getOrElse(body.take(200))
…
ChatCompletionError.Malformed(s"$reason, in ${body.take(200)}")
```

Up to 200 characters of the provider's response body are carried in the error. On a `Malformed` — a 200
response whose body did not parse — that body is the model's own completion, which holds whatever the
conversation held.

The path to a log is now short, and this review is the first place it is written down:

1. the client fails with `Malformed`, whose `message` interpolates the excerpt
2. the call is wrapped in `monitor.measure(…)`
3. `Monitor.WithLogging` does `.onError(cause => ZIO.logErrorCause(…, cause))`

So conversation content can land in the log of any service that composes `WithLogging` over its monitor.
The excerpt is genuinely useful when a gateway returns an HTML error page, which is what it was added for.
The two are separable: keep the excerpt for a body that is *not* JSON, and drop it for one that parsed as
JSON and merely was not a completion — the decoder's own message already says what was missing.

### Checked and clean

- **No token value in any error message.** Every failure in `auth` carries a reason
  (`TokenRejected("token not valid for audience '…'")`), never the credential. `CanNotDecodeToken` carries
  `cause.getMessage` from jwt-scala, which is the one place a library could widen this; worth a glance if
  jwt-scala is ever swapped.
- **No SQL is built by interpolation of values.** `postgres` goes through Magnum's `sql"…"`, which
  parameterises. There is no `createStatement`, no string-concatenated predicate.
- **No partial escapes anywhere.** Zero `throw`, `orElseThrow`, `asInstanceOf`, `Option.get`. See the
  style review for the full rule-by-rule measurement; it matters here because each of those is a failure
  mode that does not appear in a signature.

## Performance

### A tool's JSON Schema is rebuilt on every request

`llm/schema/JsonSchema.scala:48` — `def json: Json`

The schema itself is computed once: `Registered.jsonSchema` is a constructor field, so generation happens
at registration. The *rendering* is not. `json` is a `def` on an immutable `final case class`, and it walks
`root` and every entry in `definitions`, allocating a fresh `Json` AST each time it is called.

It is called once per advertised tool per request:

```
CompletionRequest.from → request.tools.map(ToolRequest.from) → advertised.arguments.json
```

So a session offering eight tools rebuilds eight schema ASTs on every call to the model. The values are
immutable and constructed once, which makes this the cheapest possible fix: `lazy val json` instead of
`def json`, and the same for `Node.json` if the recursion should be memoised at each level too.

Nothing here is measured against a profile. It is called out because the cost is per-request, unbounded in
the number of tools, and removable by one keyword — not because it has been shown to matter.

### A blocking build on the async pool

`auth/K8sTokenReviewer.scala:156`

```scala
ZIO.fromAutoCloseable(ZIO.attempt(KubernetesClientBuilder().build()))
```

`build()` reads a kubeconfig or a service-account token and CA certificate from disk, and may resolve
environment and cluster configuration. That is file I/O on `ZIO.attempt`, which runs on the async pool. The
same file's `send` already gets this right — `ZIO.attemptBlocking(client.tokenReviews().create(review))` at
line 86 — so this is an inconsistency within one file rather than a missing idea.

It happens once per reviewer, at construction, so the cost is small and the fix is one word:
`attemptBlocking`.

### Pruning on every write makes each cache miss O(n)

`auth/CachedTokenVerifier.scala:63`

```scala
cache.update(_.filter { case (_, e) => e.isFresh(now) }.updated(token, entry))
```

Every miss rebuilds the whole map to drop stale entries. With `n` live tokens that is O(n) work and O(n)
allocation per miss, under a `Ref.update` that may retry under contention. For a handful of service
identities this is nothing. For a gateway verifying per-user tokens it is a cost that grows with traffic.

Pruning on a schedule, or only when the map exceeds a threshold, separates the two concerns — the write
becomes `updated`, and eviction stops riding on the miss path.

### Collection conversions at the ZIO boundary

37 sites across the published modules convert between `Chunk` and `List` (31 `.toList`, 6
`Chunk.fromIterable`). Most are one boundary: `common`'s ports speak `List` while ZIO's own API speaks
`Chunk`, so `QueueSource.takeUpTo` does `queue.takeBetween(1, n).map(_.toList)` — a copy on every dequeue.

Each conversion is O(n) and allocates. None of them is hot enough to matter on its own; together they are a
symptom rather than a cost, and the cause belongs to the style review, which has the measurement.

### Four unbounded queues, deliberately

`common/messaging/PollConsumer.scala:314-317` — `demand`, `supply`, `settlement`, `cancels` are all
`Queue.unbounded`.

This is argued in the source rather than accidental: *"the queue is unbounded so returning them cannot
block"* and *"The post cannot block: the settlement queue is unbounded."* The trade is memory for liveness,
and for this component liveness is the right side — a settler blocked on a full queue while holding claimed
work is a worse failure than a large queue.

The observation is not that it is wrong; it is that the invariant bounding the memory is not stated next to
the queues. `supply` stays small because demand is only ever raised by an idle worker, so the number in
flight is bounded by the worker count. That is the property a reader needs in order to believe the
unbounded queue is safe, and it lives elsewhere in the file. A sentence at the declaration would carry it.

### Checked and fine

- **`Tool.Result.render` builds an encoder per call** (`llm/Tool.scala:266`) while the failure path uses a
  cached `errorEncoder`. This looks asymmetric and is not a defect: `Error` is monomorphic so its encoder
  can be a module-level `val`, while `A` varies per instance so no single `val` can serve. Caching per
  schema would need a keyed cache, which is more machinery than an unmeasured cost justifies.
- **No unbounded growth in the token cache under a spray**, since failures are not cached.
- **No `Int.MaxValue` bounds, no busy-wait loops, no `while(true)` in the published modules.**

## What to do first

1. **Key the token cache on a digest.** Smallest change, removes credentials from the heap.
2. **Narrow the body excerpt to non-JSON bodies.** Keeps the diagnostic, drops conversation content from
   logs.
3. **`lazy val json`.** One keyword, removes per-request work proportional to the tool count.
4. `attemptBlocking` for the Kubernetes client build.

Items 3 and 4 are small enough to do together. Items 1 and 2 change behaviour a caller could observe — a
different cache key, a shorter error message — so they want their own commits and their own tests.
