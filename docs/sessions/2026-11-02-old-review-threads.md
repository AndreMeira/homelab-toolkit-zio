---
title: "2026-11-02 — the old review threads, answered; renames approved and parked"
type: session
status: current
updated: 2026-11-02
tags: [review, pollconsumer, naming, backlog]
---

# 2026-11-02 — the old review threads, answered

Old review threads that were still unanswered (on toolkit #1, #2, #13, #20, #21, #22 and on DKQ) were triaged
against current `main`. The ones that no longer held got a reply naming the commit that fixed them, or saying
why they were not valid. The ones that still held are fixed in the PR this note ships with. The DKQ side is in
`distributed-keyed-queue/docs/sessions/2026-11-02-old-review-threads.md`.

## Fixed in this PR

- **`messaging.md` and the `PollConsumer` class doc** state the lease bound in terms of the fibers calling
  `consume`, since there is no `concurrency` setting any more. They also name the exception: a caller interrupted
  while waiting for its verdict leaves a lease that no caller holds until the settler writes it.
- **The `PollConsumer.Channel` doc** now says four queues, not three. Its token claim now counts what departed
  callers leave behind: a cancellation debt, or a queued verdict.
- **`dependabot.yml`**: the header no longer claims to watch the runner image. Dependabot reads `uses:`, never
  `runs-on:`.
- **`AnthropicClientSpec`** asserts exactly one measured call, as `OpenAIClientSpec` does.

**The settlement queue stays unbounded.** That was already decided in
[`code-review-performance-and-security.md`](../research/code-review-performance-and-security.md) ("Four
unbounded queues, deliberately"): memory is traded for liveness, because a settler stuck on a full queue while
holding claimed work is the worse failure. The Copilot thread found one case that the decision's "bounded by
the worker count" sentence missed (the interrupted-while-waiting caller), and the docs above now state it.

## Approved, not done yet

The goal was to keep the verification queue short, so these are parked here rather than mixed into the review
fixes:

- **`PollConsumer.Verdict.of(exit)` → `Verdict(exit)`.** No `of` factories, which are a Java/Kotlin convention.
- **`InMemoryBucket.of(value)` → `InMemoryBucket.make(value)`**, alongside the existing `make`. This is public in
  `homelab-common`, so it is a source break; say so in the next release notes.

The DKQ renames (`KeyLayout.of`, and the `claim(of: …)` / `verdicts(of: …)` spec helpers) are listed in the DKQ
note.

## Still open

- Adopting `Shutdown` in DKQ, deferred since the toolkit v0.0.6 release.
