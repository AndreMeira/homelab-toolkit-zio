---
title: "Standing removed — why the toolkit stopped tracking outstanding work"
type: research
status: current
updated: 2026-09-23
tags: [llm, agent, tool, transcript, subagent, removal, v4]
---

# Standing removed

`Tool.Result.Standing` and `Transcript.Outstanding` were removed from the LLM toolkit on 2026-09-23,
before the v4 promotion. This note records what they were, why they went, and what has to be true for
them to come back.

## What they were

A tool's result carried, beside its value, what the call left owed:

```scala
enum Standing:
  case Answered                                    // the text is the whole result
  case Promised(handles: NonEmptyChunk[String])    // work started, result not here
  case Delivered(handles: NonEmptyChunk[String])   // this call carries work named earlier
```

A transcript entry kept it, and `Transcript.outstanding` folded promised minus delivered over a
conversation to answer "is anything still owed". The case it was built for: a tool launches a subagent,
returns immediately, and the conversation ends with an answer while the child is still running.

## Why it went

**Nothing could produce one.** The only public constructor was

```scala
def success[A: Schema](value: A): Result[A] = Result.Succeeded(value, Standing.Answered)
```

with `Answered` hardcoded. A tool wanting to promise had to build `Result.Succeeded(v, Promised(…))` by
hand. Nothing read the result either — `outstanding` was called from tests and from nowhere else. The
feature was unreachable in both directions.

**It was a second record of a fact that lives elsewhere.** A tool that starts a subagent has its own row
for it: which child, started when, still alive or not. `Standing` shadowed that row in the transcript, and
the two can drift — a row deleted, or a delivery appended by someone who forgot to mark it. The
transcript's whole premise is the opposite: *nothing records where a turn has got to; the entries say it,
so there is no second record to keep in step.* Standing was the one thing in it that broke that rule, and
the tool's own row is the better of the two anyway, because it knows **what** is running and can check
whether it still is.

**It was vestigial.** `Delivered` only means something when a *later tool call* hands back work an
*earlier* one started — the `wait` tool of the first design, where the parent holds a call open. That
design was replaced by the nudge: the parent ends its run and settles, and a signal wakes the conversation
when the child finishes. In the nudge design the delivery arrives as a **message**, not as an answer to an
outstanding call, so there is no tool result for `Delivered` to sit on. `Promised` without `Delivered` is a
marker nobody ever clears.

See [the conversation model](llm-conversation-model.md) for the design this supersedes in part; the nudge
it lost to is `docs/research/the-queue-as-a-signal.md` in the distributed-keyed-queue repo.

## What replaces it

Nothing in the toolkit. The tool owns its own bookkeeping:

1. the tool starts the child, writes a row keyed by the call id, and answers the model in words —
   "working; the result will be appended when it finishes";
2. the loop appends that answer and settles, holding nothing;
3. the child finishes, writes its result to the row, and enqueues a nudge on the conversation's key,
   carrying the call id;
4. the woken runner reads the row and appends the result as a message.

Every part of that is the tool's and the application's. The toolkit's transcript carries words and who
said them, which is all a request needs.

## What was given up

The toolkit can no longer answer **"is this conversation owed anything?"** generically. A supervisor
sweeping for conversations waiting on something that will never arrive has to ask each tool's own storage
instead of asking the transcript once.

That is a real loss and it is accepted, because the generic answer was the weaker one: promised-minus-
delivered says only that something was once promised, where the tool's row says whether the child is
still alive.

## What would bring it back

- **A cross-tool handle** — tool A promises work that tool B delivers. The transcript is then the only
  place both can see, and a handle vocabulary has to live somewhere shared.
- **A wait-style tool** — if a parent ever holds a call open across a child's lifetime again, `Delivered`
  regains the site it lost.
- **A toolkit-level supervisor** — something in the library, rather than in an application, that has to
  sweep stalled conversations without knowing which tools exist.

None of these is on the roadmap. If one arrives, the shapes it could take — a side store, a column on the
message store, or a second row in the conversation's own stream — are laid out with their costs in
[`outstanding-work-in-storage.md`](./outstanding-work-in-storage.md).

## Still open

What a nudge-delivered result is recorded as. It is not a `ToolResult` — that call was answered when the
tool returned "working" — so on the wire it is a `User` message like any other. Whether the store also
records that it came from a subagent rather than from the caller is a question for the store's row, not
for the message: the transcript is `Chunk[Model.Message]`, and a domain type for provenance was tried
(`Entry`) and removed once `Standing` left it with nothing to carry. Undecided at the time of writing.
