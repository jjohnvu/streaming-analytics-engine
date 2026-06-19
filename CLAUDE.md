# CONTEXT.md — Streaming Analytics Engine

> This file is the source of truth for the project. Read it fully before doing
> anything. When a decision conflicts with this file, this file wins. When you
> make a meaningful decision not covered here, propose adding it to this file.

## What we're building

A single-process stream-processing engine (in Go) that consumes a stream of
timestamped events, groups them by key, and computes aggregations over time
windows — correctly handling **late and out-of-order** events.

The headline capability — the reason this project exists — is **event-time
processing with watermarks**. Anything that undermines or shortcuts the
watermark / late-data path is the wrong call.

The hard conceptual spine is two clocks:
- **Event time** — when the event actually happened (baked into the event).
- **Processing time** — when the engine sees it.

These diverge in real systems (network delay, retries, offline clients). A naive
engine uses processing time and is silently wrong. This engine uses event time,
which forces the central question: *when is it safe to declare a window complete
if more data for it might still arrive?* Watermarks answer that.

## Language & tooling

- **Go.** Not Rust. (Timeline is ~10 hrs/week; Rust's borrow-checker tax on
  concurrent state isn't worth it here. Performance story is fine in Go.)
- Standard library first. Do not add dependencies without proposing it here.
- Tests with the standard `testing` package. Every core mechanic gets a test.
- `go fmt` / `go vet` clean at all times.

## Architecture — the pipeline

Five stages, data flowing left to right:

```
Source → Ingestion → Window Assigner → Aggregator → Sink
                │                                      │
          (watermark gen)                       (+ side output for late data)
```

- **Source** — the load generator (synthetic events). Later could be file/socket;
  not now.
- **Ingestion** — parses events, extracts event-time, hosts the **watermark
  generator** (tracks max event-time seen, emits watermarks).
- **Window Assigner** — maps each event to the window(s) it belongs to, by
  event-time. One event may land in multiple windows (sliding) or extend one
  (session).
- **Aggregator** — maintains per-(key, window) aggregate state, updates on each
  event.
- **Sink** — receives finalized window results when windows close; has a
  **separate side-output channel** for events too late to include.

### Two design decisions that are locked in

1. **Aggregator is an interface, not a hardcoded sum:**
   `Add(value)`, `Merge(other)`, `Result()`. The `Merge` method is what makes
   session-window merging and (later) distributed partial aggregates clean.
   Build it as an interface from day one.

2. **Watermarks travel through the same channels as events** — a unified stream
   (a tagged struct or an interface both satisfy). Watermarks must NOT travel
   out-of-band, or ordering guarantees break. "A watermark is just another thing
   in the stream" is the mental model (this is how Flink does it).

## Data model

Keep the event minimal. Do not add fields without justifying them here.

```go
type Event struct {
    Key       string  // grouping dimension, e.g. "zone-3"
    Value     float64 // the thing being aggregated
    EventTime int64   // when it happened (unix millis) — the important one
}

type Watermark struct {
    Timestamp int64 // "no event with EventTime < this should arrive"
}

type Window struct {
    Start int64 // inclusive
    End   int64 // exclusive — half-open [Start, End)
}

type WindowState struct {
    Window Window
    Key    string
    Agg    Aggregator // running aggregate
}
```

Window state lives in a map keyed by (key, window). Managing that map's size —
closing and evicting windows once they can't receive more data — is part of the
problem, not an afterthought.

**Half-open intervals `[Start, End)` are mandatory** — that's how a boundary
event avoids being counted in two windows.

## Domain

Events model **ride/delivery trips** (chosen to rhyme with Uber Freight, so the
project and the day job tell one story):
- `Key` = city zone or route ID (e.g. "zone-3")
- `Value` = trip fare or delivery time
- `EventTime` = when the trip completed

Aggregations read as "avg fare per zone per minute" / "p99 delivery time per
route" — real-time operational metrics a logistics platform actually computes.
The engine itself is domain-agnostic; the domain is just for a coherent demo.

## The load generator (first thing to build)

Data is **generated, not real**. The generator is a feature, not a shortcut — it
manufactures the exact conditions the engine's hard parts exist to handle.

Must be configurable with these knobs (build them in from the start):
- `eventsPerSec` — throughput (drives benchmark numbers)
- `lateFraction` — fraction of events deliberately emitted late
- `maxLatenessMs` — how late a late event can be
- `outOfOrderJitterMs` — jitter so even non-late events aren't perfectly ordered

**Without injected lateness, the watermark/late-data path is never exercised** —
it'd be untested code. Manufacturing late events on purpose is the generator's
most important job.

## Window types (in build order)

1. **Tumbling** — fixed-size, non-overlapping. Each event in exactly one window.
   `start = eventTime - (eventTime % size)`. This is the MVP assigner.
2. **Sliding** — fixed size + slide interval; overlapping. An event lands in
   every window whose range covers it (up to size/slide windows).
3. **Session** — variable size, defined by a gap timeout. Events extend the
   current session until a gap of inactivity closes it. A late event in the gap
   can **merge** two previously-separate sessions (this is where `Merge` earns
   its keep).

## Watermark mechanics (the heart — spend depth here)

- Watermark generator tracks max event-time seen, emits
  `watermark = maxEventTime - allowedLateness`. The subtraction is the engine
  deliberately holding back to tolerate stragglers.
- When watermark W flows down: **every window with `End <= W` closes** — emit its
  final result to the sink, evict its state.
- Late event (`eventTime < W`):
  - within an **allowed-lateness** grace period → update the window, emit an
    updated result.
  - past that → route to **side output** (never silently dropped).
- The tradeoff to be able to articulate at a whiteboard:
  **larger watermark delay / allowed lateness → more correct, higher latency;
  smaller → faster, more data to side output. No free lunch.**

## Scope discipline

### In scope now (MVP → hard milestone)
- Load generator with the four knobs
- Tumbling → sliding → session assigners
- Sum aggregator (then avg, min/max, count) behind the Aggregator interface
- Watermark generation, window closing/eviction, allowed lateness, side output
- Throughput + p50/p99 latency benchmarks
- Tests for each core mechanic

### Explicitly OUT of scope (do not build until earned)
- Distribution / partitioning across nodes
- Checkpointing & exactly-once recovery
- Real Kafka / socket / file ingestion
- Any persistence layer

Naming these out-of-scope is the discipline that keeps the early weeks
finishable. If the engine is solid and there's time, the distributed +
checkpointing stretch comes last and interview-driven, not preemptively.

## Roadmap (≈10 hrs/week)

- **Weeks 1–2 — MVP, shippable.** Generator + tumbling + sum aggregator + real
  throughput/latency numbers. The moment it works: push to GitHub with a README
  that has a benchmark and a pipeline diagram.
- **Weeks 3–5 — watermark milestone (the interview-winner).** Event-time vs
  processing-time, watermarks, allowed lateness, side output, then sliding +
  session windows.
- **Weeks 6+ — distributed stretch (optional, interview-driven).** Partitioning,
  checkpointing, exactly-once recovery.

Update live materials (resume / LinkedIn / GitHub README) **at each milestone**,
not at the end. A partially-built project with a clean README and a working demo
is deployable in conversations immediately.

## Definition of done — Week 1 MVP
- [ ] `Event`, `Watermark`, `Window`, `WindowState` types defined
- [ ] `Aggregator` interface + a `SumAggregator` implementation
- [ ] Load generator emitting an out-of-order, occasionally-late stream with all
      four config knobs working
- [ ] Tumbling window assigner (half-open intervals, verified by test)
- [ ] Events flow Source → Ingestion → Assigner → Aggregator → Sink
- [ ] Engine prints per-(zone, window) aggregates
- [ ] A benchmark prints events/sec and p50/p99 processing latency
- [ ] `go vet` clean, core mechanics covered by tests
- [ ] README with a one-line run command and a pipeline diagram

## Working agreement for Claude Code
- Read this file before each task. Keep changes small and reviewable.
- Write the test alongside the code for any core mechanic.
- When you hit a fork not covered here, state the options and the tradeoff, pick
  one, and propose the addition to this file rather than deciding silently.
- Don't pull in dependencies or build out-of-scope features without flagging it
  against the Scope section first.
