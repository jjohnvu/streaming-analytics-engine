# Streaming Analytics Engine

A single-process **stream-processing engine in Go** that consumes a stream of
timestamped events, groups them by key, and computes aggregations over time
windows — correctly handling **late and out-of-order events**.

The headline capability is **event-time processing with watermarks**: the engine
reasons about *when an event actually happened*, not when it happened to arrive,
and uses watermarks to decide when a time window is safe to close.

## Why this is the hard part

Real event streams have two clocks:

- **Event time** — when the event actually happened (baked into the event).
- **Processing time** — when the engine sees it.

These diverge in practice (network delay, retries, offline clients). An engine
that aggregates on processing time is silently *wrong*. Using event time forces
the central question this project exists to answer:

> When is it safe to declare a window complete, if more data for it might still
> arrive?

**Watermarks** are the answer. A watermark of `W` is a promise that no event with
event time `< W` should still arrive. When `W` passes a window's end, that window
closes. Events that arrive late are either folded in (within an allowed-lateness
grace period) or routed to a **side output** — never silently dropped.

The tradeoff, in one line: **larger watermark delay → more correct, higher
latency; smaller → faster, more data to the side output. No free lunch.**

## Architecture

Five stages, data flowing left to right:

```
Source → Ingestion → Window Assigner → Aggregator → Sink
              │                                        │
        (watermark gen)                          (+ side output for late data)
```

- **Source** — the synthetic load generator.
- **Ingestion** — parses events, extracts event time, hosts the watermark
  generator.
- **Window Assigner** — maps each event to its window(s) by event time.
- **Aggregator** — maintains per-(key, window) aggregate state.
- **Sink** — receives finalized window results, with a separate channel for
  late data.

Two design choices are load-bearing:

1. **`Aggregator` is an interface** (`Add`, `Merge`, `Result`) from day one —
   `Merge` is what makes session-window merging and distributed partial
   aggregates clean.
2. **Watermarks travel in-band**, through the same channels as events, so
   ordering guarantees hold (the way Flink does it).

## The load generator

Input data is *generated*, not real — and that's the point: the generator
manufactures the exact late / out-of-order conditions the engine's hard parts
exist to handle. It exposes four knobs:

| Knob                 | Meaning                                            |
| -------------------- | -------------------------------------------------- |
| `EventsPerSec`       | throughput                                         |
| `LateFraction`       | fraction of events emitted deliberately late       |
| `MaxLatenessMs`      | how far behind the frontier a late event can sit   |
| `OutOfOrderJitterMs` | jitter so even on-time events aren't perfectly ordered |

It separates a **logical event-time frontier** (which marches forward one step
per event) from **real-time pacing** (which only controls *when* events are
emitted) — so scheduling jitter never corrupts the data.

## Domain

Events model ride/delivery trips: `Key` = a city zone or route (e.g. `zone-3`),
`Value` = a trip fare or delivery time, `EventTime` = when the trip completed.
Aggregations read as "avg fare per zone per minute" / "p99 delivery time per
route." The engine itself is domain-agnostic; the domain is just a coherent demo.

## Status

Built so far:

- [x] Core types — `Event`, `Watermark`, `Window` (half-open `[Start, End)`),
      `WindowState`
- [x] `Aggregator` interface + `SumAggregator`
- [x] Load generator with all four knobs
- [ ] Tumbling window assigner
- [ ] Pipeline wiring (Source → … → Sink) printing per-(zone, window) aggregates
- [ ] Throughput + p50/p99 latency benchmark
- [ ] Watermarks, allowed lateness, side output (the milestone)
- [ ] Sliding + session windows

## Build & test

Standard library only — no third-party dependencies.

```sh
go test ./...      # run all tests
go vet ./...       # static checks
```

A one-line run command for the live demo lands with the pipeline-wiring step.

## Layout

```
engine/
  types.go        # Event, Watermark, Window, WindowState
  aggregator.go   # Aggregator interface + SumAggregator
  generator.go    # synthetic load generator (four knobs)
  *_test.go       # a test alongside each core mechanic
```
