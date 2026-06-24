// Command bench measures the engine's steady-state processing performance:
// throughput (events/sec) and per-event processing latency (p50/p99).
//
// Run: go run ./cmd/bench
//
// Events are pre-generated so the load generator's pacing isn't on the critical
// path — this measures the engine's aggregation hot path, not the source.
package main

import (
	"flag"
	"fmt"
	"time"

	"github.com/jjohnvu/streaming-analytics-engine/engine"
)

func main() {
	var (
		n        = flag.Int("n", 1_000_000, "number of events to process")
		windowMs = flag.Int64("window", 1000, "tumbling window size in ms")
	)
	flag.Parse()
	if *n <= 0 {
		fmt.Println("n must be positive")
		return
	}

	gen := engine.NewGenerator(engine.GeneratorConfig{
		EventsPerSec: 1000, Seed: 1,
		LateFraction: 0.1, MaxLatenessMs: 2000, OutOfOrderJitterMs: 250,
	})
	events := gen.Generate(*n)
	newAgg := func() engine.Aggregator { return engine.NewSumAggregator() }

	// Pass 1 — throughput: no per-event timer, so timing overhead doesn't tax
	// the number.
	ag := engine.NewAggregation(engine.NewTumblingAssigner(*windowMs), newAgg)
	start := time.Now()
	for _, ev := range events {
		ag.AddEvent(ev)
	}
	elapsed := time.Since(start)
	eps := float64(*n) / elapsed.Seconds()

	// Pass 2 — latency distribution: time each Add. (This includes timer
	// overhead, so absolute values are conservative.)
	ag2 := engine.NewAggregation(engine.NewTumblingAssigner(*windowMs), newAgg)
	lat := make([]time.Duration, *n)
	for i, ev := range events {
		t0 := time.Now()
		ag2.AddEvent(ev)
		lat[i] = time.Since(t0)
	}

	fmt.Printf("events:      %d\n", *n)
	fmt.Printf("window:      %d ms\n", *windowMs)
	fmt.Printf("processed in %s\n", elapsed.Round(time.Millisecond))
	fmt.Printf("throughput:  %.0f events/sec\n", eps)
	fmt.Printf("latency p50: %v\n", engine.Percentile(lat, 50))
	fmt.Printf("latency p99: %v\n", engine.Percentile(lat, 99))
}
