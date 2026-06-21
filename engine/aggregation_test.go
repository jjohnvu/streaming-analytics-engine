package engine

import (
	"context"
	"testing"
	"time"
)

func sumFactory() func() Aggregator {
	return func() Aggregator { return NewSumAggregator() }
}

// TestAggregationSumsByKeyWindow checks the core fold: events are summed within
// their (key, window) cell and kept separate across keys and windows.
func TestAggregationSumsByKeyWindow(t *testing.T) {
	ag := NewAggregation(NewTumblingAssigner(1000), sumFactory())

	ag.Add(Event{Key: "zone-a", Value: 10, EventTime: 500})
	ag.Add(Event{Key: "zone-a", Value: 5, EventTime: 700})
	ag.Add(Event{Key: "zone-a", Value: 3, EventTime: 1500}) // next window
	ag.Add(Event{Key: "zone-b", Value: 2, EventTime: 500})

	want := []WindowResult{
		{Key: "zone-a", Window: Window{0, 1000}, Value: 15},
		{Key: "zone-a", Window: Window{1000, 2000}, Value: 3},
		{Key: "zone-b", Window: Window{0, 1000}, Value: 2},
	}

	got := ag.Results()
	if len(got) != len(want) {
		t.Fatalf("got %d results, want %d: %+v", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("result %d = %+v, want %+v", i, got[i], want[i])
		}
	}
}

// TestAggregationResultsSorted: results come out ordered by key then window
// start, regardless of insertion order.
func TestAggregationResultsSorted(t *testing.T) {
	ag := NewAggregation(NewTumblingAssigner(100), sumFactory())
	ag.Add(Event{Key: "z", Value: 1, EventTime: 350})
	ag.Add(Event{Key: "a", Value: 1, EventTime: 250})
	ag.Add(Event{Key: "a", Value: 1, EventTime: 50})

	got := ag.Results()
	for i := 1; i < len(got); i++ {
		prev, cur := got[i-1], got[i]
		ordered := prev.Key < cur.Key ||
			(prev.Key == cur.Key && prev.Window.Start <= cur.Window.Start)
		if !ordered {
			t.Errorf("results not sorted at %d: %+v then %+v", i, prev, cur)
		}
	}
}

// TestAggregationConservesTotal: total of all results equals the sum of all
// input values — nothing dropped or double-counted under tumbling windows.
func TestAggregationConservesTotal(t *testing.T) {
	gen := NewGenerator(GeneratorConfig{
		EventsPerSec: 1000, StartMillis: 1_000_000, Seed: 99,
		LateFraction: 0.2, MaxLatenessMs: 3000, OutOfOrderJitterMs: 200,
	})
	events := gen.Generate(5000)

	ag := NewAggregation(NewTumblingAssigner(1000), sumFactory())
	var inputTotal float64
	for _, ev := range events {
		inputTotal += ev.Value
		ag.Add(ev)
	}

	var outputTotal float64
	for _, r := range ag.Results() {
		outputTotal += r.Value
	}

	if diff := outputTotal - inputTotal; diff < -1e-6 || diff > 1e-6 {
		t.Errorf("total not conserved: input %.4f, output %.4f", inputTotal, outputTotal)
	}
}

// TestPipelineRunShape exercises the concurrent wiring: it runs, shuts down on
// context expiry, and returns well-formed, sorted results.
func TestPipelineRunShape(t *testing.T) {
	gen := NewGenerator(GeneratorConfig{
		EventsPerSec: 20000, Seed: 1,
		LateFraction: 0.1, MaxLatenessMs: 1000, OutOfOrderJitterMs: 100,
	})
	const windowMs = 1000
	p := NewPipeline(gen, NewAggregation(NewTumblingAssigner(windowMs), sumFactory()))

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	results := p.Run(ctx) // must return (not hang) after ctx expires
	if len(results) == 0 {
		t.Fatal("pipeline produced no results")
	}
	for _, r := range results {
		if r.Window.End-r.Window.Start != windowMs {
			t.Errorf("result %+v has wrong window width", r)
		}
		if r.Value <= 0 {
			t.Errorf("result %+v has non-positive value", r)
		}
		if floorMod(r.Window.Start, windowMs) != 0 {
			t.Errorf("result %+v window not aligned", r)
		}
	}
}
