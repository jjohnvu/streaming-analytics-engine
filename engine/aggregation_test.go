package engine

import (
	"context"
	"testing"
	"time"
)

func sumFactory() func() Aggregator {
	return func() Aggregator { return NewSumAggregator() }
}

// TestAggregationSumsByKeyWindow folds events with no watermarks, then flushes —
// checking the pure per-(key, window) sum independent of window closing.
func TestAggregationSumsByKeyWindow(t *testing.T) {
	ag := NewAggregation(NewTumblingAssigner(1000), sumFactory())

	ag.AddEvent(Event{Key: "zone-a", Value: 10, EventTime: 500})
	ag.AddEvent(Event{Key: "zone-a", Value: 5, EventTime: 700})
	ag.AddEvent(Event{Key: "zone-a", Value: 3, EventTime: 1500}) // next window
	ag.AddEvent(Event{Key: "zone-b", Value: 2, EventTime: 500})

	want := []WindowResult{
		{Key: "zone-a", Window: Window{0, 1000}, Value: 15},
		{Key: "zone-a", Window: Window{1000, 2000}, Value: 3},
		{Key: "zone-b", Window: Window{0, 1000}, Value: 2},
	}

	got := ag.Flush() // closes everything at end of stream
	assertResults(t, got, want)
}

// TestWatermarkClosesPassedWindows: a watermark closes only the windows it has
// passed (End <= watermark); later windows stay open in state.
func TestWatermarkClosesPassedWindows(t *testing.T) {
	ag := NewAggregation(NewTumblingAssigner(1000), sumFactory())
	ag.AddEvent(Event{Key: "z", Value: 10, EventTime: 500}) // window [0,1000)
	ag.AddEvent(Event{Key: "z", Value: 7, EventTime: 1500}) // window [1000,2000)

	// Watermark at 1000 closes [0,1000) (End 1000 <= 1000) but not [1000,2000).
	closed := ag.Advance(Watermark{Timestamp: 1000})
	assertResults(t, closed, []WindowResult{
		{Key: "z", Window: Window{0, 1000}, Value: 10},
	})

	// The later window is still open: flushing now yields it.
	rest := ag.Flush()
	assertResults(t, rest, []WindowResult{
		{Key: "z", Window: Window{1000, 2000}, Value: 7},
	})
}

// TestEvictionFreesState: a closed window is removed from state, so re-closing
// or flushing doesn't re-emit it.
func TestEvictionFreesState(t *testing.T) {
	ag := NewAggregation(NewTumblingAssigner(1000), sumFactory())
	ag.AddEvent(Event{Key: "z", Value: 4, EventTime: 200})
	ag.Advance(Watermark{Timestamp: 5000}) // closes & evicts [0,1000)

	if n := len(ag.state); n != 0 {
		t.Fatalf("state not evicted: %d entries remain", n)
	}
	if got := ag.Flush(); len(got) != 0 {
		t.Errorf("flush re-emitted evicted window: %+v", got)
	}
}

// TestLateEventToSideOutput: an event for a window that already closed is
// returned as late, not folded.
func TestLateEventToSideOutput(t *testing.T) {
	ag := NewAggregation(NewTumblingAssigner(1000), sumFactory())
	ag.AddEvent(Event{Key: "z", Value: 1, EventTime: 100})
	ag.Advance(Watermark{Timestamp: 1000}) // closes [0,1000)

	tooLate := Event{Key: "z", Value: 99, EventTime: 200} // belongs to [0,1000)
	late := ag.AddEvent(tooLate)

	if len(late) != 1 || late[0] != tooLate {
		t.Fatalf("expected event routed to side output, got %+v", late)
	}
	// And it must not resurrect or affect any window.
	if got := ag.Flush(); len(got) != 0 {
		t.Errorf("late event leaked into state: %+v", got)
	}
}

// TestLateButInGraceFolds: an out-of-order event whose window is still open
// (watermark held back below the window end) is folded normally — this is the
// allowed-lateness grace the holdback buys.
func TestLateButInGraceFolds(t *testing.T) {
	ag := NewAggregation(NewTumblingAssigner(1000), sumFactory())
	ag.AddEvent(Event{Key: "z", Value: 10, EventTime: 900}) // window [0,1000)

	// Watermark advances to 950: behind the window end (1000), so [0,1000) is
	// still open.
	if closed := ag.Advance(Watermark{Timestamp: 950}); len(closed) != 0 {
		t.Fatalf("window closed too early: %+v", closed)
	}
	// An older event (800 < 950 watermark) still lands in the open window.
	if late := ag.AddEvent(Event{Key: "z", Value: 5, EventTime: 800}); late != nil {
		t.Fatalf("in-grace event wrongly marked late: %+v", late)
	}
	assertResults(t, ag.Flush(), []WindowResult{
		{Key: "z", Window: Window{0, 1000}, Value: 15},
	})
}

// TestProcessDispatch: Process routes events and watermarks to the right path.
func TestProcessDispatch(t *testing.T) {
	ag := NewAggregation(NewTumblingAssigner(1000), sumFactory())

	if closed, late := ag.Process(Event{Key: "z", Value: 3, EventTime: 100}); closed != nil || late != nil {
		t.Errorf("on-time event should produce nothing, got closed=%v late=%v", closed, late)
	}
	closed, late := ag.Process(Watermark{Timestamp: 5000})
	if late != nil {
		t.Errorf("watermark should not produce late events")
	}
	assertResults(t, closed, []WindowResult{
		{Key: "z", Window: Window{0, 1000}, Value: 3},
	})
}

// TestConservationWithSideOutput: every input value ends up either in a closed
// window or in the side output — nothing is lost.
func TestConservationWithSideOutput(t *testing.T) {
	gen := NewGenerator(GeneratorConfig{
		EventsPerSec: 1000, StartMillis: 1_000_000, Seed: 99,
		LateFraction: 0.3, MaxLatenessMs: 4000, OutOfOrderJitterMs: 200,
	})
	events := gen.Generate(5000)
	wmgen := NewWatermarkGenerator(500)
	ag := NewAggregation(NewTumblingAssigner(1000), sumFactory())

	var inputTotal, windowTotal, lateTotal float64
	for _, ev := range events {
		inputTotal += ev.Value
		late := ag.AddEvent(ev)
		for _, l := range late {
			lateTotal += l.Value
		}
		if wm, ok := wmgen.Observe(ev.EventTime); ok {
			for _, r := range ag.Advance(wm) {
				windowTotal += r.Value
			}
		}
	}
	for _, r := range ag.Flush() {
		windowTotal += r.Value
	}

	if diff := windowTotal + lateTotal - inputTotal; diff < -1e-6 || diff > 1e-6 {
		t.Errorf("not conserved: input %.4f, windows %.4f + late %.4f = %.4f",
			inputTotal, windowTotal, lateTotal, windowTotal+lateTotal)
	}
	if lateTotal == 0 {
		t.Error("expected some late events given 30%% lateness and 500ms holdback")
	}
}

// TestPipelineRunShape exercises the concurrent wiring end to end.
func TestPipelineRunShape(t *testing.T) {
	gen := NewGenerator(GeneratorConfig{
		EventsPerSec: 20000, Seed: 1,
		LateFraction: 0.1, MaxLatenessMs: 1000, OutOfOrderJitterMs: 100,
	})
	const windowMs = 1000
	p := NewPipeline(
		gen,
		NewWatermarkGenerator(500),
		NewAggregation(NewTumblingAssigner(windowMs), sumFactory()),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	res := p.Run(ctx) // must return (not hang) after ctx expires
	if len(res.Windows) == 0 {
		t.Fatal("pipeline produced no windows")
	}
	for _, r := range res.Windows {
		if r.Window.End-r.Window.Start != windowMs {
			t.Errorf("result %+v has wrong window width", r)
		}
		if floorMod(r.Window.Start, windowMs) != 0 {
			t.Errorf("result %+v window not aligned", r)
		}
	}
}

// assertResults compares result slices element-by-element.
func assertResults(t *testing.T, got, want []WindowResult) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("got %d results, want %d: %+v", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("result %d = %+v, want %+v", i, got[i], want[i])
		}
	}
}
