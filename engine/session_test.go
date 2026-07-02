package engine

import (
	"context"
	"testing"
	"time"
)

// TestSessionChainWithinGap: events closer than the gap chain into one session
// whose End is the last event's time + gap.
func TestSessionChainWithinGap(t *testing.T) {
	sa := NewSessionAggregation(1000, sumFactory())
	sa.AddEvent(Event{Key: "z", Value: 1, EventTime: 0})
	sa.AddEvent(Event{Key: "z", Value: 2, EventTime: 500})
	sa.AddEvent(Event{Key: "z", Value: 3, EventTime: 1400})

	assertResults(t, sa.Flush(), []WindowResult{
		{Key: "z", Window: Window{0, 2400}, Value: 6},
	})
}

// TestSessionGapBoundaryHalfOpen: an event at exactly lastEvent+gap does NOT
// extend the session — an inactivity gap of exactly gapMs closes it. Half-open
// carried over to sessions.
func TestSessionGapBoundaryHalfOpen(t *testing.T) {
	sa := NewSessionAggregation(1000, sumFactory())
	sa.AddEvent(Event{Key: "z", Value: 1, EventTime: 0})    // session [0, 1000)
	sa.AddEvent(Event{Key: "z", Value: 2, EventTime: 1000}) // touches: NEW session

	assertResults(t, sa.Flush(), []WindowResult{
		{Key: "z", Window: Window{0, 1000}, Value: 1},
		{Key: "z", Window: Window{1000, 2000}, Value: 2},
	})
}

// TestSessionBridgeMergesSessions is THE session test: a late event landing in
// the gap between two separate sessions merges them into one, combining their
// aggregates via Aggregator.Merge.
func TestSessionBridgeMergesSessions(t *testing.T) {
	sa := NewSessionAggregation(1000, sumFactory())
	sa.AddEvent(Event{Key: "z", Value: 10, EventTime: 0})    // session A [0, 1000)
	sa.AddEvent(Event{Key: "z", Value: 20, EventTime: 1500}) // session B [1500, 2500)

	// Out-of-order bridge at 800: proto [800, 1800) overlaps both A and B.
	sa.AddEvent(Event{Key: "z", Value: 5, EventTime: 800})

	assertResults(t, sa.Flush(), []WindowResult{
		{Key: "z", Window: Window{0, 2500}, Value: 35},
	})
}

// TestSessionBridgeMergesAvg: the bridge merge is exact for aggregates with
// state richer than their result (avg carries sum+count through Merge).
func TestSessionBridgeMergesAvg(t *testing.T) {
	sa := NewSessionAggregation(1000, func() Aggregator { return NewAvgAggregator() })
	sa.AddEvent(Event{Key: "z", Value: 10, EventTime: 0})
	sa.AddEvent(Event{Key: "z", Value: 20, EventTime: 1500})
	sa.AddEvent(Event{Key: "z", Value: 30, EventTime: 800}) // bridge

	// avg(10, 20, 30) = 20 — count-weighted, not an average of session averages.
	assertResults(t, sa.Flush(), []WindowResult{
		{Key: "z", Window: Window{0, 2500}, Value: 20},
	})
}

// TestSessionKeysIndependent: sessions never chain or merge across keys.
func TestSessionKeysIndependent(t *testing.T) {
	sa := NewSessionAggregation(1000, sumFactory())
	sa.AddEvent(Event{Key: "a", Value: 1, EventTime: 0})
	sa.AddEvent(Event{Key: "b", Value: 2, EventTime: 500}) // within a's gap, different key

	assertResults(t, sa.Flush(), []WindowResult{
		{Key: "a", Window: Window{0, 1000}, Value: 1},
		{Key: "b", Window: Window{500, 1500}, Value: 2},
	})
}

// TestSessionWatermarkCloses: a watermark closes only sessions it has passed;
// an active session stays open and keeps extending.
func TestSessionWatermarkCloses(t *testing.T) {
	sa := NewSessionAggregation(1000, sumFactory())
	sa.AddEvent(Event{Key: "z", Value: 1, EventTime: 0})    // [0, 1000)
	sa.AddEvent(Event{Key: "z", Value: 2, EventTime: 3000}) // [3000, 4000)

	closed := sa.Advance(Watermark{Timestamp: 2000})
	assertResults(t, closed, []WindowResult{
		{Key: "z", Window: Window{0, 1000}, Value: 1},
	})

	// The open session still extends normally.
	sa.AddEvent(Event{Key: "z", Value: 3, EventTime: 3500})
	assertResults(t, sa.Flush(), []WindowResult{
		{Key: "z", Window: Window{3000, 4500}, Value: 5},
	})
}

// TestSessionLateToSideOutput: an event whose proto-window the watermark has
// fully passed, with no open session to join, is late — routed out, and it must
// not resurrect state.
func TestSessionLateToSideOutput(t *testing.T) {
	sa := NewSessionAggregation(1000, sumFactory())
	sa.AddEvent(Event{Key: "z", Value: 1, EventTime: 0})
	sa.Advance(Watermark{Timestamp: 5000}) // closes & evicts [0, 1000)

	tooLate := Event{Key: "z", Value: 99, EventTime: 200} // proto [200, 1200), End <= 5000
	late := sa.AddEvent(tooLate)
	if len(late) != 1 || late[0] != tooLate {
		t.Fatalf("expected side output, got %+v", late)
	}
	if got := sa.Flush(); len(got) != 0 {
		t.Errorf("late event resurrected state: %+v", got)
	}
}

// TestSessionInGraceJoinsOpenSession: an event older than the watermark still
// folds in when it overlaps a session the watermark hasn't closed — the grace
// the hold-back buys, same rule as fixed windows.
func TestSessionInGraceJoinsOpenSession(t *testing.T) {
	sa := NewSessionAggregation(1000, sumFactory())
	sa.AddEvent(Event{Key: "z", Value: 1, EventTime: 1000}) // session [1000, 2000)
	sa.Advance(Watermark{Timestamp: 1500})                  // session still open (End 2000 > 1500)

	// 1200 < watermark 1500, but it overlaps the open session: folds in.
	if late := sa.AddEvent(Event{Key: "z", Value: 2, EventTime: 1200}); late != nil {
		t.Fatalf("in-grace event wrongly marked late: %+v", late)
	}
	assertResults(t, sa.Flush(), []WindowResult{
		{Key: "z", Window: Window{1000, 2200}, Value: 3},
	})
}

// TestSessionConservation: with generated late/out-of-order traffic, every input
// value lands in exactly one closed session or the side output.
func TestSessionConservation(t *testing.T) {
	gen := NewGenerator(GeneratorConfig{
		EventsPerSec: 1000, StartMillis: 1_000_000, Seed: 7,
		LateFraction: 0.3, MaxLatenessMs: 4000, OutOfOrderJitterMs: 200,
	})
	events := gen.Generate(5000)
	wmgen := NewWatermarkGenerator(500)
	sa := NewSessionAggregation(50, sumFactory()) // small gap: many sessions

	var inputTotal, windowTotal, lateTotal float64
	for _, ev := range events {
		inputTotal += ev.Value
		for _, l := range sa.AddEvent(ev) {
			lateTotal += l.Value
		}
		if wm, ok := wmgen.Observe(ev.EventTime); ok {
			for _, r := range sa.Advance(wm) {
				windowTotal += r.Value
			}
		}
	}
	for _, r := range sa.Flush() {
		windowTotal += r.Value
	}

	if diff := windowTotal + lateTotal - inputTotal; diff < -1e-6 || diff > 1e-6 {
		t.Errorf("not conserved: input %.4f, windows %.4f + late %.4f",
			inputTotal, windowTotal, lateTotal)
	}
}

// TestSessionInvariants: under random traffic the per-key session lists stay
// sorted, non-overlapping, and separated by at least the half-open gap rule.
func TestSessionInvariants(t *testing.T) {
	gen := NewGenerator(GeneratorConfig{
		EventsPerSec: 1000, StartMillis: 1_000_000, Seed: 21,
		LateFraction: 0.2, MaxLatenessMs: 2000, OutOfOrderJitterMs: 300,
	})
	sa := NewSessionAggregation(40, sumFactory())

	for _, ev := range gen.Generate(3000) {
		sa.AddEvent(ev)
		list := sa.sessions[ev.Key]
		for i := 1; i < len(list); i++ {
			if list[i-1].Window.End > list[i].Window.Start {
				t.Fatalf("sessions overlap for %q: %+v then %+v",
					ev.Key, list[i-1].Window, list[i].Window)
			}
		}
	}
}

// TestSessionPipelineEndToEnd: sessions run behind the Processor interface in
// the real concurrent pipeline.
func TestSessionPipelineEndToEnd(t *testing.T) {
	gen := NewGenerator(GeneratorConfig{
		EventsPerSec: 20000, Seed: 1,
		LateFraction: 0.1, MaxLatenessMs: 1000, OutOfOrderJitterMs: 100,
	})
	p := NewPipeline(gen, NewWatermarkGenerator(500), NewSessionAggregation(20, sumFactory()))

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	res := p.Run(ctx)
	if len(res.Windows) == 0 {
		t.Fatal("session pipeline produced no windows")
	}
	for _, r := range res.Windows {
		if r.Window.End-r.Window.Start < 20 {
			t.Errorf("session %+v narrower than the gap", r)
		}
	}
}
