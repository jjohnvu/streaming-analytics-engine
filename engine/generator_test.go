package engine

import (
	"context"
	"math"
	"testing"
	"time"
)

// baseCfg gives a clean logical interval of exactly 1ms (EventsPerSec=1000) and
// a pinned epoch, so a test can recompute the frontier for event i as start+i.
func baseCfg() GeneratorConfig {
	return GeneratorConfig{
		EventsPerSec: 1000,
		StartMillis:  1_000_000,
		Seed:         42,
		Keys:         []string{"zone-a", "zone-b", "zone-c"},
	}
}

// TestGenerateDeterministic: same config + seed yields identical streams.
func TestGenerateDeterministic(t *testing.T) {
	cfg := baseCfg()
	cfg.LateFraction = 0.3
	cfg.MaxLatenessMs = 500
	cfg.OutOfOrderJitterMs = 5

	a := NewGenerator(cfg).Generate(1000)
	b := NewGenerator(cfg).Generate(1000)

	for i := range a {
		if a[i] != b[i] {
			t.Fatalf("event %d differs across runs: %+v vs %+v", i, a[i], b[i])
		}
	}
}

// TestPerfectStream: with no lateness and no jitter, event time is exactly the
// frontier and the stream is strictly monotonic.
func TestPerfectStream(t *testing.T) {
	cfg := baseCfg() // LateFraction=0, jitter=0
	g := NewGenerator(cfg)
	events := g.Generate(100)

	for i, ev := range events {
		want := cfg.StartMillis + int64(i)
		if ev.EventTime != want {
			t.Errorf("event %d: EventTime = %d, want %d", i, ev.EventTime, want)
		}
	}
}

// TestJitterBounded: with only jitter, every event stays within +/- jitter of
// its frontier — out of order, but bounded.
func TestJitterBounded(t *testing.T) {
	cfg := baseCfg()
	cfg.OutOfOrderJitterMs = 5
	g := NewGenerator(cfg)
	events := g.Generate(2000)

	for i, ev := range events {
		f := cfg.StartMillis + int64(i)
		if d := ev.EventTime - f; d < -5 || d > 5 {
			t.Errorf("event %d: deviation %d exceeds jitter bound +/-5", i, d)
		}
	}
}

// TestAllLate: LateFraction=1 with no jitter means every event sits behind its
// frontier by [1, MaxLatenessMs].
func TestAllLate(t *testing.T) {
	cfg := baseCfg()
	cfg.LateFraction = 1.0
	cfg.MaxLatenessMs = 300
	g := NewGenerator(cfg)
	events := g.Generate(2000)

	for i, ev := range events {
		f := cfg.StartMillis + int64(i)
		lateness := f - ev.EventTime
		if lateness < 1 || lateness > 300 {
			t.Errorf("event %d: lateness %d outside [1,300]", i, lateness)
		}
	}
}

// TestLateFractionProportion: with jitter off and MaxLateness>0, a late event
// is exactly those stamped behind their frontier. Over many events the observed
// fraction should track LateFraction.
func TestLateFractionProportion(t *testing.T) {
	const n = 20000
	cfg := baseCfg()
	cfg.LateFraction = 0.25
	cfg.MaxLatenessMs = 1000 // >> interval, so late events are unambiguous
	g := NewGenerator(cfg)
	events := g.Generate(n)

	late := 0
	for i, ev := range events {
		f := cfg.StartMillis + int64(i)
		if ev.EventTime < f {
			late++
		}
	}

	got := float64(late) / float64(n)
	if math.Abs(got-0.25) > 0.02 {
		t.Errorf("late fraction = %.3f, want ~0.25 (+/-0.02)", got)
	}
}

// TestKeysAndValues: events only use configured keys, and values land in the
// fare range [1, 100).
func TestKeysAndValues(t *testing.T) {
	cfg := baseCfg()
	g := NewGenerator(cfg)
	events := g.Generate(1000)

	allowed := map[string]bool{"zone-a": true, "zone-b": true, "zone-c": true}
	for i, ev := range events {
		if !allowed[ev.Key] {
			t.Errorf("event %d: unexpected key %q", i, ev.Key)
		}
		if ev.Value < 1 || ev.Value >= 100 {
			t.Errorf("event %d: value %v outside [1,100)", i, ev.Value)
		}
	}
}

// TestDefaultKeys: an empty Keys config falls back to the zone defaults.
func TestDefaultKeys(t *testing.T) {
	g := NewGenerator(GeneratorConfig{StartMillis: 1, Seed: 1})
	events := g.Generate(200)
	allowed := map[string]bool{}
	for _, k := range defaultZoneKeys {
		allowed[k] = true
	}
	for i, ev := range events {
		if !allowed[ev.Key] {
			t.Errorf("event %d: key %q not in default zones", i, ev.Key)
		}
	}
}

// TestRunStreamsAndStops: Run delivers events and closes the channel promptly
// when the context is cancelled.
func TestRunStreamsAndStops(t *testing.T) {
	cfg := baseCfg()
	cfg.EventsPerSec = 10000 // fast pacing so the test is quick
	g := NewGenerator(cfg)

	ctx, cancel := context.WithCancel(context.Background())
	ch := g.Run(ctx)

	// Read a few events to confirm the stream is live.
	for i := 0; i < 5; i++ {
		select {
		case _, ok := <-ch:
			if !ok {
				t.Fatal("channel closed before producing events")
			}
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for events")
		}
	}

	cancel()

	// After cancel, the channel must close (drain any in-flight event first).
	deadline := time.After(time.Second)
	for {
		select {
		case _, ok := <-ch:
			if !ok {
				return // closed as expected
			}
		case <-deadline:
			t.Fatal("channel not closed after cancel")
		}
	}
}
