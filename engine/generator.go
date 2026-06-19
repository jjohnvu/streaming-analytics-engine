package engine

import (
	"context"
	"math/rand"
	"time"
)

// defaultZoneKeys are the grouping keys used when a GeneratorConfig leaves Keys
// empty — eight city zones, matching the ride/delivery domain in CONTEXT.md.
var defaultZoneKeys = []string{
	"zone-0", "zone-1", "zone-2", "zone-3",
	"zone-4", "zone-5", "zone-6", "zone-7",
}

// GeneratorConfig configures the synthetic load. The first four fields are the
// mandated knobs (CONTEXT.md): they're what manufacture the late / out-of-order
// conditions the watermark machinery exists to handle. The rest are supporting
// infrastructure with sensible defaults.
type GeneratorConfig struct {
	// --- the four mandated knobs ---

	EventsPerSec       int     // target throughput; <= 0 means emit as fast as possible
	LateFraction       float64 // [0,1]: fraction of events emitted deliberately late
	MaxLatenessMs      int64   // upper bound on how far behind the frontier a late event sits
	OutOfOrderJitterMs int64   // +/- wobble applied to non-late events' event-time

	// --- supporting config (defaulted) ---

	Keys        []string // grouping keys to sample from; defaults to defaultZoneKeys
	Seed        int64    // RNG seed, for reproducible streams
	StartMillis int64    // logical event-time epoch; 0 means "now" at construction
}

// Generator produces a synthetic Event stream. It is NOT safe for concurrent
// use: a single Generator drives a single goroutine (Run) or a single caller
// (Generate).
type Generator struct {
	cfg        GeneratorConfig
	rng        *rand.Rand
	start      int64   // logical event-time epoch (unix millis)
	i          int64   // count of events produced so far
	intervalMs float64 // logical event-time advance per event
}

// NewGenerator builds a Generator from cfg, filling in defaults. The logical
// interval (how much the event-time frontier advances per event) derives from
// EventsPerSec; when EventsPerSec <= 0 it falls back to 1ms so event time still
// advances when running unthrottled.
func NewGenerator(cfg GeneratorConfig) *Generator {
	if len(cfg.Keys) == 0 {
		cfg.Keys = defaultZoneKeys
	}
	start := cfg.StartMillis
	if start == 0 {
		start = time.Now().UnixMilli()
	}
	interval := 1.0
	if cfg.EventsPerSec > 0 {
		interval = 1000.0 / float64(cfg.EventsPerSec)
	}
	return &Generator{
		cfg:        cfg,
		rng:        rand.New(rand.NewSource(cfg.Seed)),
		start:      start,
		intervalMs: interval,
	}
}

// frontier returns the steadily-advancing logical event time for the i-th event
// (0-based). This is the "true" time the event would have if it were perfectly
// on-time and in-order; jitter and lateness are applied relative to it.
func (g *Generator) frontier(i int64) int64 {
	return g.start + int64(float64(i)*g.intervalMs)
}

// next produces the next event, advancing the internal counter. Exactly one of
// two perturbations is applied:
//   - with probability LateFraction, the event is stamped behind the frontier
//     by [1, MaxLatenessMs] — a deliberate straggler the watermark path must
//     handle.
//   - otherwise jitter of +/- OutOfOrderJitterMs makes even on-time events
//     slightly out of order.
func (g *Generator) next() Event {
	f := g.frontier(g.i)
	g.i++

	var eventTime int64
	switch {
	case g.cfg.MaxLatenessMs > 0 && g.rng.Float64() < g.cfg.LateFraction:
		lateness := 1 + g.rng.Int63n(g.cfg.MaxLatenessMs)
		eventTime = f - lateness
	case g.cfg.OutOfOrderJitterMs > 0:
		jitter := g.rng.Int63n(2*g.cfg.OutOfOrderJitterMs+1) - g.cfg.OutOfOrderJitterMs
		eventTime = f + jitter
	default:
		eventTime = f
	}

	return Event{
		Key:       g.cfg.Keys[g.rng.Intn(len(g.cfg.Keys))],
		Value:     1 + g.rng.Float64()*99, // a fare in [1, 100)
		EventTime: eventTime,
	}
}

// Generate returns n events synchronously, unthrottled. Deterministic for a
// fixed Seed — ideal for tests and for benchmarking the engine without the
// generator's pacing getting in the way.
func (g *Generator) Generate(n int) []Event {
	out := make([]Event, n)
	for k := 0; k < n; k++ {
		out[k] = g.next()
	}
	return out
}

// Run streams events on the returned channel until ctx is cancelled, then
// closes the channel. When EventsPerSec > 0 emission is paced in real
// (processing) time with a ticker; otherwise events are produced as fast as the
// consumer reads them. Pacing controls only *when* events are emitted — their
// stamped EventTime always comes from the logical frontier.
func (g *Generator) Run(ctx context.Context) <-chan Event {
	out := make(chan Event)
	go func() {
		defer close(out)

		var tick <-chan time.Time
		if g.cfg.EventsPerSec > 0 {
			t := time.NewTicker(time.Second / time.Duration(g.cfg.EventsPerSec))
			defer t.Stop()
			tick = t.C
		}

		for {
			if tick != nil {
				select {
				case <-ctx.Done():
					return
				case <-tick:
				}
			} else if ctx.Err() != nil {
				return
			}

			select {
			case <-ctx.Done():
				return
			case out <- g.next():
			}
		}
	}()
	return out
}
