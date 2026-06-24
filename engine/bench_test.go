package engine

import "testing"

// BenchmarkAggregationAdd measures the aggregation hot path: assign + map lookup
// + fold, the per-event work the engine does at steady state.
func BenchmarkAggregationAdd(b *testing.B) {
	gen := NewGenerator(GeneratorConfig{
		EventsPerSec: 1000, Seed: 1,
		LateFraction: 0.1, MaxLatenessMs: 2000, OutOfOrderJitterMs: 250,
	})
	events := gen.Generate(b.N)
	ag := NewAggregation(NewTumblingAssigner(1000), func() Aggregator { return NewSumAggregator() })

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ag.AddEvent(events[i])
	}
}
