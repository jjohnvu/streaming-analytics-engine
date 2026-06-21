package engine

import "context"

// Pipeline wires the stages together with goroutines and channels:
//
//	Source (generator) → Ingestion → Aggregator → Sink
//
// In the MVP, Ingestion is a thin forwarding stage — its real job (hosting the
// watermark generator) arrives with the watermark milestone, but it's kept as a
// distinct stage now so that wiring is already in place. The Sink is the final
// read-out of accumulated aggregates when the stream ends.
type Pipeline struct {
	gen *Generator
	agg *Aggregation
}

// NewPipeline connects a load generator to an aggregation stage.
func NewPipeline(gen *Generator, agg *Aggregation) *Pipeline {
	return &Pipeline{gen: gen, agg: agg}
}

// Run drives the pipeline until ctx is cancelled, then returns the finalized
// per-(key, window) aggregates. Each stage is its own goroutine connected by a
// channel; cancellation propagates from the source outward, so the whole
// pipeline drains and shuts down cleanly.
func (p *Pipeline) Run(ctx context.Context) []WindowResult {
	source := p.gen.Run(ctx) // Source: paced synthetic events
	ingested := p.ingest(ctx, source)

	// Aggregator + Sink: fold every event, then read out results once the
	// upstream channel closes (on cancellation).
	for ev := range ingested {
		p.agg.Add(ev)
	}
	return p.agg.Results()
}

// ingest is the Ingestion stage: today it simply forwards events downstream.
// It's the natural home for the watermark generator later (tracking max event
// time and emitting watermarks in-band on this same channel).
func (p *Pipeline) ingest(ctx context.Context, in <-chan Event) <-chan Event {
	out := make(chan Event)
	go func() {
		defer close(out)
		for ev := range in {
			select {
			case <-ctx.Done():
				return
			case out <- ev:
			}
		}
	}()
	return out
}
