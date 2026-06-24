package engine

import "context"

// PipelineResult is what a run produces: the finalized window aggregates and the
// late events that arrived after their window had closed (the side output).
type PipelineResult struct {
	Windows []WindowResult
	Late    []Event
}

// Pipeline wires the stages together with goroutines and channels:
//
//	Source (generator) → Ingestion (+ watermark gen) → Aggregator → Sink
//	                                                          │
//	                                                    side output (late data)
//
// The Ingestion stage now emits a unified stream of StreamElements — events with
// watermarks interleaved in-band — so the aggregator sees watermarks in order
// relative to the events. The Sink is the final read-out of closed windows; late
// events are collected separately as the side output.
type Pipeline struct {
	gen   *Generator
	wmgen *WatermarkGenerator
	agg   *Aggregation
}

// NewPipeline connects a load generator, a watermark generator, and an
// aggregation stage.
func NewPipeline(gen *Generator, wmgen *WatermarkGenerator, agg *Aggregation) *Pipeline {
	return &Pipeline{gen: gen, wmgen: wmgen, agg: agg}
}

// Run drives the pipeline until ctx is cancelled, then flushes any windows the
// watermark never reached and returns the finalized aggregates plus the side
// output. Each stage is its own goroutine connected by a channel; cancellation
// propagates from the source outward, so the pipeline drains and shuts down
// cleanly.
func (p *Pipeline) Run(ctx context.Context) PipelineResult {
	source := p.gen.Run(ctx)
	stream := p.ingest(ctx, source)

	var res PipelineResult
	for elem := range stream {
		closed, late := p.agg.Process(elem)
		res.Windows = append(res.Windows, closed...)
		res.Late = append(res.Late, late...)
	}
	res.Windows = append(res.Windows, p.agg.Flush()...)
	sortResults(res.Windows)
	return res
}

// ingest is the Ingestion stage: it forwards each event downstream and, right
// after, emits a watermark in-band whenever the watermark generator advances.
// Emitting the watermark immediately after the event that moved it keeps
// watermarks correctly ordered within the stream.
func (p *Pipeline) ingest(ctx context.Context, in <-chan Event) <-chan StreamElement {
	out := make(chan StreamElement)
	go func() {
		defer close(out)
		for ev := range in {
			select {
			case <-ctx.Done():
				return
			case out <- ev:
			}
			if wm, ok := p.wmgen.Observe(ev.EventTime); ok {
				select {
				case <-ctx.Done():
					return
				case out <- wm:
				}
			}
		}
	}()
	return out
}
