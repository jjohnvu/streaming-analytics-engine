// Command engine runs the streaming analytics pipeline against the synthetic
// load generator and prints per-(zone, window) aggregates.
//
// Run: go run ./cmd/engine
package main

import (
	"context"
	"flag"
	"fmt"
	"time"

	"github.com/jjohnvu/streaming-analytics-engine/engine"
)

func main() {
	var (
		eventsPerSec = flag.Int("eps", 1000, "events per second")
		lateFrac     = flag.Float64("late", 0.05, "fraction of events emitted late")
		maxLate      = flag.Int64("maxlate", 2000, "max lateness in ms")
		jitter       = flag.Int64("jitter", 250, "out-of-order jitter in ms")
		windowMs     = flag.Int64("window", 1000, "tumbling window size in ms")
		lateness     = flag.Int64("lateness", 500, "watermark allowed lateness (holdback) in ms")
		dur          = flag.Duration("dur", 3*time.Second, "how long to run")
	)
	flag.Parse()

	gen := engine.NewGenerator(engine.GeneratorConfig{
		EventsPerSec:       *eventsPerSec,
		LateFraction:       *lateFrac,
		MaxLatenessMs:      *maxLate,
		OutOfOrderJitterMs: *jitter,
	})
	wmgen := engine.NewWatermarkGenerator(*lateness)
	agg := engine.NewAggregation(
		engine.NewTumblingAssigner(*windowMs),
		func() engine.Aggregator { return engine.NewSumAggregator() },
	)
	p := engine.NewPipeline(gen, wmgen, agg)

	ctx, cancel := context.WithTimeout(context.Background(), *dur)
	defer cancel()

	fmt.Printf("running %d ev/s, %.0f%% late, %dms window, %dms allowed lateness, for %s...\n\n",
		*eventsPerSec, *lateFrac*100, *windowMs, *lateness, *dur)

	res := p.Run(ctx)

	fmt.Printf("per-(zone, window) sum of fares:\n")
	for _, r := range res.Windows {
		fmt.Printf("  %-8s [%d, %d)  sum=%.2f\n",
			r.Key, r.Window.Start, r.Window.End, r.Value)
	}
	fmt.Printf("\n%d windows closed, %d events too late (side output)\n",
		len(res.Windows), len(res.Late))
}
