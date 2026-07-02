// Command engine runs the streaming analytics pipeline against the synthetic
// load generator and prints per-(zone, window) aggregates.
//
// Run: go run ./cmd/engine
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/jjohnvu/streaming-analytics-engine/engine"
)

// aggFactories maps the -agg flag to aggregate constructors.
var aggFactories = map[string]func() engine.Aggregator{
	"sum":   func() engine.Aggregator { return engine.NewSumAggregator() },
	"avg":   func() engine.Aggregator { return engine.NewAvgAggregator() },
	"min":   func() engine.Aggregator { return engine.NewMinAggregator() },
	"max":   func() engine.Aggregator { return engine.NewMaxAggregator() },
	"count": func() engine.Aggregator { return engine.NewCountAggregator() },
}

func main() {
	var (
		eventsPerSec = flag.Int("eps", 1000, "events per second")
		lateFrac     = flag.Float64("late", 0.05, "fraction of events emitted late")
		maxLate      = flag.Int64("maxlate", 2000, "max lateness in ms")
		jitter       = flag.Int64("jitter", 250, "out-of-order jitter in ms")
		windows      = flag.String("windows", "tumbling", "window type: tumbling | sliding | session")
		windowMs     = flag.Int64("window", 1000, "window size in ms (tumbling/sliding)")
		slideMs      = flag.Int64("slide", 500, "slide interval in ms (sliding)")
		gapMs        = flag.Int64("gap", 2000, "session inactivity gap in ms (session)")
		aggName      = flag.String("agg", "sum", "aggregation: sum | avg | min | max | count")
		lateness     = flag.Int64("lateness", 500, "watermark allowed lateness (holdback) in ms")
		dur          = flag.Duration("dur", 3*time.Second, "how long to run")
	)
	flag.Parse()

	newAgg, ok := aggFactories[*aggName]
	if !ok {
		fmt.Fprintf(os.Stderr, "unknown -agg %q (want sum|avg|min|max|count)\n", *aggName)
		os.Exit(2)
	}

	var proc engine.Processor
	var windowDesc string
	switch *windows {
	case "tumbling":
		proc = engine.NewAggregation(engine.NewTumblingAssigner(*windowMs), newAgg)
		windowDesc = fmt.Sprintf("%dms tumbling windows", *windowMs)
	case "sliding":
		proc = engine.NewAggregation(engine.NewSlidingAssigner(*windowMs, *slideMs), newAgg)
		windowDesc = fmt.Sprintf("%dms sliding windows every %dms", *windowMs, *slideMs)
	case "session":
		proc = engine.NewSessionAggregation(*gapMs, newAgg)
		windowDesc = fmt.Sprintf("session windows (%dms gap)", *gapMs)
	default:
		fmt.Fprintf(os.Stderr, "unknown -windows %q (want tumbling|sliding|session)\n", *windows)
		os.Exit(2)
	}

	gen := engine.NewGenerator(engine.GeneratorConfig{
		EventsPerSec:       *eventsPerSec,
		LateFraction:       *lateFrac,
		MaxLatenessMs:      *maxLate,
		OutOfOrderJitterMs: *jitter,
	})
	wmgen := engine.NewWatermarkGenerator(*lateness)
	p := engine.NewPipeline(gen, wmgen, proc)

	ctx, cancel := context.WithTimeout(context.Background(), *dur)
	defer cancel()

	fmt.Printf("running %d ev/s, %.0f%% late, %s, %dms allowed lateness, for %s...\n\n",
		*eventsPerSec, *lateFrac*100, windowDesc, *lateness, *dur)

	res := p.Run(ctx)

	fmt.Printf("per-(zone, window) %s of fares:\n", *aggName)
	for _, r := range res.Windows {
		fmt.Printf("  %-8s [%d, %d)  %s=%.2f\n",
			r.Key, r.Window.Start, r.Window.End, *aggName, r.Value)
	}
	fmt.Printf("\n%d windows closed, %d events too late (side output)\n",
		len(res.Windows), len(res.Late))
}
