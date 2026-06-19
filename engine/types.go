// Package engine contains the core stream-processing primitives: the data model
// (Event, Watermark, Window, WindowState) and the aggregation contract. These
// are the shared vocabulary every pipeline stage speaks.
package engine

// Event is a single timestamped observation flowing through the engine. Kept
// deliberately minimal — every field must earn its place (see CONTEXT.md).
type Event struct {
	Key       string  // grouping dimension, e.g. "zone-3"
	Value     float64 // the thing being aggregated (fare, delivery time, ...)
	EventTime int64   // when it happened, unix millis — the clock that matters
}

// Watermark is a control signal travelling in-band with events: a promise that
// no event with EventTime < Timestamp should still arrive. When a watermark
// flows downstream, every window it has passed can be closed.
type Watermark struct {
	Timestamp int64 // "no event with EventTime < this should arrive"
}

// Window is a half-open interval [Start, End) over event time. Half-open is
// mandatory: it's what stops a boundary event from being counted in two
// adjacent windows.
type Window struct {
	Start int64 // inclusive
	End   int64 // exclusive
}

// Contains reports whether event time t falls in this window, honoring the
// half-open rule [Start, End). An event at exactly End belongs to the *next*
// window, not this one. Centralizing the comparison here keeps every assigner
// consistent.
func (w Window) Contains(t int64) bool {
	return t >= w.Start && t < w.End
}

// WindowState is the per-(key, window) running aggregate the engine maintains.
// The aggregate is held behind the Aggregator interface so the same state works
// for sum, avg, min/max, count, etc.
type WindowState struct {
	Window Window
	Key    string
	Agg    Aggregator
}
