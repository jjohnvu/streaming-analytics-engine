package engine

import (
	"math"
	"sort"
)

// WindowResult is a finalized per-(key, window) aggregate, ready for the sink.
type WindowResult struct {
	Key    string
	Window Window
	Value  float64
}

// stateKey identifies one aggregate cell. Both fields are comparable, so this
// works directly as a map key.
type stateKey struct {
	Key    string
	Window Window
}

// Aggregation is the engine's stateful core. It folds events into per-(key,
// window) aggregates and reacts to watermarks: when a watermark passes a
// window's end, that window is finalized (its result emitted) and evicted, which
// is also what keeps the state map from growing without bound.
//
// Lateness is handled by the watermark holdback (see WatermarkGenerator): a
// window stays open — and keeps accepting late, out-of-order events — until the
// watermark reaches its end. An event whose window has already closed is *late*:
// it is never silently dropped, but returned for routing to the side output.
//
// It is deliberately channel-free so it can be tested deterministically; the
// Pipeline wraps it in goroutines.
type Aggregation struct {
	assigner  WindowAssigner
	newAgg    func() Aggregator
	state     map[stateKey]*WindowState
	watermark int64
	hasWM     bool
}

// Aggregation implements Processor.
var _ Processor = (*Aggregation)(nil)

// NewAggregation builds an Aggregation over the given assigner. newAgg mints a
// fresh aggregate for each new (key, window) cell.
func NewAggregation(assigner WindowAssigner, newAgg func() Aggregator) *Aggregation {
	return &Aggregation{
		assigner: assigner,
		newAgg:   newAgg,
		state:    make(map[stateKey]*WindowState),
	}
}

// Process consumes one in-band stream element: an Event is folded (or returned
// as late), a Watermark closes and evicts every window it has passed. The two
// return slices are the finalized window results (the sink) and the late events
// (the side output); both are nil in the common per-event case, so steady-state
// processing doesn't allocate.
func (ag *Aggregation) Process(elem StreamElement) (closed []WindowResult, late []Event) {
	switch e := elem.(type) {
	case Event:
		return nil, ag.AddEvent(e)
	case Watermark:
		return ag.Advance(e), nil
	default:
		return nil, nil
	}
}

// AddEvent folds an event into each window it belongs to whose window is still
// open. For any assigned window that has already closed (end <= current
// watermark), the event is late and returned for the side output instead.
func (ag *Aggregation) AddEvent(ev Event) (late []Event) {
	for _, w := range ag.assigner.Assign(ev.EventTime) {
		if ag.hasWM && w.End <= ag.watermark {
			late = append(late, ev) // window already closed & evicted
			continue
		}
		k := stateKey{Key: ev.Key, Window: w}
		ws := ag.state[k]
		if ws == nil {
			ws = &WindowState{Window: w, Key: ev.Key, Agg: ag.newAgg()}
			ag.state[k] = ws
		}
		ws.Agg.Add(ev.Value)
	}
	return late
}

// Advance moves the watermark forward (never backward) and closes every window
// the new watermark has passed, returning their finalized results.
func (ag *Aggregation) Advance(wm Watermark) []WindowResult {
	if !ag.hasWM || wm.Timestamp > ag.watermark {
		ag.watermark = wm.Timestamp
		ag.hasWM = true
	}
	return ag.closeWindows(ag.watermark)
}

// Flush closes and evicts every remaining window regardless of the watermark —
// used at end of stream so the most recent windows (which the watermark never
// reached) still produce results.
func (ag *Aggregation) Flush() []WindowResult {
	return ag.closeWindows(math.MaxInt64)
}

// closeWindows finalizes and evicts every window with End <= upTo, returning the
// results sorted by key then window start for deterministic output.
func (ag *Aggregation) closeWindows(upTo int64) []WindowResult {
	var closed []WindowResult
	for k, ws := range ag.state {
		if ws.Window.End <= upTo {
			closed = append(closed, WindowResult{
				Key:    ws.Key,
				Window: ws.Window,
				Value:  ws.Agg.Result(),
			})
			delete(ag.state, k)
		}
	}
	sortResults(closed)
	return closed
}

// sortResults orders results by key, then by window start.
func sortResults(r []WindowResult) {
	sort.Slice(r, func(i, j int) bool {
		if r[i].Key != r[j].Key {
			return r[i].Key < r[j].Key
		}
		return r[i].Window.Start < r[j].Window.Start
	})
}
