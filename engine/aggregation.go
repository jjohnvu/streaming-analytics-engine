package engine

import "sort"

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

// Aggregation is the engine's stateful core: for each event it asks the assigner
// which window(s) the event belongs to and folds the event's value into the
// running aggregate for each (key, window). It's deliberately channel-free so it
// can be tested deterministically; the Pipeline wraps it in goroutines.
//
// NOTE: in the MVP there is no window closing — state accumulates for the life
// of the run and is read out at the end. Watermark-driven closing and eviction
// land with the watermark milestone.
type Aggregation struct {
	assigner WindowAssigner
	newAgg   func() Aggregator
	state    map[stateKey]*WindowState
}

// NewAggregation builds an Aggregation over the given assigner. newAgg mints a
// fresh aggregate for each new (key, window) cell — passing a factory (rather
// than a single Aggregator) is what keeps per-cell state independent and lets
// the same pipeline run sum, avg, etc.
func NewAggregation(assigner WindowAssigner, newAgg func() Aggregator) *Aggregation {
	return &Aggregation{
		assigner: assigner,
		newAgg:   newAgg,
		state:    make(map[stateKey]*WindowState),
	}
}

// Add folds one event into every window it belongs to.
func (ag *Aggregation) Add(ev Event) {
	for _, w := range ag.assigner.Assign(ev.EventTime) {
		k := stateKey{Key: ev.Key, Window: w}
		ws := ag.state[k]
		if ws == nil {
			ws = &WindowState{Window: w, Key: ev.Key, Agg: ag.newAgg()}
			ag.state[k] = ws
		}
		ws.Agg.Add(ev.Value)
	}
}

// Results returns every current aggregate, sorted by key then window start, so
// output is stable and readable.
func (ag *Aggregation) Results() []WindowResult {
	out := make([]WindowResult, 0, len(ag.state))
	for _, ws := range ag.state {
		out = append(out, WindowResult{
			Key:    ws.Key,
			Window: ws.Window,
			Value:  ws.Agg.Result(),
		})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Key != out[j].Key {
			return out[i].Key < out[j].Key
		}
		return out[i].Window.Start < out[j].Window.Start
	})
	return out
}
