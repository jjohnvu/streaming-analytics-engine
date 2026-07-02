package engine

import "math"

// SessionAggregation implements session windows: variable-size windows defined
// by a gap timeout. Each event opens a proto-window [t, t+gap); overlapping
// sessions for the same key are one session, so consecutive events closer than
// the gap chain together, and a session's End is its last event's time + gap.
//
// The half-open rule carries over: proto-windows that merely *touch* an existing
// session (event exactly at its End) do NOT merge — an inactivity gap of exactly
// gapMs closes a session.
//
// This is where Aggregator.Merge earns its keep: a late event landing in the gap
// between two existing sessions bridges them, and their aggregates are combined
// with Merge. Like Aggregation, sessions close when the watermark passes their
// End, and events that can't join or start any open session go to the side
// output.
//
// Deliberately channel-free (testable deterministically); the Pipeline drives it
// through the Processor interface.
type SessionAggregation struct {
	gapMs     int64
	newAgg    func() Aggregator
	sessions  map[string][]*WindowState // per key, sorted by Start, non-overlapping
	watermark int64
	hasWM     bool
}

// SessionAggregation implements Processor.
var _ Processor = (*SessionAggregation)(nil)

// NewSessionAggregation builds a session-window processor with the given gap
// timeout in milliseconds. Panics on a non-positive gap.
func NewSessionAggregation(gapMs int64, newAgg func() Aggregator) *SessionAggregation {
	if gapMs <= 0 {
		panic("engine: session gap must be positive")
	}
	return &SessionAggregation{
		gapMs:    gapMs,
		newAgg:   newAgg,
		sessions: make(map[string][]*WindowState),
	}
}

// Process consumes one in-band stream element, exactly like Aggregation.Process.
func (sa *SessionAggregation) Process(elem StreamElement) (closed []WindowResult, late []Event) {
	switch e := elem.(type) {
	case Event:
		return nil, sa.AddEvent(e)
	case Watermark:
		return sa.Advance(e), nil
	default:
		return nil, nil
	}
}

// AddEvent folds an event into its key's sessions. The proto-window [t, t+gap)
// either extends/creates a session or — when it overlaps several — merges them
// into one, combining their aggregates with Merge. An event whose proto-window
// overlaps nothing and could no longer be reached by any future session
// (End <= watermark) is late and returned for the side output.
func (sa *SessionAggregation) AddEvent(ev Event) (late []Event) {
	proto := Window{Start: ev.EventTime, End: ev.EventTime + sa.gapMs}
	list := sa.sessions[ev.Key]

	// Locate the run of sessions [i, j) strictly overlapping the proto-window.
	// The list is sorted by Start and non-overlapping, so the run is contiguous.
	i := 0
	for i < len(list) && list[i].Window.End <= proto.Start {
		i++
	}
	j := i
	for j < len(list) && list[j].Window.Start < proto.End {
		j++
	}

	if i == j { // overlaps no session
		if sa.hasWM && proto.End <= sa.watermark {
			// Any session this event could have joined has already closed and
			// been evicted: side output.
			return []Event{ev}
		}
		ws := &WindowState{Window: proto, Key: ev.Key, Agg: sa.newAgg()}
		ws.Agg.Add(ev.Value)
		list = append(list, nil)
		copy(list[i+1:], list[i:])
		list[i] = ws
		sa.sessions[ev.Key] = list
		return nil
	}

	// Fold into the first overlapping session, then absorb the rest of the run —
	// a bridging event merging previously-separate sessions is exactly what
	// Aggregator.Merge exists for.
	target := list[i]
	if proto.Start < target.Window.Start {
		target.Window.Start = proto.Start
	}
	if proto.End > target.Window.End {
		target.Window.End = proto.End
	}
	for _, absorbed := range list[i+1 : j] {
		target.Agg.Merge(absorbed.Agg)
		if absorbed.Window.End > target.Window.End {
			target.Window.End = absorbed.Window.End
		}
	}
	target.Agg.Add(ev.Value)
	sa.sessions[ev.Key] = append(list[:i+1], list[j:]...)
	return nil
}

// Advance moves the watermark forward (never backward) and closes every session
// it has passed, returning their finalized results.
func (sa *SessionAggregation) Advance(wm Watermark) []WindowResult {
	if !sa.hasWM || wm.Timestamp > sa.watermark {
		sa.watermark = wm.Timestamp
		sa.hasWM = true
	}
	return sa.closeSessions(sa.watermark)
}

// Flush closes and evicts every remaining session regardless of the watermark —
// used at end of stream.
func (sa *SessionAggregation) Flush() []WindowResult {
	return sa.closeSessions(math.MaxInt64)
}

// closeSessions finalizes and evicts every session with End <= upTo. Per key the
// list is sorted by Start and non-overlapping — hence sorted by End too — so the
// closable sessions are exactly a prefix.
func (sa *SessionAggregation) closeSessions(upTo int64) []WindowResult {
	var closed []WindowResult
	for key, list := range sa.sessions {
		n := 0
		for n < len(list) && list[n].Window.End <= upTo {
			closed = append(closed, WindowResult{
				Key:    key,
				Window: list[n].Window,
				Value:  list[n].Agg.Result(),
			})
			n++
		}
		switch {
		case n == len(list):
			delete(sa.sessions, key)
		case n > 0:
			sa.sessions[key] = list[n:]
		}
	}
	sortResults(closed)
	return closed
}
