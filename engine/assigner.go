package engine

// WindowAssigner maps an event time to the window(s) it belongs to. Tumbling
// returns exactly one window; sliding may return several; session windows (which
// depend on surrounding events, not just a single timestamp) will extend this
// contract later.
type WindowAssigner interface {
	Assign(eventTime int64) []Window
}

// TumblingAssigner assigns each event to exactly one fixed-size, non-overlapping
// window aligned to the epoch: window starts fall on multiples of sizeMs.
type TumblingAssigner struct {
	sizeMs int64
}

// NewTumblingAssigner returns a tumbling assigner with the given window size in
// milliseconds. Panics on a non-positive size — that's a programming error, not
// runtime data.
func NewTumblingAssigner(sizeMs int64) *TumblingAssigner {
	if sizeMs <= 0 {
		panic("engine: tumbling window size must be positive")
	}
	return &TumblingAssigner{sizeMs: sizeMs}
}

// Assign returns the single window containing eventTime. The window is the
// half-open interval [start, start+size) where start = eventTime floored to a
// multiple of size. Floored modulo keeps this correct for negative event times.
func (a *TumblingAssigner) Assign(eventTime int64) []Window {
	start := eventTime - floorMod(eventTime, a.sizeMs)
	return []Window{{Start: start, End: start + a.sizeMs}}
}

// floorMod returns the non-negative remainder of a/b (b > 0). Go's % can return
// a negative remainder for negative a; this normalizes it so window alignment is
// correct across the zero boundary.
func floorMod(a, b int64) int64 {
	m := a % b
	if m < 0 {
		m += b
	}
	return m
}
