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

// SlidingAssigner assigns each event to every window covering it: fixed-size
// windows whose starts fall on multiples of slideMs. With slide < size the
// windows overlap and one event lands in ceil(size/slide) of them; slide == size
// degenerates to tumbling. slide > size is permitted (sampling windows) but
// leaves gaps — events between windows are assigned to none.
type SlidingAssigner struct {
	sizeMs  int64
	slideMs int64
}

// NewSlidingAssigner returns a sliding assigner with the given window size and
// slide interval in milliseconds. Panics if either is non-positive — that's a
// programming error, not runtime data.
func NewSlidingAssigner(sizeMs, slideMs int64) *SlidingAssigner {
	if sizeMs <= 0 {
		panic("engine: sliding window size must be positive")
	}
	if slideMs <= 0 {
		panic("engine: sliding window slide must be positive")
	}
	return &SlidingAssigner{sizeMs: sizeMs, slideMs: slideMs}
}

// Assign returns every window containing eventTime, in increasing start order.
// A window [s, s+size) contains t iff t-size < s <= t, with s aligned to a
// multiple of slide; the latest such start is t floored to the slide grid, and
// the rest step back by slide until they fall out of range.
func (a *SlidingAssigner) Assign(eventTime int64) []Window {
	latest := eventTime - floorMod(eventTime, a.slideMs)
	span := latest - eventTime + a.sizeMs // how far the valid starts extend back
	if span <= 0 {
		return nil // slide > size gap: no window covers this instant
	}
	n := (span + a.slideMs - 1) / a.slideMs // ceil(span/slide) valid starts
	ws := make([]Window, n)
	for i := int64(0); i < n; i++ {
		s := latest - (n-1-i)*a.slideMs
		ws[i] = Window{Start: s, End: s + a.sizeMs}
	}
	return ws
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
