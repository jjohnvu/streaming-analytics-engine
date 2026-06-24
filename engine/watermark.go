package engine

// WatermarkGenerator turns a stream of observed event times into watermarks. It
// tracks the maximum event time seen and emits
//
//	watermark = maxEventTime - allowedLatenessMs
//
// The subtraction is the engine deliberately holding back: it declares "no event
// older than this should still arrive" only once it's seen data at least
// allowedLatenessMs beyond that point. Larger lateness tolerates more
// out-of-order/late data but delays closing windows; smaller closes sooner but
// sends more data to the side output. No free lunch.
type WatermarkGenerator struct {
	allowedLatenessMs int64
	maxEventTime      int64
	seenEvent         bool
	lastWatermark     int64
	emitted           bool
}

// NewWatermarkGenerator returns a generator with the given lateness holdback in
// milliseconds. Panics on a negative value.
func NewWatermarkGenerator(allowedLatenessMs int64) *WatermarkGenerator {
	if allowedLatenessMs < 0 {
		panic("engine: allowed lateness must be non-negative")
	}
	return &WatermarkGenerator{allowedLatenessMs: allowedLatenessMs}
}

// Observe records an event time and returns the current watermark together with
// whether it advanced (strictly increased) since the last emission. Because
// maxEventTime only ever grows, the watermark is monotonic non-decreasing —
// out-of-order events never move it backwards. Callers emit the returned
// watermark in-band only when the bool is true.
func (g *WatermarkGenerator) Observe(eventTime int64) (Watermark, bool) {
	if !g.seenEvent || eventTime > g.maxEventTime {
		g.maxEventTime = eventTime
		g.seenEvent = true
	}
	wm := g.maxEventTime - g.allowedLatenessMs
	if !g.emitted || wm > g.lastWatermark {
		g.lastWatermark = wm
		g.emitted = true
		return Watermark{Timestamp: wm}, true
	}
	return Watermark{}, false
}

// Current returns the latest watermark value implied by what's been observed,
// regardless of whether it's been emitted.
func (g *WatermarkGenerator) Current() int64 {
	return g.maxEventTime - g.allowedLatenessMs
}
