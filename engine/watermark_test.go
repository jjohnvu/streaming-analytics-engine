package engine

import "testing"

// StreamElement is sealed but Event and Watermark must satisfy it.
var (
	_ StreamElement = Event{}
	_ StreamElement = Watermark{}
)

// TestWatermarkHoldback: the watermark sits allowedLateness behind the max
// event time.
func TestWatermarkHoldback(t *testing.T) {
	g := NewWatermarkGenerator(200)
	wm, ok := g.Observe(1000)
	if !ok {
		t.Fatal("first observation should emit a watermark")
	}
	if wm.Timestamp != 800 {
		t.Errorf("watermark = %d, want 800 (1000-200)", wm.Timestamp)
	}
}

// TestWatermarkMonotonic: an out-of-order (older) event must not move the
// watermark backwards, and shouldn't re-emit.
func TestWatermarkMonotonic(t *testing.T) {
	g := NewWatermarkGenerator(100)

	if wm, ok := g.Observe(1000); !ok || wm.Timestamp != 900 {
		t.Fatalf("Observe(1000) = (%d,%v), want (900,true)", wm.Timestamp, ok)
	}
	// Older event: max unchanged, watermark unchanged, no emission.
	if wm, ok := g.Observe(500); ok {
		t.Errorf("Observe(500) emitted %d; watermark should not advance on older event", wm.Timestamp)
	}
	if g.Current() != 900 {
		t.Errorf("Current() = %d, want 900", g.Current())
	}
}

// TestWatermarkAdvances: a newer max advances and re-emits the watermark.
func TestWatermarkAdvances(t *testing.T) {
	g := NewWatermarkGenerator(100)
	g.Observe(1000) // -> 900
	wm, ok := g.Observe(1200)
	if !ok || wm.Timestamp != 1100 {
		t.Errorf("Observe(1200) = (%d,%v), want (1100,true)", wm.Timestamp, ok)
	}
}

// TestWatermarkZeroLateness: with no holdback the watermark equals the max.
func TestWatermarkZeroLateness(t *testing.T) {
	g := NewWatermarkGenerator(0)
	if wm, _ := g.Observe(1234); wm.Timestamp != 1234 {
		t.Errorf("watermark = %d, want 1234", wm.Timestamp)
	}
}

// TestWatermarkNegativePanics: a negative holdback is a programming error.
func TestWatermarkNegativePanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Error("NewWatermarkGenerator(-1) did not panic")
		}
	}()
	NewWatermarkGenerator(-1)
}
