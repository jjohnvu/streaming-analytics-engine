package engine

import (
	"math/rand"
	"testing"
)

// TumblingAssigner must satisfy WindowAssigner.
var _ WindowAssigner = (*TumblingAssigner)(nil)

// TestTumblingAssignExact pins specific event times to their windows, including
// the boundary: an event at a multiple of size opens the NEXT window.
func TestTumblingAssignExact(t *testing.T) {
	a := NewTumblingAssigner(1000)

	cases := []struct {
		eventTime  int64
		wantWindow Window
	}{
		{0, Window{0, 1000}},
		{1, Window{0, 1000}},
		{999, Window{0, 1000}},
		{1000, Window{1000, 2000}}, // boundary -> next window
		{1001, Window{1000, 2000}},
		{2500, Window{2000, 3000}},
	}

	for _, c := range cases {
		got := a.Assign(c.eventTime)
		if len(got) != 1 {
			t.Fatalf("Assign(%d) returned %d windows, want exactly 1", c.eventTime, len(got))
		}
		if got[0] != c.wantWindow {
			t.Errorf("Assign(%d) = %+v, want %+v", c.eventTime, got[0], c.wantWindow)
		}
		if !got[0].Contains(c.eventTime) {
			t.Errorf("Assign(%d) window %+v does not Contain its own event time", c.eventTime, got[0])
		}
	}
}

// TestTumblingBoundaryNoDoubleCount: two events straddling a boundary land in
// adjacent, distinct windows — never the same one.
func TestTumblingBoundaryNoDoubleCount(t *testing.T) {
	a := NewTumblingAssigner(1000)
	last := a.Assign(999)[0]   // [0,1000)
	first := a.Assign(1000)[0] // [1000,2000)
	if last == first {
		t.Fatalf("boundary events shared a window %+v", last)
	}
	if last.End != first.Start {
		t.Errorf("windows not adjacent: %+v then %+v", last, first)
	}
}

// TestTumblingNegativeEventTime: floored alignment keeps windows correct across
// the zero boundary.
func TestTumblingNegativeEventTime(t *testing.T) {
	a := NewTumblingAssigner(1000)
	cases := []struct {
		eventTime int64
		want      Window
	}{
		{-1, Window{-1000, 0}},
		{-1000, Window{-1000, 0}},
		{-1001, Window{-2000, -1000}},
	}
	for _, c := range cases {
		got := a.Assign(c.eventTime)[0]
		if got != c.want {
			t.Errorf("Assign(%d) = %+v, want %+v", c.eventTime, got, c.want)
		}
		if !got.Contains(c.eventTime) {
			t.Errorf("Assign(%d) window %+v does not Contain its event time", c.eventTime, got)
		}
	}
}

// TestTumblingProperties: over many random event times, every assignment yields
// exactly one window that (a) contains the event, (b) has start aligned to a
// multiple of size, and (c) has width exactly size.
func TestTumblingProperties(t *testing.T) {
	const size = 600
	a := NewTumblingAssigner(size)
	rng := rand.New(rand.NewSource(7))

	for i := 0; i < 100000; i++ {
		et := rng.Int63n(20_000_000) - 10_000_000 // span negatives too
		ws := a.Assign(et)
		if len(ws) != 1 {
			t.Fatalf("Assign(%d) returned %d windows", et, len(ws))
		}
		w := ws[0]
		if !w.Contains(et) {
			t.Fatalf("window %+v does not contain event time %d", w, et)
		}
		if floorMod(w.Start, size) != 0 {
			t.Fatalf("window start %d not aligned to size %d", w.Start, size)
		}
		if w.End-w.Start != size {
			t.Fatalf("window width %d, want %d", w.End-w.Start, size)
		}
	}
}

// SlidingAssigner must satisfy WindowAssigner.
var _ WindowAssigner = (*SlidingAssigner)(nil)

// TestSlidingAssignExact pins events to their full set of overlapping windows,
// in increasing start order.
func TestSlidingAssignExact(t *testing.T) {
	a := NewSlidingAssigner(1000, 500) // each event in 2 windows

	cases := []struct {
		eventTime int64
		want      []Window
	}{
		{0, []Window{{-500, 500}, {0, 1000}}},
		{499, []Window{{-500, 500}, {0, 1000}}},
		{500, []Window{{0, 1000}, {500, 1500}}},
		{999, []Window{{0, 1000}, {500, 1500}}},
		{1000, []Window{{500, 1500}, {1000, 2000}}}, // boundary -> shifts forward
		{-1, []Window{{-1000, 0}, {-500, 500}}},     // negative event time
	}

	for _, c := range cases {
		got := a.Assign(c.eventTime)
		if len(got) != len(c.want) {
			t.Fatalf("Assign(%d) = %v, want %v", c.eventTime, got, c.want)
		}
		for i := range c.want {
			if got[i] != c.want[i] {
				t.Errorf("Assign(%d)[%d] = %+v, want %+v", c.eventTime, i, got[i], c.want[i])
			}
		}
	}
}

// TestSlidingEqualsTumblingWhenSlideIsSize: slide == size degenerates to
// tumbling — same single window for any event time.
func TestSlidingEqualsTumblingWhenSlideIsSize(t *testing.T) {
	s := NewSlidingAssigner(1000, 1000)
	tu := NewTumblingAssigner(1000)
	rng := rand.New(rand.NewSource(3))

	for i := 0; i < 10000; i++ {
		et := rng.Int63n(20_000_000) - 10_000_000
		sw, tw := s.Assign(et), tu.Assign(et)
		if len(sw) != 1 || sw[0] != tw[0] {
			t.Fatalf("Assign(%d): sliding %v != tumbling %v", et, sw, tw)
		}
	}
}

// TestSlidingProperties: for slide dividing size, every event lands in exactly
// size/slide windows; each contains the event, is slide-aligned, and is exactly
// size wide; starts strictly increase by slide.
func TestSlidingProperties(t *testing.T) {
	const size, slide = 900, 300 // 3 windows per event
	a := NewSlidingAssigner(size, slide)
	rng := rand.New(rand.NewSource(11))

	for i := 0; i < 100000; i++ {
		et := rng.Int63n(20_000_000) - 10_000_000
		ws := a.Assign(et)
		if len(ws) != size/slide {
			t.Fatalf("Assign(%d) returned %d windows, want %d", et, len(ws), size/slide)
		}
		for k, w := range ws {
			if !w.Contains(et) {
				t.Fatalf("window %+v does not contain %d", w, et)
			}
			if floorMod(w.Start, slide) != 0 {
				t.Fatalf("window start %d not slide-aligned", w.Start)
			}
			if w.End-w.Start != size {
				t.Fatalf("window width %d, want %d", w.End-w.Start, size)
			}
			if k > 0 && w.Start != ws[k-1].Start+slide {
				t.Fatalf("starts not increasing by slide: %v", ws)
			}
		}
	}
}

// TestSlidingGapWhenSlideExceedsSize: with slide > size, instants between
// windows are covered by none — allowed, but they assign to zero windows.
func TestSlidingGapWhenSlideExceedsSize(t *testing.T) {
	a := NewSlidingAssigner(300, 1000) // windows [0,300), [1000,1300), ...

	if got := a.Assign(100); len(got) != 1 || (got[0] != Window{0, 300}) {
		t.Errorf("Assign(100) = %v, want [{0 300}]", got)
	}
	if got := a.Assign(500); len(got) != 0 {
		t.Errorf("Assign(500) = %v, want none (gap)", got)
	}
}

// TestNewSlidingAssignerPanics: non-positive size or slide is a programming
// error.
func TestNewSlidingAssignerPanics(t *testing.T) {
	cases := []struct{ size, slide int64 }{{0, 500}, {-1, 500}, {1000, 0}, {1000, -5}}
	for _, c := range cases {
		func() {
			defer func() {
				if recover() == nil {
					t.Errorf("NewSlidingAssigner(%d, %d) did not panic", c.size, c.slide)
				}
			}()
			NewSlidingAssigner(c.size, c.slide)
		}()
	}
}

// TestNewTumblingAssignerPanics: a non-positive size is a programming error.
func TestNewTumblingAssignerPanics(t *testing.T) {
	for _, size := range []int64{0, -1, -1000} {
		func() {
			defer func() {
				if recover() == nil {
					t.Errorf("NewTumblingAssigner(%d) did not panic", size)
				}
			}()
			NewTumblingAssigner(size)
		}()
	}
}
