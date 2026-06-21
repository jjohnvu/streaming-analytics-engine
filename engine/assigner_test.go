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
