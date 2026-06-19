package engine

import "testing"

// TestWindowContains pins down the half-open [Start, End) rule, especially the
// boundary: an event at exactly End must NOT be in this window (it belongs to
// the next one). This is the invariant that stops double-counting at edges.
func TestWindowContains(t *testing.T) {
	w := Window{Start: 1000, End: 2000}

	cases := []struct {
		name string
		t    int64
		want bool
	}{
		{"before start", 999, false},
		{"at start (inclusive)", 1000, true},
		{"interior", 1500, true},
		{"just before end", 1999, true},
		{"at end (exclusive)", 2000, false},
		{"after end", 2001, false},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := w.Contains(c.t); got != c.want {
				t.Errorf("Window{%d,%d}.Contains(%d) = %v, want %v",
					w.Start, w.End, c.t, got, c.want)
			}
		})
	}
}

// TestAdjacentWindowsNoOverlap verifies the practical consequence: a boundary
// timestamp lands in exactly one of two abutting windows, never both.
func TestAdjacentWindowsNoOverlap(t *testing.T) {
	left := Window{Start: 0, End: 1000}
	right := Window{Start: 1000, End: 2000}

	const boundary = 1000
	if left.Contains(boundary) {
		t.Errorf("boundary %d should not be in left window [%d,%d)", boundary, left.Start, left.End)
	}
	if !right.Contains(boundary) {
		t.Errorf("boundary %d should be in right window [%d,%d)", boundary, right.Start, right.End)
	}
}
