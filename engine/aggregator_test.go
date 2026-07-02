package engine

import "testing"

// TestSumAggregatorEmpty: a fresh aggregate reads zero.
func TestSumAggregatorEmpty(t *testing.T) {
	s := NewSumAggregator()
	if got := s.Result(); got != 0 {
		t.Errorf("empty SumAggregator.Result() = %v, want 0", got)
	}
}

// TestSumAggregatorAdd: repeated Add accumulates.
func TestSumAggregatorAdd(t *testing.T) {
	s := NewSumAggregator()
	s.Add(1.5)
	s.Add(2.5)
	s.Add(-1.0)
	if got, want := s.Result(), 3.0; got != want {
		t.Errorf("after Adds, Result() = %v, want %v", got, want)
	}
}

// TestSumAggregatorMerge: merging folds another aggregate's result in, leaving
// the source untouched (Merge reads, it doesn't consume).
func TestSumAggregatorMerge(t *testing.T) {
	a := NewSumAggregator()
	a.Add(10)
	a.Add(5)

	b := NewSumAggregator()
	b.Add(7)
	b.Add(3)

	a.Merge(b)

	if got, want := a.Result(), 25.0; got != want {
		t.Errorf("after Merge, a.Result() = %v, want %v", got, want)
	}
	if got, want := b.Result(), 10.0; got != want {
		t.Errorf("Merge mutated source: b.Result() = %v, want %v", got, want)
	}
}

// All aggregators must satisfy the interface (compile-time check).
var (
	_ Aggregator = (*SumAggregator)(nil)
	_ Aggregator = (*AvgAggregator)(nil)
	_ Aggregator = (*MinAggregator)(nil)
	_ Aggregator = (*MaxAggregator)(nil)
	_ Aggregator = (*CountAggregator)(nil)
)

// TestAvgAggregator: mean over adds; empty reads 0.
func TestAvgAggregator(t *testing.T) {
	a := NewAvgAggregator()
	if got := a.Result(); got != 0 {
		t.Errorf("empty avg = %v, want 0", got)
	}
	a.Add(10)
	a.Add(20)
	a.Add(30)
	if got, want := a.Result(), 20.0; got != want {
		t.Errorf("avg = %v, want %v", got, want)
	}
}

// TestAvgAggregatorMergeExact: merge must be count-weighted, not an average of
// averages. {10} merged with {2,2,2} is 4.0 — averaging the averages would give
// 6.0.
func TestAvgAggregatorMergeExact(t *testing.T) {
	a := NewAvgAggregator()
	a.Add(10)

	b := NewAvgAggregator()
	b.Add(2)
	b.Add(2)
	b.Add(2)

	a.Merge(b)
	if got, want := a.Result(), 4.0; got != want {
		t.Errorf("merged avg = %v, want %v (count-weighted)", got, want)
	}
}

// TestMinMaxAggregators: track extremes, including negatives.
func TestMinMaxAggregators(t *testing.T) {
	mn, mx := NewMinAggregator(), NewMaxAggregator()
	for _, v := range []float64{3, -7, 12, 0.5} {
		mn.Add(v)
		mx.Add(v)
	}
	if got := mn.Result(); got != -7 {
		t.Errorf("min = %v, want -7", got)
	}
	if got := mx.Result(); got != 12 {
		t.Errorf("max = %v, want 12", got)
	}
}

// TestMinMaxMergeWithEmpty: merging an empty aggregate is a no-op — the empty
// side's zero must not masquerade as a real value.
func TestMinMaxMergeWithEmpty(t *testing.T) {
	mn := NewMinAggregator()
	mn.Add(5) // 0 from an empty merge would wrongly become the min
	mn.Merge(NewMinAggregator())
	if got := mn.Result(); got != 5 {
		t.Errorf("min after empty merge = %v, want 5", got)
	}

	mx := NewMaxAggregator()
	mx.Add(-5) // 0 from an empty merge would wrongly become the max
	mx.Merge(NewMaxAggregator())
	if got := mx.Result(); got != -5 {
		t.Errorf("max after empty merge = %v, want -5", got)
	}
}

// TestMinMaxMerge: a real merge folds the other's extreme in.
func TestMinMaxMerge(t *testing.T) {
	a, b := NewMinAggregator(), NewMinAggregator()
	a.Add(4)
	b.Add(-2)
	a.Merge(b)
	if got := a.Result(); got != -2 {
		t.Errorf("merged min = %v, want -2", got)
	}
}

// TestCountAggregator: counts adds regardless of value; merge sums counts.
func TestCountAggregator(t *testing.T) {
	a := NewCountAggregator()
	a.Add(99)
	a.Add(-1)
	a.Add(0)

	b := NewCountAggregator()
	b.Add(7)

	a.Merge(b)
	if got, want := a.Result(), 4.0; got != want {
		t.Errorf("count = %v, want %v", got, want)
	}
}

// TestMergeKindMismatchPanics: merging different aggregate kinds is a
// programming error and must fail loudly, not corrupt results silently.
func TestMergeKindMismatchPanics(t *testing.T) {
	targets := []Aggregator{
		NewAvgAggregator(), NewMinAggregator(), NewMaxAggregator(), NewCountAggregator(),
	}
	for _, target := range targets {
		func() {
			defer func() {
				if recover() == nil {
					t.Errorf("%T.Merge(*SumAggregator) did not panic", target)
				}
			}()
			target.Merge(NewSumAggregator())
		}()
	}
}
