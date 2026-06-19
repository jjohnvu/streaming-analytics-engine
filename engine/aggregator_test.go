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

// TestSumAggregatorSatisfiesInterface: SumAggregator is usable as an Aggregator
// (compile-time check that the contract is met).
func TestSumAggregatorSatisfiesInterface(t *testing.T) {
	var _ Aggregator = NewSumAggregator()
}
