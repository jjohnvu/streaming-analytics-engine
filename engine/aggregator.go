package engine

import "fmt"

// Aggregator is the running-aggregate contract, an interface from day one (see
// CONTEXT.md). Add folds in a single value; Merge combines another aggregate
// into this one — the operation that makes session-window merging and (later)
// distributed partial aggregates clean; Result reads out the current value.
//
// Merge is only defined between aggregates of the same kind: merging an avg
// into a min is meaningless, and aggregates whose state is richer than their
// Result (avg needs sum AND count) can't merge through Result() alone. Kinds
// that need internal state type-assert and panic on a mismatch — a mixed merge
// is a programming error, not runtime data.
type Aggregator interface {
	Add(value float64)
	Merge(other Aggregator)
	Result() float64
}

// SumAggregator is the simplest Aggregator: a running total. It's the MVP
// aggregate; avg, min/max, and count follow behind the same interface.
type SumAggregator struct {
	sum float64
}

// NewSumAggregator returns a zero-valued sum aggregate.
func NewSumAggregator() *SumAggregator {
	return &SumAggregator{}
}

// Add folds value into the running total.
func (s *SumAggregator) Add(value float64) {
	s.sum += value
}

// Merge combines another aggregate's result into this one. For a sum, adding
// the other's Result() is exact, and reading through the interface keeps
// SumAggregator from depending on the concrete type of other.
func (s *SumAggregator) Merge(other Aggregator) {
	s.sum += other.Result()
}

// Result returns the current sum.
func (s *SumAggregator) Result() float64 {
	return s.sum
}

// AvgAggregator computes the arithmetic mean. Its state is richer than its
// Result — it must carry sum and count separately so that Merge is exact.
type AvgAggregator struct {
	sum float64
	n   int64
}

// NewAvgAggregator returns an empty average aggregate.
func NewAvgAggregator() *AvgAggregator {
	return &AvgAggregator{}
}

// Add folds value into the running mean.
func (a *AvgAggregator) Add(value float64) {
	a.sum += value
	a.n++
}

// Merge combines another AvgAggregator's sum and count into this one. Merging
// via Result() would average the averages — wrong for unequal counts — so this
// requires the concrete type.
func (a *AvgAggregator) Merge(other Aggregator) {
	o, ok := other.(*AvgAggregator)
	if !ok {
		panic(fmt.Sprintf("engine: cannot merge %T into *AvgAggregator", other))
	}
	a.sum += o.sum
	a.n += o.n
}

// Result returns the mean of everything added, or 0 for an empty aggregate.
func (a *AvgAggregator) Result() float64 {
	if a.n == 0 {
		return 0
	}
	return a.sum / float64(a.n)
}

// MinAggregator tracks the smallest value seen. An empty aggregate reports 0;
// the seen flag (not a sentinel like +Inf) is what makes Merge with an empty
// side exact.
type MinAggregator struct {
	m    float64
	seen bool
}

// NewMinAggregator returns an empty min aggregate.
func NewMinAggregator() *MinAggregator {
	return &MinAggregator{}
}

// Add folds value into the running minimum.
func (a *MinAggregator) Add(value float64) {
	if !a.seen || value < a.m {
		a.m = value
	}
	a.seen = true
}

// Merge combines another MinAggregator into this one; an empty other is a
// no-op.
func (a *MinAggregator) Merge(other Aggregator) {
	o, ok := other.(*MinAggregator)
	if !ok {
		panic(fmt.Sprintf("engine: cannot merge %T into *MinAggregator", other))
	}
	if o.seen {
		a.Add(o.m)
	}
}

// Result returns the minimum seen, or 0 for an empty aggregate.
func (a *MinAggregator) Result() float64 {
	return a.m
}

// MaxAggregator tracks the largest value seen. Same empty-state handling as
// MinAggregator.
type MaxAggregator struct {
	m    float64
	seen bool
}

// NewMaxAggregator returns an empty max aggregate.
func NewMaxAggregator() *MaxAggregator {
	return &MaxAggregator{}
}

// Add folds value into the running maximum.
func (a *MaxAggregator) Add(value float64) {
	if !a.seen || value > a.m {
		a.m = value
	}
	a.seen = true
}

// Merge combines another MaxAggregator into this one; an empty other is a
// no-op.
func (a *MaxAggregator) Merge(other Aggregator) {
	o, ok := other.(*MaxAggregator)
	if !ok {
		panic(fmt.Sprintf("engine: cannot merge %T into *MaxAggregator", other))
	}
	if o.seen {
		a.Add(o.m)
	}
}

// Result returns the maximum seen, or 0 for an empty aggregate.
func (a *MaxAggregator) Result() float64 {
	return a.m
}

// CountAggregator counts events, ignoring their values.
type CountAggregator struct {
	n int64
}

// NewCountAggregator returns a zero count aggregate.
func NewCountAggregator() *CountAggregator {
	return &CountAggregator{}
}

// Add increments the count; the value is irrelevant.
func (a *CountAggregator) Add(float64) {
	a.n++
}

// Merge adds another CountAggregator's count into this one.
func (a *CountAggregator) Merge(other Aggregator) {
	o, ok := other.(*CountAggregator)
	if !ok {
		panic(fmt.Sprintf("engine: cannot merge %T into *CountAggregator", other))
	}
	a.n += o.n
}

// Result returns the count.
func (a *CountAggregator) Result() float64 {
	return float64(a.n)
}
