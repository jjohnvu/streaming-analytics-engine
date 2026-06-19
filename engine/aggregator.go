package engine

// Aggregator is the running-aggregate contract, an interface from day one (see
// CONTEXT.md). Add folds in a single value; Merge combines another aggregate
// into this one — the operation that makes session-window merging and (later)
// distributed partial aggregates clean; Result reads out the current value.
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
