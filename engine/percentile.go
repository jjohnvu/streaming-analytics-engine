package engine

import (
	"math"
	"sort"
	"time"
)

// Percentile returns the q-th percentile (q in [0,100]) of the durations using
// the nearest-rank method. It sorts a copy, so the caller's slice is untouched.
// An empty input returns 0.
func Percentile(d []time.Duration, q float64) time.Duration {
	if len(d) == 0 {
		return 0
	}
	s := make([]time.Duration, len(d))
	copy(s, d)
	sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })

	if q <= 0 {
		return s[0]
	}
	if q >= 100 {
		return s[len(s)-1]
	}
	rank := int(math.Ceil(q / 100 * float64(len(s))))
	if rank < 1 {
		rank = 1
	}
	if rank > len(s) {
		rank = len(s)
	}
	return s[rank-1]
}
