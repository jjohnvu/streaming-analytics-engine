package engine

import (
	"testing"
	"time"
)

func TestPercentile(t *testing.T) {
	// 1..10 ms
	d := make([]time.Duration, 10)
	for i := range d {
		d[i] = time.Duration(i+1) * time.Millisecond
	}

	cases := []struct {
		q    float64
		want time.Duration
	}{
		{0, 1 * time.Millisecond},    // min
		{50, 5 * time.Millisecond},   // nearest-rank: ceil(0.5*10)=5 -> s[4]
		{90, 9 * time.Millisecond},   // ceil(0.9*10)=9 -> s[8]
		{99, 10 * time.Millisecond},  // ceil(0.99*10)=10 -> s[9]
		{100, 10 * time.Millisecond}, // max
	}
	for _, c := range cases {
		if got := Percentile(d, c.q); got != c.want {
			t.Errorf("Percentile(p%g) = %v, want %v", c.q, got, c.want)
		}
	}
}

func TestPercentileUnsortedInputUntouched(t *testing.T) {
	d := []time.Duration{5, 1, 3, 2, 4}
	orig := append([]time.Duration(nil), d...)

	if got := Percentile(d, 50); got != 3 {
		t.Errorf("p50 = %v, want 3", got)
	}
	for i := range d {
		if d[i] != orig[i] {
			t.Fatalf("Percentile mutated input: %v != %v", d, orig)
		}
	}
}

func TestPercentileEmpty(t *testing.T) {
	if got := Percentile(nil, 50); got != 0 {
		t.Errorf("Percentile(empty) = %v, want 0", got)
	}
}
