package main

import (
	"errors"
	"fmt"
	"testing"

	"github.com/alipourhabibi/detsim/sim"
)

// -seeds a..b includes b, so 1..20 is 20 seeds and 5..5 is one
func TestParseRangeIsInclusive(t *testing.T) {
	for _, c := range []struct {
		in       string
		from, to uint64
	}{
		{"1..20", 1, 21},
		{"5..5", 5, 6},
		{"7", 7, 8},
	} {
		from, to, err := parseRange(c.in)
		if err != nil || from != c.from || to != c.to {
			t.Errorf("parseRange(%q) = %d, %d, %v, want %d, %d", c.in, from, to, err, c.from, c.to)
		}
	}
	if _, _, err := parseRange("9..3"); err == nil {
		t.Error("9..3 should be an error")
	}
}

// an invariant that breaks while RunHealed runs is an invariant failure, not
// a liveness one
func TestKindOfHealedInvariant(t *testing.T) {
	err := fmt.Errorf("x: %w", &sim.InvariantError{Err: errors.New("two holders")})
	if got := runErr(err); got.kind != "invariant" {
		t.Fatalf("kind = %q, want invariant", got.kind)
	}
}
