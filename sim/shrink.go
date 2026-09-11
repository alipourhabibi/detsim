package sim

import "slices"

// Shrink removes faults from a failing plan while stillFails holds.
//
// stillFails MUST use the same seed as the original failure. Otherwise it tests
// whether the failure reproduces at all, not whether a given fault mattered.
func Shrink(p *Plan, stillFails func(*Plan) bool) *Plan {
	if !stillFails(p) {
		panic("sim: Shrink called on a plan that does not fail")
	}

	best := p

	for i := len(best.Faults) - 1; i >= 0; i-- {
		candidate := best.Clone()
		candidate.Faults = slices.Delete(candidate.Faults, i, i+1)
		if stillFails(candidate) {
			best = candidate
		}
	}

	for i := len(best.Buggify) - 1; i >= 0; i-- {
		candidate := best.Clone()
		candidate.Buggify = slices.Delete(candidate.Buggify, i, i+1)
		if stillFails(candidate) {
			best = candidate
		}
	}

	return best
}

// ShrinkToFixpoint repeats Shrink until a pass removes nothing. Catches faults
// that only became removable once something else was gone.
func ShrinkToFixpoint(p *Plan, stillFails func(*Plan) bool) *Plan {
	for {
		next := Shrink(p, stillFails)
		if next.Len() == p.Len() && next.BuggifyLen() == p.BuggifyLen() {
			return next
		}
		p = next
	}
}
