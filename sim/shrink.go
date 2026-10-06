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

	// Then make the numbers small, so downtime=487 reads as downtime=1.
	for i := range best.Faults {
		for {
			f, ok := halve(best.Faults[i].Fault)
			if !ok {
				break
			}
			candidate := best.Clone()
			candidate.Faults[i].Fault = f
			if !stillFails(candidate) {
				break
			}
			best = candidate
		}
	}

	return best
}

// halve cuts the number in a fault in half. false when it is already 1, or
// the fault has no number.
func halve(f Fault) (Fault, bool) {
	switch f := f.(type) {
	case crashFault:
		if f.Downtime <= 1 { // 0 means stay down, that is not a smaller crash
			return nil, false
		}
		f.Downtime /= 2
		return f, true
	case pauseFault:
		if f.Duration <= 1 {
			return nil, false
		}
		f.Duration /= 2
		return f, true
	case lossLinkFault:
		if f.DropPPM <= 1 {
			return nil, false
		}
		f.DropPPM /= 2
		return f, true
	case duplicateLinkFault:
		if f.DuplicatePPM <= 1 {
			return nil, false
		}
		f.DuplicatePPM /= 2
		return f, true
	}
	return nil, false
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
