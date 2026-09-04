package sim

import "fmt"

// Failure is a seed that failed, with everything needed to reproduce it.
type Failure struct {
	Seed uint64
	Plan *Plan
	Err  error
	Sim  *Sim // trace intact for Dump
}

func (f *Failure) String() string {
	return fmt.Sprintf("seed %d failed after %d events: %v\n\nplan:\n%s",
		f.Seed, f.Sim.EventCount(), f.Err, f.Plan)
}

// Sweep runs seeds in order and returns the first failure, or nil.
//
// Sequential on purpose. Parallelising across seeds is safe, seeds are
// independent, but the reported failure must be the LOWEST failing seed, not
// whichever goroutine finished first, or the sweep itself stops being
// reproducible.
func Sweep(
	from uint64,
	to uint64,
	gen func(seed uint64, r Rand) *Plan,
	build func(seed uint64, p *Plan) *Sim,
	check func(*Sim) error,
) *Failure {

	for seed := from; seed < to; seed++ {
		r := newStream(seed, streamFault)
		plan := gen(seed, r)
		s := build(seed, plan)
		if err := check(s); err != nil {
			return &Failure{Seed: seed, Plan: plan, Err: err, Sim: s}
		}
	}
	return nil
}
