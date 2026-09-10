package sim

import (
	"fmt"
	"runtime"
	"sync"
)

// Failure is a seed that failed, with everything needed to reproduce it.
type Failure[T any] struct {
	Seed uint64
	Plan *Plan
	Err  error
	Sim  *Sim // trace intact for Dump
	Ctx  T
}

func (f *Failure[T]) String() string {
	return fmt.Sprintf("seed %d failed after %d events: %v\n\nplan:\n%s",
		f.Seed, f.Sim.EventCount(), f.Err, f.Plan)
}

// Sweep runs seeds across all cores and returns the lowest failing seed, plus
// every seed that failed.
//
// Lowest, not first: with several workers, "first" is whichever core finished,
// so the same sweep would give a different answer each run. The full list is
// for the saving the failed one.
func Sweep[T any](
	from uint64,
	to uint64,
	gen func(seed uint64, r Rand) *Plan,
	build func(seed uint64, p *Plan) (*Sim, T),
	check func(s *Sim, ctx T) error,
) (*Failure[T], []SeedFailure) {

	if to <= from {
		return nil, nil
	}

	workers := min(uint64(runtime.GOMAXPROCS(0)), to-from)

	seeds := make(chan uint64)
	fails := make(chan *Failure[T])

	go func() {
		defer close(seeds)
		for seed := from; seed < to; seed++ {
			seeds <- seed
		}
	}()

	var wg sync.WaitGroup
	wg.Add(int(workers))

	for range workers {
		go func() {
			defer wg.Done()
			for seed := range seeds {
				r := NewStream(seed, StreamFault)
				plan := gen(seed, r)
				s, ctx := build(seed, plan)
				if err := check(s, ctx); err != nil {
					fails <- &Failure[T]{
						Seed: seed,
						Plan: plan,
						Err:  err,
						Sim:  s,
						Ctx:  ctx,
					}
				}
			}
		}()
	}

	go func() {
		wg.Wait()
		close(fails)
	}()

	var best *Failure[T]
	allSeeds := make([]SeedFailure, 0, to-from)
	for f := range fails {
		allSeeds = append(allSeeds, SeedFailure{
			Seed: f.Seed,
			Err:  f.Err,
		})
		if best == nil || f.Seed < best.Seed {
			best = f
		}
	}
	return best, allSeeds
}
