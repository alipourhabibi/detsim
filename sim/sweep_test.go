package sim

import (
	"errors"
	"sync"
	"testing"
)

var fakeErr = errors.New("fake failure")

func trivialBuild(seed uint64, _ *Plan) (*Sim, uint64) {
	s := New(testConfig(seed), []int{0, 1}, noop)
	s.Start()
	return s, seed
}

// A build that really applies the plan and produces events.
func realBuild(seed uint64, p *Plan) (*Sim, uint64) {
	s := New(testConfig(seed), []int{0, 1},
		func(id int, _ Deps) Handler { return &pingOnTimer{peer: 1 - id} })
	s.Start()
	p.Apply(s)
	return s, seed
}

func emptyPlan(uint64, Rand) *Plan {
	return &Plan{}
}

func TestSweepReportsLowestFailingSeed(t *testing.T) {
	failing := map[uint64]bool{7: true, 12: true, 40: true}

	check := func(_ *Sim, seed uint64) error {
		if failing[seed] {
			return fakeErr
		}
		return nil
	}

	f, _ := Sweep(1, 100, emptyPlan, trivialBuild, check)
	if f == nil {
		t.Fatal("Sweep found no failure")
	}
	if f.Seed != 7 {
		t.Fatalf("Sweep reported seed %d, want 7 (the lowest failing seed)", f.Seed)
	}
}

func TestSweepReturnsEveryFailingSeed(t *testing.T) {
	failing := map[uint64]bool{7: true, 12: true, 40: true}

	check := func(_ *Sim, seed uint64) error {
		if failing[seed] {
			return fakeErr
		}
		return nil
	}

	_, all := Sweep(1, 100, emptyPlan, trivialBuild, check)
	if len(all) != len(failing) {
		t.Fatalf("got %d failures, want %d", len(all), len(failing))
	}
	for _, sf := range all {
		if !failing[sf.Seed] {
			t.Errorf("seed %d reported as failing but should have passed", sf.Seed)
		}
		if !errors.Is(sf.Err, fakeErr) {
			t.Errorf("seed %d carries %v, want the check's error", sf.Seed, sf.Err)
		}
	}
}

func TestSweepReturnsNilWhenNothingFails(t *testing.T) {
	check := func(*Sim, uint64) error { return nil }

	f, all := Sweep(1, 100, emptyPlan, trivialBuild, check)
	if f != nil {
		t.Fatalf("Sweep reported a failure at seed %d with no failures", f.Seed)
	}
	if len(all) != 0 {
		t.Fatalf("Sweep returned %d failures with none", len(all))
	}
}

func TestSweepFailureIsReproducible(t *testing.T) {
	gen := func(seed uint64, r Rand) *Plan {
		return GeneratePlan(testPlanConfig(), []int{0, 1}, seed, r)
	}

	check := func(s *Sim, seed uint64) error {
		if err := s.RunUntil(1000); err != nil {
			return err
		}
		if seed == 3 {
			return fakeErr
		}
		return nil
	}

	f, _ := Sweep(1, 10, gen, realBuild, check)
	if f == nil {
		t.Fatal("Sweep found no failure")
	}
	if f.Seed != 3 {
		t.Fatalf("reported seed %d, want 3", f.Seed)
	}
	if f.Plan == nil {
		t.Fatal("failure carries no plan; it cannot be replayed")
	}
	if f.Sim == nil || f.Sim.Trace().Len() == 0 {
		t.Fatal("failure carries no trace; the run cannot be inspected")
	}
	if f.Ctx != 3 {
		t.Fatalf("failure carries context %d, want the failing seed", f.Ctx)
	}
	if !errors.Is(f.Err, fakeErr) {
		t.Fatalf("failure carries %v, want the check's error", f.Err)
	}
}

// The plan handed to build must be the plan gen produced for that seed. If they
// diverge, the reported plan does not reproduce the reported failure.
func TestSweepPassesGeneratedPlanToBuild(t *testing.T) {
	var generated, built *Plan

	gen := func(seed uint64, r Rand) *Plan {
		generated = GeneratePlan(testPlanConfig(), []int{0, 1}, seed, r)
		return generated
	}
	build := func(seed uint64, p *Plan) (*Sim, uint64) {
		built = p
		return trivialBuild(seed, p)
	}

	Sweep(1, 2, gen, build, func(*Sim, uint64) error { return fakeErr })

	if built != generated {
		t.Fatal("build received a different plan than gen produced")
	}
}

// Two sweeps over the same range produce the same plans.
//
// Keyed by seed, not by position. gen runs on several goroutines, so the order
// it is called in changes between runs
func TestSweepIsDeterministic(t *testing.T) {
	collect := func() map[uint64]string {
		var mu sync.Mutex
		out := map[uint64]string{}

		gen := func(seed uint64, r Rand) *Plan {
			p := GeneratePlan(testPlanConfig(), []int{0, 1, 2}, seed, r)

			mu.Lock()
			out[seed] = p.String()
			mu.Unlock()

			return p
		}

		Sweep(1, 20, gen, trivialBuild, func(*Sim, uint64) error { return nil })
		return out
	}

	a, b := collect(), collect()
	if len(a) != len(b) {
		t.Fatalf("sweeps generated %d and %d plans", len(a), len(b))
	}
	for seed, plan := range a {
		if b[seed] != plan {
			t.Fatalf("seed %d gave a different plan between identical sweeps", seed)
		}
	}
}
