package sim

import (
	"errors"
	"testing"
)

var fakeErr = errors.New("fake failure")

func trivialBuild(seed uint64, _ *Plan) *Sim {
	s := New(testConfig(seed), []int{0, 1}, noop)
	s.Start()
	return s
}

func emptyPlan(uint64, Rand) *Plan { return &Plan{} }

func TestSweepReportsLowestFailingSeed(t *testing.T) {
	failing := map[uint64]bool{7: true, 12: true, 40: true}

	check := func(s *Sim) error {
		if failing[s.cfg.Seed] {
			return fakeErr
		}
		return nil
	}

	f := Sweep(1, 100, emptyPlan, trivialBuild, check)
	if f == nil {
		t.Fatal("Sweep found no failure")
	}
	if f.Seed != 7 {
		t.Fatalf("Sweep reported seed %d, want 7 (the lowest failing seed)", f.Seed)
	}
}

func TestSweepReturnsNilWhenNothingFails(t *testing.T) {
	if f := Sweep(1, 100, emptyPlan, trivialBuild, func(*Sim) error { return nil }); f != nil {
		t.Fatalf("Sweep reported a failure at seed %d with no failures", f.Seed)
	}
}

// A build that actually applies the plan and produces events, so the trace
// assertion means something.
func realBuild(seed uint64, p *Plan) *Sim {
	s := New(testConfig(seed), []int{0, 1},
		func(id int, _ Deps) Handler { return &pingOnTimer{peer: 1 - id} })
	s.Start()
	p.Apply(s)
	return s
}

func TestSweepFailureIsReproducible(t *testing.T) {
	gen := func(seed uint64, r Rand) *Plan {
		return GeneratePlan(testPlanConfig(), []int{0, 1}, r)
	}

	check := func(s *Sim) error {
		if err := s.RunUntil(1000); err != nil {
			return err
		}
		if s.cfg.Seed == 3 {
			return fakeErr
		}
		return nil
	}

	f := Sweep(1, 10, gen, realBuild, check)
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
	if !errors.Is(f.Err, fakeErr) {
		t.Fatalf("failure carries %v, want the check's error", f.Err)
	}
}

// The plan handed to build must be the plan gen produced for that seed. If they
// diverge, the reported plan does not reproduce the reported failure.
func TestSweepPassesGeneratedPlanToBuild(t *testing.T) {
	var generated, built *Plan

	gen := func(seed uint64, r Rand) *Plan {
		generated = GeneratePlan(testPlanConfig(), []int{0, 1}, r)
		return generated
	}
	build := func(seed uint64, p *Plan) *Sim {
		built = p
		return trivialBuild(seed, p)
	}

	Sweep(1, 2, gen, build, func(*Sim) error { return fakeErr })

	if built != generated {
		t.Fatal("build received a different plan than gen produced")
	}
}

// two sweeps over the same range produce the same plans.
func TestSweepIsDeterministic(t *testing.T) {
	collect := func() []string {
		var out []string
		gen := func(seed uint64, r Rand) *Plan {
			p := GeneratePlan(testPlanConfig(), []int{0, 1, 2}, r)
			out = append(out, p.String())
			return p
		}
		Sweep(1, 20, gen, trivialBuild, func(*Sim) error { return nil })
		return out
	}

	a, b := collect(), collect()
	if len(a) != len(b) {
		t.Fatalf("sweeps generated %d and %d plans", len(a), len(b))
	}
	for i := range a {
		if a[i] != b[i] {
			t.Fatalf("plan %d differs between identical sweeps", i)
		}
	}
}
