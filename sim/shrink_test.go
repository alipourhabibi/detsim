package sim

import (
	"testing"
)

// builds a pred function that fails on given nodes
func failsIfContains(nodes ...int) func(*Plan) bool {
	return func(p *Plan) bool {
		seen := map[int]bool{}
		for _, f := range p.Faults {
			if c, ok := f.Fault.(crashFault); ok {
				seen[c.Node] = true
			}
		}
		for _, n := range nodes {
			if !seen[n] {
				return false
			}
		}
		return true
	}
}

// padded builds a plan with 20 pauses that do not matter, plus one crash per
// listed node. Shrink should end up with only the crashes.
func padded(crashNodes ...int) *Plan {
	p := &Plan{}
	at := Time(0)
	for i := range 20 {
		at += 100
		p.Faults = append(p.Faults, Scheduled{at, NewPauseFault(i%5, 10)})
	}
	for _, n := range crashNodes {
		at += 100
		p.Faults = append(p.Faults, Scheduled{at, NewCrashFault(n, false, 50)})
	}
	return p
}

func TestShrinkPreservesFailure(t *testing.T) {
	fails := failsIfContains(2, 3)
	p := padded(2, 3)

	if !fails(p) {
		t.Fatal("setup: the original plan does not fail")
	}

	got := Shrink(p, fails)
	if !fails(got) {
		t.Fatal("shrunk plan no longer fails")
	}
}

func TestShrinkRemovesIrrelevantFaults(t *testing.T) {
	fails := failsIfContains(2, 3)
	p := padded(2, 3)

	got := Shrink(p, fails)
	if got.Len() != 2 {
		t.Fatalf("shrink left %d faults, want 2 (only the two crashes)\n%s",
			got.Len(), got)
	}
}

func TestShrinkDoesNotMutateInput(t *testing.T) {
	fails := failsIfContains(2)
	p := padded(2)
	before := p.String()

	Shrink(p, fails)

	if p.String() != before {
		t.Fatal("Shrink mutated its input plan")
	}
}

func TestShrinkOnMinimalPlanChangesNothing(t *testing.T) {
	fails := failsIfContains(1, 2, 3)
	p := &Plan{Faults: []Scheduled{
		{100, NewCrashFault(1, false, 10)},
		{200, NewCrashFault(2, false, 10)},
		{300, NewCrashFault(3, false, 10)},
	}}
	before := p.String()

	got := Shrink(p, fails)

	if got.String() != before {
		t.Fatalf("shrink changed a minimal plan:\nwas:\n%s\nnow:\n%s", before, got)
	}
}

// A plan that never fails has nothing removable, so every removal is rejected
// and the plan comes back whole.
func TestShrinkOnPassingPlanChangesNothing(t *testing.T) {
	never := func(*Plan) bool { return false }
	p := padded()
	before := p.String()

	got := Shrink(p, never)

	if got.String() != before {
		t.Fatalf("shrink changed a plan that never fails:\nwas:\n%s\nnow:\n%s",
			before, got)
	}
}

func TestShrinkIsDeterministic(t *testing.T) {
	fails := failsIfContains(2, 3)
	a := Shrink(padded(2, 3), fails)
	b := Shrink(padded(2, 3), fails)

	if a.String() != b.String() {
		t.Fatal("Shrink produced different results for the same input")
	}
}
