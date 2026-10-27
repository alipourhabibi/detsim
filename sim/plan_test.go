package sim

import (
	"strings"
	"testing"
)

func TestPlanCloneIsIndependent(t *testing.T) {
	p := &Plan{Faults: []Scheduled{
		{At: 10, Fault: NewCrashFault(0, false, 100)},
		{At: 20, Fault: NewHealFault(0)},
	}}

	c := p.Clone()
	c.Faults[0].At = 999

	if p.Faults[0].At != 10 {
		t.Fatal("mutating the clone changed the original; Shrink would corrupt " +
			"the plan it is shrinking")
	}
	if c.Len() != p.Len() {
		t.Fatalf("clone has %d faults, original has %d", c.Len(), p.Len())
	}
}

// The plan is what a failing test prints, so every fault must be identifiable
// from the output alone.
func TestPlanStringNamesEveryFault(t *testing.T) {
	p := &Plan{Faults: []Scheduled{
		{At: 10, Fault: NewCrashFault(3, true, 100)},
		{At: 20, Fault: NewPartitionFault([]int{0, 1}, []int{2, 3})},
		{At: 30, Fault: NewHealFault(0)},
	}}

	out := p.String()
	for _, want := range []string{"t=10", "t=20", "t=30"} {
		if !strings.Contains(out, want) {
			t.Errorf("plan output is missing %q:\n%s", want, out)
		}
	}
	if lines := strings.Count(strings.TrimSpace(out), "\n") + 1; lines != 3 {
		t.Errorf("plan printed %d lines, want 3:\n%s", lines, out)
	}
}

func TestPlanApplySchedulesEverything(t *testing.T) {
	p := &Plan{Faults: []Scheduled{
		{At: 100, Fault: NewCrashFault(0, false, 50)},
		{At: 300, Fault: NewPauseFault(1, 50)},
	}}

	s := New(testConfig(1), []int{0, 1}, noop)
	s.StartWith(p)

	if err := s.RunUntil(1000); err != nil {
		t.Fatal(err)
	}

	// Each planned fault must appear at its planned instant. Counting all
	// EvFault entries would be wrong: a pause schedules its own resume, and a
	// crash with downtime schedules its own restart.
	for _, want := range p.Faults {
		found := false
		for _, e := range s.Trace().Entries() {
			if e.Kind != EnEvent || e.Event.Kind != EvFault {
				continue
			}
			if e.Event.At == want.At && want.Fault.Equal(e.Event.Payload) {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("planned fault %v at t=%d never reached the queue",
				want.Fault, want.At)
		}
	}
}

// isolated nodes count toward MaxDown like crashed ones, until a heal-all
func TestGeneratePlanMaxDownCountsIsolate(t *testing.T) {
	cfg := PlanConfig{
		Until: 20_000, MeanGap: 20, MaxDown: 1, MaxDowntime: 2000,
		Weights: Weights{Crash: 1, Isolate: 1},
	}
	nodes := []int{0, 1, 2, 3, 4}
	for seed := uint64(1); seed < 20; seed++ {
		p := GeneratePlan(cfg, nodes, seed, NewStream(seed, StreamFault))
		downUntil := map[int]Time{}
		isolated := map[int]bool{}
		for _, f := range p.Faults {
			for id, until := range downUntil {
				if until <= f.At {
					delete(downUntil, id)
				}
			}
			switch x := f.Fault.(type) {
			case crashFault:
				downUntil[x.Node] = f.At.Add(x.Downtime)
			case isolateFault:
				isolated[x.Node] = true
			case healFault:
				clear(isolated)
			}
			out := map[int]bool{}
			for id := range downUntil {
				out[id] = true
			}
			for id := range isolated {
				out[id] = true
			}
			if len(out) > cfg.MaxDown {
				t.Fatalf("seed %d t=%d: %d nodes out, MaxDown is %d", seed, f.At, len(out), cfg.MaxDown)
			}
		}
	}
}

// WipeChance 0 means no wipes, as the doc says
func TestGeneratePlanWipeChanceZeroNeverWipes(t *testing.T) {
	cfg := PlanConfig{Until: 50_000, MeanGap: 20, MaxDown: 3, Weights: Weights{Crash: 1}}
	for seed := uint64(1); seed < 10; seed++ {
		p := GeneratePlan(cfg, []int{0, 1, 2, 3, 4}, seed, NewStream(seed, StreamFault))
		for _, f := range p.Faults {
			if c, ok := f.Fault.(crashFault); ok && c.WipeDisk {
				t.Fatalf("seed %d: wipe at t=%d with WipeChance 0", seed, f.At)
			}
		}
	}
}

// WipeChance N is 1 in N, not 1 in N+1: with N=1 every crash wipes
func TestGeneratePlanWipeChanceOneAlwaysWipes(t *testing.T) {
	cfg := PlanConfig{Until: 5_000, MeanGap: 20, MaxDown: 3, WipeChance: 1, Weights: Weights{Crash: 1}}
	p := GeneratePlan(cfg, []int{0, 1, 2, 3, 4}, 1, NewStream(1, StreamFault))
	for _, f := range p.Faults {
		if c, ok := f.Fault.(crashFault); ok && !c.WipeDisk {
			t.Fatalf("crash at t=%d did not wipe with WipeChance 1", f.At)
		}
	}
}
