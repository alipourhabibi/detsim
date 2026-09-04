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
	s.Start()
	p.Apply(s)

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
