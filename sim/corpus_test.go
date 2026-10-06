package sim

import (
	"bytes"
	"testing"
)

// Every fault kind must come back from the file the same as it went in.
func TestCorpusKeepsThePlan(t *testing.T) {
	plan := &Plan{
		Faults: []Scheduled{
			{10, NewCrashFault(2, true, 150)},
			{20, NewPauseFault(1, 40)},
			{30, NewRestartFault(2)},
			{40, resumeFault{Node: 1, Token: 3}},
			{50, NewPartitionFault([]int{0, 1}, []int{2, 3})},
			{60, NewIsolateFault(3)},
			{70, NewHealFault(0)},
			{80, NewHealFault(4)},
			{90, NewDropLinkFault(0, 1, 183_456)},
			{100, NewDuplicateLinkFault(1, 0, 7)},
			{110, NewResetConnectionFault(0, 2)},
			{120, blockLinkFault{From: 2, To: 0}},
			{130, quiesceFault{}},
		},
		Buggify: []string{"lockserver/skip-sync"},
	}

	var buf bytes.Buffer
	err := SaveCorpus(&buf, "test", []SeedEntry{{Seed: 7, Reason: "boom", Plan: plan}})
	if err != nil {
		t.Fatal(err)
	}

	got, err := LoadCorpus(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Seed != 7 || got[0].Plan == nil {
		t.Fatalf("loaded %+v", got)
	}
	if got[0].Plan.String() != plan.String() {
		t.Fatalf("plan changed on the way through:\nwas:\n%s\nnow:\n%s", plan, got[0].Plan)
	}
	for i, f := range got[0].Plan.Faults {
		if !f.Fault.Equal(plan.Faults[i].Fault) {
			t.Errorf("fault %d: got %v, want %v", i, f.Fault, plan.Faults[i].Fault)
		}
	}
}
