package sim

import (
	"errors"
	"testing"
)

func TestRunUntilAdvancesClockWithEmptyQueue(t *testing.T) {
	s := New(testConfig(1), []int{0, 1}, noop)
	s.Start()

	if err := s.RunUntil(500); err != nil {
		t.Fatal(err)
	}
	if got := s.Now(); got != 500 {
		t.Fatalf("Now() = %d, want 500", got)
	}
}

func TestStepStopsEarlyWhenQueueDrains(t *testing.T) {
	s := New(testConfig(1), []int{0, 1}, noop)
	s.Start()

	before := s.Now()
	if err := s.Step(100); err != nil { // nothing to do
		t.Fatalf("Step on an empty queue returned %v, want nil", err)
	}
	if s.Now() != before {
		t.Fatalf("Now() moved from %d to %d on an empty queue", before, s.Now())
	}
}

type runaway struct{}

func (runaway) OnRestart(ctx *Ctx) {
	ctx.SetTimer("spin", 0)
}
func (runaway) OnTimer(ctx *Ctx, name string) {
	ctx.SetTimer(name, 0)
}
func (runaway) OnMessage(*Ctx, int, Message) {}

// A zero-delay timer re-armed from its own handler is a realistic protocol bug.
// Without the cap it hangs the test binary instead of failing it.
func TestMaxEventsStopsRunawayLoop(t *testing.T) {
	cfg := testConfig(1)
	cfg.MaxEvents = 1000
	s := New(cfg, []int{0}, func(int, Deps) Handler { return runaway{} })
	s.Start()

	err := s.RunUntil(1_000_000)
	if !errors.Is(err, ErrMaxEvents) {
		t.Fatalf("err = %v, want ErrMaxEvents", err)
	}
	if s.Trace().Len() == 0 {
		t.Fatal("trace is empty; it should survive the cap for diagnosis")
	}
}

// RunHealed repairs through the queue, not by mutating state directly, so every
// repair lands in the trace and the hash like any other fault.
func TestRunHealedRepairsEverythingViaTrace(t *testing.T) {
	s := New(testConfig(1), []int{0, 1, 2}, noop)
	s.Start()

	s.InjectFault(NewCrashFault(0, false, 0)) // stays down
	s.InjectFault(NewPauseFault(1, 100_000))  // effectively forever
	s.InjectFault(NewPartitionFault([]int{0}, []int{1, 2}))
	if err := s.RunUntil(10); err != nil {
		t.Fatal(err)
	}

	before := s.Hash()

	if err := s.RunHealed(1000); err != nil {
		t.Fatal(err)
	}

	for _, id := range s.Nodes() {
		if got := s.Status(id); got != Healthy {
			t.Errorf("node %d status = %v after RunHealed, want Healthy", id, got)
		}
	}
	if !s.network.reachable(0, 1, s.Now()) {
		t.Error("partition survived RunHealed")
	}
	if s.Hash() == before {
		t.Error("RunHealed did not fold its repairs into the hash; it is " +
			"mutating state instead of scheduling faults")
	}
}

func TestNewRejectsEmptyNodeList(t *testing.T) {
	defer wantPanic(t, "empty node list")()
	New(testConfig(1), nil, noop)
}

func TestNewRejectsDuplicateIDs(t *testing.T) {
	defer wantPanic(t, "duplicate id")()
	New(testConfig(1), []int{0, 1, 1}, noop)
}

// -1 is the no-node sentinel in Event.Source and Event.Target, so a node
// cannot hold it without becoming indistinguishable from "none" in the trace.
func TestNewRejectsNegativeIDs(t *testing.T) {
	defer wantPanic(t, "negative id")()
	New(testConfig(1), []int{-1, 0}, noop)
}

func TestNewRejectsNilFactory(t *testing.T) {
	defer wantPanic(t, "nil factory")()
	New(testConfig(1), []int{0, 1}, nil)
}

func TestStartTwicePanics(t *testing.T) {
	s := New(testConfig(1), []int{0, 1}, noop)
	s.Start()

	defer wantPanic(t, "second Start")()
	s.Start()
}
