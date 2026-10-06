package sim

import (
	"errors"
	"io"
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
	s := New(cfg, []int{0}, func(int) Handler { return runaway{} })
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

// Skew gives each node its own fixed offset, the same for the same seed.
func TestClockSkew(t *testing.T) {
	offsets := func() []Duration {
		cfg := testConfig(5)
		cfg.MaxClockSkew = 1000
		s := New(cfg, []int{0, 1, 2}, noop)
		var out []Duration
		for _, id := range s.Nodes() {
			o := Duration(s.nodeNow(id) - s.Now())
			if o < -1000 || o >= 1000 {
				t.Fatalf("node %d offset %d is out of range", id, o)
			}
			out = append(out, o)
		}
		return out
	}

	a, b := offsets(), offsets()
	if a[0] == a[1] && a[1] == a[2] {
		t.Fatalf("all nodes got the same offset %v", a)
	}
	for i := range a {
		if a[i] != b[i] {
			t.Fatalf("same seed gave offsets %v then %v", a, b)
		}
	}
}

type sliceMsg struct{ Vals []int64 }

func (m sliceMsg) HashInto(w io.Writer) {
	h := hashTag(w, 'S')
	for _, v := range m.Vals {
		h.i64(v)
	}
}

func (m sliceMsg) Equal(o any) bool { _, ok := o.(sliceMsg); return ok }

// The sender keeps the slice and changes it while the message is on the wire.
func TestMessageChangedAfterSend(t *testing.T) {
	vals := []int64{1}
	s := New(delayedConfig(1, 10), []int{0, 1}, func(id int) Handler {
		if id != 0 {
			return NoOpHandler{}
		}
		return effectHandler{
			onRestart: func(c *Ctx) {
				c.Send(1, sliceMsg{Vals: vals})
				c.SetTimer("change", 5)
			},
			onTimer: func(*Ctx, string) { vals[0] = 2 },
		}
	})
	s.Start()

	if err := s.RunUntilQuiescent(); !errors.Is(err, ErrMessageChanged) {
		t.Fatalf("got %v, want ErrMessageChanged", err)
	}
}
