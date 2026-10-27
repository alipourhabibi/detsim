package sim

import (
	"errors"
	"fmt"
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

type panicky struct {
	onBoot  bool // panic in the first OnRestart
	negTime bool // set a negative timer from OnRestart
}

func (n *panicky) OnRestart(ctx *Ctx) {
	if n.onBoot {
		panic("boom at boot")
	}
	if n.negTime {
		ctx.SetTimer("t", -1)
	}
}

func (n *panicky) OnTimer(*Ctx, string)         {}
func (n *panicky) OnMessage(*Ctx, int, Message) {}

// these panics must come back as run errors. Sweep runs sims in goroutines,
// so a panic that escapes kills the whole sweep.
func TestPanicsBecomeRunErrors(t *testing.T) {
	cases := []struct {
		name  string
		start func() *Sim
		want  error
	}{
		{"panic in first OnRestart", func() *Sim {
			s := New(testConfig(1), []int{0},
				func(int) Handler { return &panicky{onBoot: true} })
			s.Start()
			return s
		}, ErrHandlerPanic},
		{"negative timer", func() *Sim {
			s := New(testConfig(1), []int{0},
				func(int) Handler { return &panicky{negTime: true} })
			s.Start()
			return s
		}, ErrHandlerPanic},
		{"factory panics on restart", func() *Sim {
			boots := 0
			s := New(testConfig(1), []int{0}, func(int) Handler {
				if boots++; boots > 1 {
					panic("factory boom")
				}
				return NoOpHandler{}
			})
			s.Start()
			s.InjectFault(NewCrashFault(0, false, 10))
			return s
		}, ErrHandlerPanic},
		{"fault Apply panics", func() *Sim {
			s := New(testConfig(1), []int{0}, noop)
			s.Start()
			s.InjectFault(NewPauseFault(0, 0))
			return s
		}, ErrFaultPanic},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var s *Sim
			func() {
				defer func() {
					if r := recover(); r != nil {
						t.Fatalf("panic escaped: %v", r)
					}
				}()
				s = c.start()
			}()
			err := s.RunUntil(100)
			if !errors.Is(err, c.want) {
				t.Fatalf("got %v, want %v", err, c.want)
			}
		})
	}
}

// after a panic in Start, every run method returns the error
func TestBootErrorIsSticky(t *testing.T) {
	s := New(testConfig(1), []int{0},
		func(int) Handler { return &panicky{onBoot: true} })
	s.Start()
	for name, run := range map[string]func() error{
		"RunUntil":          func() error { return s.RunUntil(100) },
		"Step":              func() error { return s.Step(1) },
		"RunUntilQuiescent": s.RunUntilQuiescent,
		"RunHealed":         func() error { return s.RunHealed(100) },
	} {
		if err := run(); !errors.Is(err, ErrHandlerPanic) {
			t.Errorf("%s: got %v, want ErrHandlerPanic", name, err)
		}
	}
}

// time never goes back
func TestRunUntilEarlierTimeKeepsNow(t *testing.T) {
	s := New(testConfig(1), []int{0}, noop)
	s.Start()
	if err := s.RunUntil(50); err != nil {
		t.Fatal(err)
	}
	if err := s.RunUntil(10); err != nil {
		t.Fatal(err)
	}
	if s.Now() != 50 {
		t.Fatalf("now = %d after RunUntil(10), want 50", s.Now())
	}
}

type randInState struct{ NoOpHandler }

func (randInState) StateString(ctx *Ctx) string {
	return fmt.Sprint(ctx.Rand().Int64N(10))
}

// StateString only runs when tracing is on, so drawing from Rand there
// would make the trace level change the run.
func TestReadCtxRandPanics(t *testing.T) {
	s := New(testConfig(1), []int{0}, func(int) Handler { return randInState{} })
	defer wantPanic(t, "Rand on a read-only Ctx")()
	s.Start()
}

// a fault planned after the run horizon must not fire in the healed phase
func TestRunHealedCancelsPlannedFaults(t *testing.T) {
	s := New(testConfig(1), []int{0, 1}, noop)
	s.Start()
	s.ScheduleFault(7000, NewCrashFault(0, false, 0)) // 0 downtime: stays down
	if err := s.RunUntil(5000); err != nil {
		t.Fatal(err)
	}
	if err := s.RunHealed(5000); err != nil {
		t.Fatal(err)
	}
	if got := s.Status(0); got != Healthy {
		t.Fatalf("n0 is %v after RunHealed, want Healthy", got)
	}
}

type bootWriter struct{ NoOpHandler }

func (bootWriter) OnRestart(ctx *Ctx) {
	ctx.Put("booted", []byte{1})
	ctx.Sync()
}

// a bad state made by the first OnRestart is caught at t=0, not blamed
// on the first event
func TestInvariantsRunAfterStart(t *testing.T) {
	s := New(testConfig(1), []int{0}, func(int) Handler { return bootWriter{} })
	s.AddInvariant(func(s *Sim) error {
		if _, ok := s.Get(0, "booted"); ok {
			return errors.New("booted")
		}
		return nil
	})
	s.Start()
	err := s.RunUntil(100)
	var ie *InvariantError
	if !errors.As(err, &ie) {
		t.Fatalf("got %v, want InvariantError", err)
	}
	if ie.At != 0 || ie.Seq != 0 {
		t.Fatalf("invariant blamed on t=%d #%d, want t=0 #0", ie.At, ie.Seq)
	}
}

// callers can't reorder the sim's node list
func TestNodesReturnsCopy(t *testing.T) {
	s := New(testConfig(1), []int{0, 1, 2}, noop)
	s.Nodes()[0] = 9
	if s.Nodes()[0] != 0 {
		t.Fatal("Nodes returned the internal slice")
	}
}
