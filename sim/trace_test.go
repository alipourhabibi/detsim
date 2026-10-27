package sim

import (
	"strings"
	"testing"
)

func TestTraceRingWrapsInOrder(t *testing.T) {
	tr := NewTrace(TraceHashEvents, 4)
	for i := range 10 {
		tr.Record(Event{At: Time(i), Seq: uint64(i)})
	}

	got := tr.Entries()
	if len(got) != 4 {
		t.Fatalf("len(Entries()) = %d, want 4", len(got))
	}
	if tr.Len() != len(got) {
		t.Fatalf("Len() = %d but len(Entries()) = %d", tr.Len(), len(got))
	}
	for i, want := range []uint64{6, 7, 8, 9} {
		if got[i].Event.Seq != want {
			t.Fatalf("entry %d has seq %d, want %d (ring out of order)",
				i, got[i].Event.Seq, want)
		}
	}
}

func TestTraceRingNotYetFull(t *testing.T) {
	tr := NewTrace(TraceHashEvents, 8)
	for i := range 3 {
		tr.Record(Event{At: Time(i), Seq: uint64(i)})
	}
	if got := tr.Entries(); len(got) != 3 {
		t.Fatalf("len(Entries()) = %d, want 3", len(got))
	}
	if tr.Len() != 3 {
		t.Fatalf("Len() = %d, want 3", tr.Len())
	}
}

func TestTraceKeepAll(t *testing.T) {
	tr := NewTrace(TraceHashEvents, KeepAll)
	for i := range 20 {
		tr.Record(Event{At: Time(i), Seq: uint64(i)})
	}
	if got := tr.Entries(); len(got) != 20 {
		t.Fatalf("len(Entries()) = %d, want 20", len(got))
	}
}

type senderAt struct {
	peer int
	at   Duration
}

func (n *senderAt) OnRestart(ctx *Ctx) {
	ctx.SetTimer("go", n.at)
}

func (n *senderAt) OnTimer(ctx *Ctx, _ string) {
	ctx.Send(n.peer, testMsg{N: 1})
}

func (n *senderAt) OnMessage(*Ctx, int, Message) {}

func TestStaleEpochEventIsDropped(t *testing.T) {
	s := New(delayedConfig(1, 100), []int{0, 1},
		func(id int) Handler {
			if id == 0 {
				return &senderAt{peer: 1, at: 10}
			}
			return NoOpHandler{}
		})
	s.Start()

	// Sent at t=10, arrives t=110. Crash and restart node 1 in between.
	s.ScheduleFault(20, NewCrashFault(1, false, 10))
	if err := s.RunUntil(200); err != nil {
		t.Fatal(err)
	}

	if droppedFor(s, "stale epoch") == 0 {
		t.Fatal("no stale-epoch drop; the restart did not invalidate the " +
			"in-flight message")
	}
}

func TestEventToCrashedNodeIsDropped(t *testing.T) {
	s := New(testConfig(1), []int{0, 1},
		func(id int) Handler {
			if id == 0 {
				return &senderAt{peer: 1, at: 50}
			}
			return NoOpHandler{}
		})
	s.Start()

	s.ScheduleFault(10, NewCrashFault(1, false, 0)) // stays down
	if err := s.RunUntil(200); err != nil {
		t.Fatal(err)
	}

	if droppedFor(s, "node down") == 0 {
		t.Fatal("message to a crashed node was not dropped with 'node down'")
	}
}

// Partition forms AFTER the send. The message left, then died on the wire.
func TestPartitionedInFlightIsDropped(t *testing.T) {
	s := New(delayedConfig(1, 100), []int{0, 1},
		func(id int) Handler {
			if id == 0 {
				return &senderAt{peer: 1, at: 10}
			}
			return NoOpHandler{}
		})
	s.Start()

	s.ScheduleFault(50, NewPartitionFault([]int{0}, []int{1})) // sent 10, lands 110
	if err := s.RunUntil(200); err != nil {
		t.Fatal(err)
	}

	if droppedFor(s, "partitioned in flight") == 0 {
		t.Fatal("no in-flight drop; reachability is not rechecked on arrival")
	}
	if n := droppedFor(s, "unreachable at send"); n != 0 {
		t.Fatalf("%d sends reported unreachable, but the partition formed after "+
			"the send", n)
	}
}

// Partition forms BEFORE the send. The message never left.
func TestUnreachableAtSendIsDropped(t *testing.T) {
	s := New(delayedConfig(1, 100), []int{0, 1},
		func(id int) Handler {
			if id == 0 {
				return &senderAt{peer: 1, at: 50}
			}
			return NoOpHandler{}
		})
	s.Start()

	s.ScheduleFault(10, NewPartitionFault([]int{0}, []int{1}))
	if err := s.RunUntil(200); err != nil {
		t.Fatal(err)
	}

	if droppedFor(s, "unreachable at send") == 0 {
		t.Fatal("send into a partition was not dropped at the source")
	}
	if n := droppedFor(s, "partitioned in flight"); n != 0 {
		t.Fatalf("%d in-flight drops, but nothing should have been scheduled", n)
	}
}

type namedTimer struct {
	NoOpHandler
	name string
}

func (n namedTimer) OnRestart(ctx *Ctx) { ctx.SetTimer(n.name, 10) }

// runs that fire different timers must hash differently
func TestHashIncludesTimerName(t *testing.T) {
	hash := func(name string) uint64 {
		s := New(testConfig(1), []int{0}, func(int) Handler { return namedTimer{name: name} })
		s.Start()
		if err := s.RunUntil(100); err != nil {
			t.Fatal(err)
		}
		return s.Hash()
	}
	if hash("a") == hash("b") {
		t.Fatal("timer \"a\" and timer \"b\" give the same hash")
	}
}

// a restart of a healthy node is dropped, not recorded and then dropped
func TestRestartOfHealthyNodeTracedOnce(t *testing.T) {
	s := New(testConfig(1), []int{0}, noop)
	s.Start()
	s.InjectFault(NewRestartFault(0))
	if err := s.RunUntil(10); err != nil {
		t.Fatal(err)
	}
	ran := s.Trace().Filter(func(e Entry) bool {
		return e.Kind == EnEvent && e.Event.Kind == EvRestart
	})
	if len(ran) != 0 {
		t.Fatalf("restart recorded as run %d times, want 0", len(ran))
	}
}

// an event that waited on a paused node keeps the parent that caused it
func TestDeferredEventKeepsParent(t *testing.T) {
	made := map[int]*pingOnTimer{}
	s := New(testConfig(1), []int{0, 1},
		stable(made, func(id int) *pingOnTimer { return &pingOnTimer{peer: 1 - id} }))
	s.Start()
	if err := s.RunUntil(50); err != nil {
		t.Fatal(err)
	}
	s.InjectFault(NewPauseFault(0, 100))
	if err := s.RunUntil(200); err != nil {
		t.Fatal(err)
	}

	bySeq := map[uint64]Event{}
	for _, e := range s.Trace().Filter(func(e Entry) bool { return e.Kind == EnEvent }) {
		bySeq[e.Event.Seq] = e.Event
	}
	for _, e := range s.Trace().Filter(func(e Entry) bool {
		return e.Kind == EnEvent && e.Event.Kind == EvDeliver && e.Event.Target == 0 && e.At == 150
	}) {
		if p := bySeq[e.Event.Parent]; p.Kind != EvTimer || p.Target != 1 {
			t.Fatalf("deferred delivery has parent %+v, want n1's timer", p)
		}
	}
}

// a broadcast to two nodes at the same time is two arrows, not one arrow and
// a count
func TestMermaidKeepsBroadcastArrows(t *testing.T) {
	tr := NewTrace(TraceHashEvents, KeepAll)
	tr.Record(Event{At: 10, Seq: 1, Kind: EvDeliver, Source: 0, Target: 1, Payload: testMsg{N: 1}})
	tr.Record(Event{At: 10, Seq: 2, Kind: EvDeliver, Source: 0, Target: 2, Payload: testMsg{N: 1}})
	var b strings.Builder
	if err := tr.Mermaid(&b, []int{0, 1, 2}, MermaidOpts{}); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(b.String(), "n0->>n2") {
		t.Fatalf("arrow to n2 is missing:\n%s", b.String())
	}
}

// the same retry at different times collapses into one arrow and a count
func TestMermaidCollapsesRetriesOverTime(t *testing.T) {
	tr := NewTrace(TraceHashEvents, KeepAll)
	for i := range 3 {
		tr.Record(Event{At: Time(10 * (i + 1)), Seq: uint64(i + 1), Kind: EvDeliver, Source: 0, Target: 1, Payload: testMsg{N: 1}})
	}
	var b strings.Builder
	if err := tr.Mermaid(&b, []int{0, 1}, MermaidOpts{}); err != nil {
		t.Fatal(err)
	}
	out := b.String()
	if n := strings.Count(out, "n0->>n1"); n != 1 {
		t.Fatalf("%d arrows, want 1:\n%s", n, out)
	}
	if !strings.Contains(out, "x3") {
		t.Fatalf("no x3 count:\n%s", out)
	}
}

// a span closes when its timer fires, not only when it is cancelled
func TestMermaidSpanClosesWhenTimerFires(t *testing.T) {
	tr := NewTrace(TraceHashEvents, KeepAll)
	tr.Note(EnTimerSet, 10, 1, "lease", 1)
	tr.Record(Event{At: 50, Seq: 2, Kind: EvTimer, Target: 1, Source: -1, Name: "lease"})
	var b strings.Builder
	if err := tr.Mermaid(&b, []int{0, 1}, MermaidOpts{SpanTimer: "lease"}); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(b.String(), "deactivate n1") {
		t.Fatalf("span never closed:\n%s", b.String())
	}
}

// faults belong to no node, so they must not show up in node 0's lane
func TestLanesFaultNotInNodeZero(t *testing.T) {
	tr := NewTrace(TraceHashEvents, KeepAll)
	tr.Record(Event{At: 10, Seq: 1, Kind: EvFault, Target: -1, Source: -1, Payload: NewPauseFault(1, 5)})
	var b strings.Builder
	if err := tr.Lanes(&b, []int{0, 1}); err != nil {
		t.Fatal(err)
	}
	line := strings.Split(strings.TrimSpace(b.String()), "\n")[1]
	if !strings.Contains(line, "fault") || !strings.Contains(line, "paused") {
		t.Fatalf("fault line is %q, want the fault text", line)
	}
}
