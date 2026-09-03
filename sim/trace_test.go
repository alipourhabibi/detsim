package sim

import "testing"

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
		func(id int, _ Deps) Handler {
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
		func(id int, _ Deps) Handler {
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
		func(id int, _ Deps) Handler {
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
		func(id int, _ Deps) Handler {
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
