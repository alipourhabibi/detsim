package sim

import (
	"slices"
	"testing"
)

type timerOrder struct {
	names []string
	arm   []string
}

func (n *timerOrder) OnRestart(ctx *Ctx) {
	for _, name := range n.arm {
		ctx.SetTimer(name, 10) // all three land on the same instant
	}
}

func (n *timerOrder) OnTimer(_ *Ctx, name string) {
	n.names = append(n.names, name)
}

func (n *timerOrder) OnMessage(*Ctx, int, Message) {}

// Two events at the same instant must pop in Seq order, always. If this ever
// depends on heap internals, every run becomes irreproducible.
func TestSameInstantResolvesBySeq(t *testing.T) {
	made := map[int]*timerOrder{}
	s := New(testConfig(1), []int{0},
		stable(made, func(int, Deps) *timerOrder {
			return &timerOrder{arm: []string{"a", "b", "c"}}
		}))
	s.Start()

	if err := s.RunUntil(50); err != nil {
		t.Fatal(err)
	}

	got := made[0].names
	want := []string{"a", "b", "c"}
	if !slices.Equal(got, want) {

	}
}

// push panics rather than silently scheduling in the past.
func TestScheduleInThePastPanics(t *testing.T) {
	s := New(testConfig(1), []int{0, 1}, noop)
	s.Start()
	if err := s.RunUntil(100); err != nil {
		t.Fatal(err)
	}

	defer func() {
		if recover() == nil {
			t.Fatal("scheduling before now did not panic")
		}
	}()
	s.push(Event{At: 50, Kind: EvTimer, Target: 0})
}

type parentProbe struct {
	peer int
}

func (n *parentProbe) OnRestart(ctx *Ctx) {
	ctx.SetTimer("t", 10)
}

func (n *parentProbe) OnTimer(ctx *Ctx, _ string) {
	ctx.Send(n.peer, testMsg{N: 1})
}

func (n *parentProbe) OnMessage(*Ctx, int, Message) {}

// Event.Parent links an event to the one whose handler caused it, so a trace
// can be read as a causal tree.
func TestParentLinksToCausingEvent(t *testing.T) {
	s := New(testConfig(1), []int{0, 1}, func(id int, _ Deps) Handler {
		return &parentProbe{peer: 1 - id}
	})
	s.Start()
	if err := s.RunUntil(100); err != nil {
		t.Fatal(err)
	}

	bySeq := map[uint64]Event{}
	var deliveries []Event
	for _, e := range s.Trace().Entries() {
		if e.Kind != EnEvent {
			continue
		}
		bySeq[e.Event.Seq] = e.Event
		if e.Event.Kind == EvDeliver {
			deliveries = append(deliveries, e.Event)
		}
	}

	if len(deliveries) == 0 {
		t.Fatal("no delivery in the trace")
	}

	for _, d := range deliveries {
		parent, ok := bySeq[d.Parent]
		if !ok {
			t.Fatalf("delivery seq=%d has Parent=%d, which is not in the trace",
				d.Seq, d.Parent)
		}
		if parent.Kind != EvTimer {
			t.Fatalf("delivery seq=%d points at a %v, want the timer that sent it",
				d.Seq, parent.Kind)
		}
		// The timer ran on the node that sent the message.
		if parent.Target != d.Source {
			t.Fatalf("delivery seq=%d from node %d points at a timer on node %d",
				d.Seq, d.Source, parent.Target)
		}
	}
}
