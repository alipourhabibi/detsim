package sim

import "testing"

type putThenSend struct {
	peer int
}

func (n *putThenSend) OnRestart(ctx *Ctx) {
	ctx.SetTimer("go", 10)
}

func (n *putThenSend) OnTimer(ctx *Ctx, _ string) {
	ctx.Send(n.peer, testMsg{N: 1}) // written FIRST, must apply SECOND
	ctx.Put("vote", []byte("3"))
	ctx.Sync()
}

func (n *putThenSend) OnMessage(*Ctx, int, Message) {}

// The load-bearing half of the drain order: a node cannot reply to a vote it
// has not yet persisted. The send is written first here and must still apply
// after the write.
func TestPutAppliesBeforeSend(t *testing.T) {
	s := New(testConfig(1), []int{0, 1},
		func(id int, _ Deps) Handler {
			if id == 0 {
				return &putThenSend{peer: 1}
			}
			return NoOpHandler{}
		})
	s.Start()
	if err := s.RunUntil(100); err != nil {
		t.Fatal(err)
	}

	var wroteAt, sentAt = -1, -1
	for i, e := range s.Trace().Entries() {
		switch {
		case e.Kind == EnDurableWrite && wroteAt < 0:
			wroteAt = i
		case e.Kind == EnSent && sentAt < 0:
			sentAt = i
		}
	}
	if wroteAt < 0 || sentAt < 0 {
		t.Fatalf("missing entries: write=%d send=%d", wroteAt, sentAt)
	}
	if wroteAt > sentAt {
		t.Fatal("send applied before the durable write; drain order is wrong")
	}
}

type cancelThenSet struct {
	fires int
}

func (n *cancelThenSet) OnRestart(ctx *Ctx) {
	ctx.SetTimer("t", 10)
}

func (n *cancelThenSet) OnTimer(ctx *Ctx, name string) {
	n.fires++
	ctx.CancelTimer(name)
	ctx.SetTimer(name, 10) // cancel drains first, so this survives
}

func (n *cancelThenSet) OnMessage(*Ctx, int, Message) {}

// EfCancelTimer drains before EfSetTimer, so a callback that cancels and
// re-arms the same timer ends with it armed.
func TestCancelThenSetLeavesTimerArmed(t *testing.T) {
	made := map[int]*cancelThenSet{}
	s := New(testConfig(1), []int{0},
		stable(made, func(int, Deps) *cancelThenSet { return &cancelThenSet{} }))
	s.Start()
	if err := s.RunUntil(100); err != nil {
		t.Fatal(err)
	}

	if made[0].fires < 5 {
		t.Fatalf("timer fired %d times in 100ms at 10ms intervals; the re-arm "+
			"was cancelled", made[0].fires)
	}
}

type bufferReuser struct {
	buf []byte
}

func (n *bufferReuser) OnRestart(ctx *Ctx) {
	n.buf = []byte("first")
	ctx.Put("k", n.buf)
	ctx.Sync()
	copy(n.buf, "XXXXX") // caller mutates its buffer after handing it over
}

func (n *bufferReuser) OnMessage(*Ctx, int, Message) {}
func (n *bufferReuser) OnTimer(*Ctx, string)         {}

// Put clones. A handler reusing a buffer would otherwise corrupt an earlier
// write, and that corruption would be schedule-dependent.
func TestPutClonesValue(t *testing.T) {
	s := New(testConfig(1), []int{0, 1},
		func(int, Deps) Handler { return &bufferReuser{} })
	s.Start()

	v, ok := s.Get(0, "k")
	if !ok {
		t.Fatal("nothing stored")
	}
	if string(v) != "first" {
		t.Fatalf("stored value = %q, want \"first\"; Put aliased the caller's "+
			"buffer", v)
	}
}
