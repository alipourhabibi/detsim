package sim

import "testing"

type cancelThenSet struct {
	fires int
}

func (n *cancelThenSet) OnRestart(ctx *Ctx) {
	ctx.SetTimer("t", 10)
}

func (n *cancelThenSet) OnTimer(ctx *Ctx, name string) {
	n.fires++
	ctx.CancelTimer(name)
	ctx.SetTimer(name, 10) // the set comes after, so it wins
}

func (n *cancelThenSet) OnMessage(*Ctx, int, Message) {}

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

func TestCancelAfterSetInTheSameTurn(t *testing.T) {
	fired := 0

	s := New(testConfig(1), []int{0},
		func(id int, deps Deps) Handler {
			return effectHandler{
				onRestart: func(c *Ctx) {
					c.SetTimer("t", 10)
					c.CancelTimer("t")
				},
				onTimer: func(c *Ctx, name string) { fired++ },
			}
		})
	s.Start()

	if err := s.RunUntil(100); err != nil {
		t.Fatal(err)
	}

	if fired != 0 {
		t.Fatalf("the handler cancelled the timer after setting it, but it fired %d time(s)", fired)
	}
}

type orderedEffects struct {
	peer      int
	sendFirst bool
}

func (n *orderedEffects) OnRestart(ctx *Ctx) {
	ctx.SetTimer("go", 10)
}

func (n *orderedEffects) OnTimer(ctx *Ctx, _ string) {
	if n.sendFirst {
		ctx.Send(n.peer, testMsg{N: 1})
	}
	ctx.Put("vote", []byte("3"))
	ctx.Sync()
	if !n.sendFirst {
		ctx.Send(n.peer, testMsg{N: 1})
	}
}

func (n *orderedEffects) OnMessage(*Ctx, int, Message) {}

func TestEffectsApplyInEmissionOrder(t *testing.T) {
	cases := []struct {
		name      string
		sendFirst bool
	}{
		{"send then write", true},
		{"write then send", false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := New(testConfig(1), []int{0, 1},
				func(id int, _ Deps) Handler {
					if id == 0 {
						return &orderedEffects{peer: 1, sendFirst: tc.sendFirst}
					}
					return NoOpHandler{}
				})
			s.Start()
			if err := s.RunUntil(100); err != nil {
				t.Fatal(err)
			}

			wroteAt, sentAt := -1, -1
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

			if tc.sendFirst && sentAt > wroteAt {
				t.Fatal("the callback sent first; the send was applied after the write")
			}
			if !tc.sendFirst && wroteAt > sentAt {
				t.Fatal("the callback wrote first; the write was applied after the send")
			}
		})
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
