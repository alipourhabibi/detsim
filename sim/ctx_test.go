package sim

import "testing"

type timerCount struct {
	fires int
}

func (n *timerCount) OnRestart(ctx *Ctx) {
	ctx.SetTimer("t", 10)
	ctx.SetTimer("t", 20) // same name: replaces, does not stack
}

func (n *timerCount) OnTimer(*Ctx, string) {
	n.fires++
}

func (n *timerCount) OnMessage(*Ctx, int, Message) {}

func TestSetTimerResetsRatherThanStacks(t *testing.T) {
	made := map[int]*timerCount{}
	s := New(testConfig(1), []int{0},
		stable(made, func(int, Deps) *timerCount {
			return &timerCount{}
		}))
	s.Start()
	if err := s.RunUntil(100); err != nil {
		t.Fatal(err)
	}

	if made[0].fires != 1 {
		t.Fatalf("timer fired %d times, want 1 (SetTimer stacked instead of "+
			"resetting)", made[0].fires)
	}
}

type cancelProbe struct {
	secondFired int
}

func (n *cancelProbe) OnRestart(ctx *Ctx) {
	ctx.SetTimer("kill", 10)
	ctx.SetTimer("second", 50)
}

func (n *cancelProbe) OnTimer(ctx *Ctx, name string) {
	switch name {
	case "kill":
		ctx.CancelTimer("second")
	case "second":
		n.secondFired++
	}
}

func (n *cancelProbe) OnMessage(*Ctx, int, Message) {}

// CancelTimer bumps the token so a pending fire goes stale.
func TestCancelTimerPreventsFire(t *testing.T) {
	made := map[int]*cancelProbe{}
	s := New(testConfig(1), []int{0},
		stable(made, func(int, Deps) *cancelProbe { return &cancelProbe{} }))
	s.Start()
	if err := s.RunUntil(100); err != nil {
		t.Fatal(err)
	}

	if made[0].secondFired != 0 {
		t.Fatalf("cancelled timer fired %d times", made[0].secondFired)
	}
}

type armOnceThenCount struct {
	armed bool
	fires int
}

func (n *armOnceThenCount) OnRestart(ctx *Ctx) {
	// only the first boot arms; a survivor would fire later
	if !n.armed {
		ctx.SetTimer("t", 100)
		n.armed = true
	}
}

func (n *armOnceThenCount) OnTimer(*Ctx, string) {
	n.fires++
}

func (n *armOnceThenCount) OnMessage(*Ctx, int, Message) {}

// // Timers set before a crash do not fire after the restart.
func TestTimersDoNotSurviveCrash(t *testing.T) {
	made := map[int]*armOnceThenCount{}
	s := New(testConfig(1), []int{0, 1},
		stable(made, func(int, Deps) *armOnceThenCount {
			return &armOnceThenCount{}
		}))
	s.Start()

	s.ScheduleFault(50, NewCrashFault(0, false, 10)) // back up at t=60
	if err := s.RunUntil(300); err != nil {
		t.Fatal(err)
	}

	if made[0].fires != 0 {
		t.Fatalf("timer armed before the crash fired %d times after restart",
			made[0].fires)
	}
}

// // A superseded timer is traced, not silently dropped.
func TestSupersededTimerIsTraced(t *testing.T) {
	made := map[int]*timerCount{}
	s := New(testConfig(1), []int{0},
		stable(made, func(int, Deps) *timerCount {
			return &timerCount{}
		}))
	s.Start()
	if err := s.RunUntil(100); err != nil {
		t.Fatal(err)
	}

	got := s.Trace().Filter(func(e Entry) bool {
		return e.Kind == EnDropped && e.Reason == "timer superseded"
	})
	if len(got) == 0 {
		t.Fatal("superseded timer was dropped without a trace entry")
	}
}

type ctxSaver struct {
	saved *Ctx
}

func (n *ctxSaver) OnRestart(ctx *Ctx) {
	n.saved = ctx
	ctx.SetTimer("t", 10)
}

func (n *ctxSaver) OnTimer(*Ctx, string) {
	n.saved.Send(1, testMsg{N: 1}) // dead Ctx from a previous callback
}

func (n *ctxSaver) OnMessage(*Ctx, int, Message) {}

// A saved Ctx would otherwise write effects into whatever buffer is current,
// which means one node's send landing in another node's turn. The generation
// counter catches it.
func TestSavedCtxPanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("using a Ctx outside its callback did not panic")
		}
	}()

	s := New(testConfig(1), []int{0, 1},
		func(int, Deps) Handler { return &ctxSaver{} })
	s.Start()
	_ = s.RunUntil(100)
}
