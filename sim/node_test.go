package sim

import (
	"testing"
)

type bootRecorder struct {
	boots     int
	sawOnBoot []byte
	sawOK     bool
}

func (n *bootRecorder) OnRestart(ctx *Ctx) {
	n.boots++
	v, ok := ctx.Get("k")
	n.sawOnBoot, n.sawOK = append([]byte(nil), v...), ok
	ctx.Put("k", []byte("v1"))
	ctx.Sync()
}

func (n *bootRecorder) OnMessage(*Ctx, int, Message) {}
func (n *bootRecorder) OnTimer(*Ctx, string)         {}

func TestStoreIsUsableFromFirstBoot(t *testing.T) {
	made := map[int]*bootRecorder{}
	s := New(testConfig(1), []int{0, 1},
		stable(made, func(int, Deps) *bootRecorder { return &bootRecorder{} }))
	s.Start()

	v, ok := s.Get(0, "k")
	if !ok || string(v) != "v1" {
		t.Fatalf("first boot did not persist: got %q ok=%v", v, ok)
	}
}

func TestDurableStateSurvivesRestart(t *testing.T) {
	made := map[int]*bootRecorder{}
	s := New(testConfig(1), []int{0, 1},
		stable(made, func(int, Deps) *bootRecorder { return &bootRecorder{} }))
	s.Start()

	s.InjectFault(NewCrashFault(0, false, 100)) // down, back up at t=100
	if err := s.RunUntil(200); err != nil {
		t.Fatal(err)
	}

	if got := s.Status(0); got != Healthy {
		t.Fatalf("node did not come back up: status=%v", got)
	}
	if made[0].boots != 2 {
		t.Fatalf("OnRestart called %d times, want 2 (restart left node inert)",
			made[0].boots)
	}
	if !made[0].sawOK || string(made[0].sawOnBoot) != "v1" {
		t.Fatalf("durable state lost across restart: got %q ok=%v",
			made[0].sawOnBoot, made[0].sawOK)
	}
}

type twoPhaseWriter struct{}

func (twoPhaseWriter) OnRestart(ctx *Ctx) {
	ctx.Put("synced", []byte("a"))
	ctx.Sync()
	ctx.SetTimer("t", 10)
}

func (twoPhaseWriter) OnTimer(ctx *Ctx, _ string) {
	ctx.Put("dirty", []byte("b")) // never synced
}

func (twoPhaseWriter) OnMessage(*Ctx, int, Message) {}

func TestCrashLosesUnsyncedWrites(t *testing.T) {
	s := New(testConfig(1), []int{0, 1},
		func(int, Deps) Handler { return twoPhaseWriter{} })
	s.Start()

	if err := s.RunUntil(20); err != nil {
		t.Fatal(err)
	}
	if _, ok := s.Get(0, "dirty"); !ok {
		t.Fatal("setup: unsynced write never landed")
	}

	s.InjectFault(NewCrashFault(0, false, 0)) // stays down
	if err := s.RunUntil(30); err != nil {
		t.Fatal(err)
	}

	if _, ok := s.Get(0, "synced"); !ok {
		t.Error("synced write did not survive the crash")
	}
	if v, ok := s.Get(0, "dirty"); ok {
		t.Errorf("unsynced write survived the crash: %q", v)
	}
}

func TestWipeDiskLosesEverything(t *testing.T) {
	s := New(testConfig(1), []int{0, 1},
		func(int, Deps) Handler { return twoPhaseWriter{} })
	s.Start()
	if err := s.RunUntil(20); err != nil {
		t.Fatal(err)
	}

	s.InjectFault(NewCrashFault(0, true, 0)) // WipeDisk
	if err := s.RunUntil(30); err != nil {
		t.Fatal(err)
	}

	if keys := s.Keys(0); len(keys) != 0 {
		t.Fatalf("disk not wiped, keys remain: %v", keys)
	}
}

type syncOrderNode struct{}

func (syncOrderNode) OnRestart(ctx *Ctx) {
	ctx.Put("first", []byte("a"))
	ctx.Sync()
	ctx.Put("second", []byte("b")) // textually after the Sync
}

func (syncOrderNode) OnMessage(*Ctx, int, Message) {}
func (syncOrderNode) OnTimer(*Ctx, string)         {}

// NOTE right now the put in the same callback after the sync will also applies
func TestSyncCoversWholeCallback(t *testing.T) {
	s := New(testConfig(1), []int{0, 1},
		func(int, Deps) Handler { return syncOrderNode{} })
	s.Start()

	s.InjectFault(NewCrashFault(0, false, 0))
	if err := s.RunUntil(10); err != nil {
		t.Fatal(err)
	}

	if _, ok := s.Get(0, "second"); !ok {
		t.Fatal("write after Sync in the same callback was rolled back; " +
			"drain order no longer applies Sync after every Put")
	}
}

func TestPauseResumesAutomatically(t *testing.T) {
	s := New(testConfig(1), []int{0, 1}, noop)
	s.Start()

	s.InjectFault(NewPauseFault(0, 100))
	if err := s.Step(1); err != nil { // apply the pause
		t.Fatal(err)
	}
	if got := s.Status(0); got != Paused {
		t.Fatalf("status after pause = %v, want Paused", got)
	}

	if err := s.RunUntil(99); err != nil {
		t.Fatal(err)
	}
	if got := s.Status(0); got != Paused {
		t.Fatalf("status at t=99 = %v, want Paused (resumed early)", got)
	}

	if err := s.RunUntil(101); err != nil {
		t.Fatal(err)
	}
	if got := s.Status(0); got != Healthy {
		t.Fatalf("status at t=101 = %v, want Healthy (pause never resumed)", got)
	}
}

func TestStaleResumeDoesNotCancelLaterPause(t *testing.T) {
	s := New(testConfig(1), []int{0, 1}, noop)
	s.Start()

	s.InjectFault(NewPauseFault(0, 100)) // token 1, resume scheduled for t=100
	if err := s.Step(1); err != nil {
		t.Fatal(err)
	}

	if err := s.RunUntil(50); err != nil {
		t.Fatal(err)
	}
	s.InjectFault(NewResumeFault(0)) // unconditional; token 1 now spent
	if err := s.Step(1); err != nil {
		t.Fatal(err)
	}
	if got := s.Status(0); got != Healthy {
		t.Fatalf("manual resume failed: status=%v", got)
	}

	s.InjectFault(NewPauseFault(0, 500)) // token 2, resume for t=550
	if err := s.Step(1); err != nil {
		t.Fatal(err)
	}

	// t=100: the token-1 resume fires. It must be rejected as superseded.
	if err := s.RunUntil(200); err != nil {
		t.Fatal(err)
	}
	if got := s.Status(0); got != Paused {
		t.Fatalf("status at t=200 = %v, want Paused (stale resume un-paused a "+
			"later pause)", got)
	}
}

type pingOnTimer struct {
	peer     int
	received int
}

func (n *pingOnTimer) OnRestart(ctx *Ctx) {
	ctx.SetTimer("t", 10)
}

func (n *pingOnTimer) OnTimer(ctx *Ctx, name string) {
	ctx.Send(n.peer, testMsg{N: 1})
	ctx.SetTimer(name, 10)
}

func (n *pingOnTimer) OnMessage(*Ctx, int, Message) {
	n.received++
}

func TestPausedNodeDefersThenReceivesBurst(t *testing.T) {
	made := map[int]*pingOnTimer{}
	s := New(testConfig(1), []int{0, 1},
		stable(made, func(id int, _ Deps) *pingOnTimer {
			return &pingOnTimer{peer: 1 - id}
		}))
	s.Start()

	if err := s.RunUntil(50); err != nil {
		t.Fatal(err)
	}
	before := made[0].received

	s.InjectFault(NewPauseFault(0, 100))
	if err := s.RunUntil(140); err != nil {
		t.Fatal(err)
	}
	if made[0].received != before {
		t.Fatalf("paused node received %d messages while frozen",
			made[0].received-before)
	}

	if err := s.RunUntil(160); err != nil {
		t.Fatal(err)
	}
	if made[0].received <= before {
		t.Fatal("deferred messages were never delivered after resume")
	}
}

func TestSecondCrashDoesNotStrandPendingRestart(t *testing.T) {
	s := New(testConfig(1), []int{0, 1}, noop)
	s.Start()

	s.InjectFault(NewCrashFault(0, false, 100)) // restart scheduled for t=100
	if err := s.Step(1); err != nil {
		t.Fatal(err)
	}
	if got := s.Status(0); got != Crashed {
		t.Fatalf("status = %v, want Crashed", got)
	}

	s.InjectFault(NewCrashFault(0, false, 0)) // crash again while down
	if err := s.Step(1); err != nil {
		t.Fatal(err)
	}

	if err := s.RunUntil(200); err != nil {
		t.Fatal(err)
	}
	if got := s.Status(0); got != Healthy {
		t.Fatalf("status at t=200 = %v, want Healthy (second crash stranded "+
			"the pending restart)", got)
	}
}

func TestRestartFaultRunsFullRestartPath(t *testing.T) {
	made := map[int]*bootRecorder{}
	s := New(testConfig(1), []int{0, 1},
		stable(made, func(int, Deps) *bootRecorder { return &bootRecorder{} }))
	s.Start()

	s.InjectFault(NewCrashFault(0, false, 0)) // stays down
	if err := s.RunUntil(10); err != nil {
		t.Fatal(err)
	}
	if got := s.Status(0); got != Crashed {
		t.Fatalf("status = %v, want Crashed", got)
	}

	s.InjectFault(NewRestartFault(0))
	if err := s.RunUntil(20); err != nil {
		t.Fatal(err)
	}

	if got := s.Status(0); got != Healthy {
		t.Fatalf("status = %v, want Healthy", got)
	}
	if made[0].boots != 2 {
		t.Fatalf("OnRestart called %d times, want 2", made[0].boots)
	}
}

func TestRestartOfHealthyNodeIsTracedNotFatal(t *testing.T) {
	s := New(testConfig(1), []int{0, 1}, noop)
	s.Start()

	s.InjectFault(NewRestartFault(0)) // node 0 is up
	if err := s.RunUntil(10); err != nil {
		t.Fatal(err)
	}
	if got := s.Status(0); got != Healthy {
		t.Fatalf("status = %v, want Healthy", got)
	}

	dropped := s.Trace().Filter(func(e Entry) bool { return e.Kind == EnDropped })
	if len(dropped) == 0 {
		t.Fatal("restart of a healthy node left no trace entry")
	}
}

type randRecorder struct {
	depsRand   Rand
	draws      []int64
	sameStream bool
}

func (n *randRecorder) OnRestart(ctx *Ctx) {
	n.sameStream = n.depsRand == ctx.Rand()
	n.draws = append(n.draws, ctx.Rand().Int64N(1<<40))
}

func (n *randRecorder) OnMessage(*Ctx, int, Message) {}
func (n *randRecorder) OnTimer(*Ctx, string)         {}

func TestRestartReseedsAndKeepsOneStream(t *testing.T) {
	made := map[int]*randRecorder{}
	s := New(testConfig(7), []int{0, 1},
		stable(made, func(_ int, d Deps) *randRecorder {
			return &randRecorder{depsRand: d.Rand}
		}))
	s.Start()

	for range 3 {
		s.InjectFault(NewCrashFault(0, false, 10))
		if err := s.RunUntil(s.Now() + 50); err != nil {
			t.Fatal(err)
		}
	}

	n := made[0]
	if !n.sameStream {
		t.Error("deps.Rand and ctx.Rand() are different streams after restart")
	}
	if len(n.draws) != 4 {
		t.Fatalf("got %d boots, want 4", len(n.draws))
	}
	seen := map[int64]bool{}
	for i, d := range n.draws {
		if seen[d] {
			t.Fatalf("draw %d repeated (%d): restart did not reseed", i, d)
		}
		seen[d] = true
	}
}

type selfRand struct {
	draw int64
}

func (n *selfRand) OnRestart(ctx *Ctx) {
	n.draw = ctx.Rand().Int64N(1 << 40)
}
func (n *selfRand) OnMessage(*Ctx, int, Message) {}
func (n *selfRand) OnTimer(*Ctx, string)         {}

// Dense ids hide id/index confusion, since there they're equal. {5,7,9} catches
// it across streams, links and rules.
func TestNonDenseNodeIDs(t *testing.T) {
	ids := []int{5, 7, 9}
	made := map[int]*selfRand{}
	s := New(testConfig(3), ids,
		stable(made, func(int, Deps) *selfRand { return &selfRand{} }))
	s.Start()

	seen := map[int64]int{}
	for _, id := range ids {
		d := made[id].draw
		if other, dup := seen[d]; dup {
			t.Fatalf("nodes %d and %d share an RNG stream", other, id)
		}
		seen[d] = id
	}

	s.Partition([]int{5}, []int{7, 9})
	if s.network.reachable(5, 7, s.Now()) {
		t.Error("partition did not block 5->7")
	}
	if !s.network.reachable(7, 9, s.Now()) {
		t.Error("partition wrongly blocked 7->9")
	}
	s.Heal(0)
	if !s.network.reachable(5, 7, s.Now()) {
		t.Error("heal did not restore 5->7")
	}
}

type selfSender struct{}

func (selfSender) OnRestart(ctx *Ctx) {
	ctx.Send(ctx.Self(), testMsg{N: 1})
}
func (selfSender) OnMessage(*Ctx, int, Message) {}
func (selfSender) OnTimer(*Ctx, string)         {}

func TestSelfSendPanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("self-send did not panic")
		}
	}()

	s := New(testConfig(1), []int{0, 1},
		func(int, Deps) Handler { return selfSender{} })
	s.Start()
}

func TestFaultScheduleIsDeterministic(t *testing.T) {
	run := func() (uint64, uint64) {
		made := map[int]*pingOnTimer{}
		cfg := testConfig(99)
		cfg.NetworkConfig = NetworkConfig{
			Delay:          DelaySpec{Kind: DelayUniform, Base: 5, Spread: 20},
			LossPPM:        50_000,
			DuplicationPPM: 10_000,
		}
		s := New(cfg, []int{0, 1},
			stable(made, func(id int, _ Deps) *pingOnTimer {
				return &pingOnTimer{peer: 1 - id}
			}))
		s.Start()

		s.ScheduleFault(100, NewCrashFault(0, false, 50))
		s.ScheduleFault(300, NewPauseFault(1, 100))
		s.ScheduleFault(500, NewPartitionFault([]int{0}, []int{1}))
		s.ScheduleFault(700, NewHealFault(0))
		s.ScheduleFault(900, NewCrashFault(1, true, 100))

		if err := s.RunUntil(2000); err != nil {
			t.Fatal(err)
		}
		return s.Hash(), s.EventCount()
	}

	h1, e1 := run()
	h2, e2 := run()

	if h1 != h2 {
		t.Fatalf("hash differs across runs: %016x vs %016x", h1, h2)
	}
	if e1 != e2 {
		t.Fatalf("event count differs across runs: %d vs %d", e1, e2)
	}
}

func TestDifferentSeedsDiverge(t *testing.T) {
	run := func(seed uint64) uint64 {
		made := map[int]*pingOnTimer{}
		cfg := testConfig(seed)
		cfg.NetworkConfig = NetworkConfig{
			Delay:   DelaySpec{Kind: DelayUniform, Base: 5, Spread: 20},
			LossPPM: 100_000,
		}
		s := New(cfg, []int{0, 1},
			stable(made, func(id int, _ Deps) *pingOnTimer {
				return &pingOnTimer{peer: 1 - id}
			}))
		s.Start()
		if err := s.RunUntil(1000); err != nil {
			t.Fatal(err)
		}
		return s.Hash()
	}

	if run(1) == run(2) {
		t.Fatal("different seeds produced the same hash")
	}
}
