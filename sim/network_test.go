package sim

import (
	"encoding/binary"
	"io"
	"math"
	"testing"
)

type seqMsg struct {
	N uint64
}

func (m seqMsg) HashInto(w io.Writer) {
	var buf [9]byte
	buf[0] = 's'
	binary.LittleEndian.PutUint64(buf[1:], m.N)
	w.Write(buf[:])
}

func (m seqMsg) Equal(other any) bool {
	o, ok := other.(seqMsg)
	return ok && m.N == o.N
}

type sender struct {
	NoOpHandler
	to       int
	sent     uint64
	count    uint64
	interval Duration
}

func (s *sender) OnRestart(ctx *Ctx) {
	ctx.SetTimer("tick", 0)
}

func (s *sender) OnTimer(ctx *Ctx, name string) {
	if s.sent >= s.count {
		return
	}
	ctx.Send(s.to, seqMsg{N: s.sent})
	s.sent++
	ctx.SetTimer("tick", s.interval)
}

type sink struct {
	NoOpHandler
}

type sendResult struct {
	s        *Sim
	sends    []Entry
	delivers []Entry
}

// setupSend wires a sender and a sink and schedules the bootstrap timer, but
// does not run. Tests that need to inject faults schedule them on the returned
// Sim before calling a run mode themselves.
func setupSend(
	t *testing.T,
	nc NetworkConfig,
	seed uint64,
	from int,
	to int,
	count uint64,
	interval Duration,
) *Sim {
	t.Helper()

	cfg := Config{
		Seed:          seed,
		MaxEvents:     10_000_000,
		TraceLevel:    TraceHashEvents,
		TraceKeep:     KeepAll,
		NetworkConfig: nc,
	}
	s := New(cfg, []int{from, to},
		func(int, Deps) Handler { return &sink{} },
		WithFactory(from, func(int, Deps) Handler {
			return &sender{to: to, count: count, interval: interval}
		}),
	)

	s.Start()
	return s
}

// collect pulls the sends and deliveries for one direction out of the trace.
func collect(s *Sim, from, to int) sendResult {
	return sendResult{
		s: s,
		sends: s.Trace().Filter(func(e Entry) bool {
			return e.Kind == EnSent && e.Event.Source == from && e.Event.Target == to
		}),
		delivers: s.Trace().Filter(func(e Entry) bool {
			return e.Kind == EnEvent && e.Event.Kind == EvDeliver &&
				e.Event.Source == from && e.Event.Target == to
		}),
	}
}

func runSend(t *testing.T, nc NetworkConfig, seed uint64, from, to int, count uint64, interval Duration) sendResult {
	t.Helper()
	s := setupSend(t, nc, seed, from, to, count, interval)
	if err := s.RunUntilQuiescent(); err != nil {
		t.Fatalf("run failed: %v", err)
	}
	return collect(s, from, to)
}

func uniformDelay(max Duration) DelaySpec {
	return DelaySpec{Kind: DelayUniform, Spread: max}
}

// TODO Read them more
// Got these from AI

// binomialTolerance returns how far, in events, an observed count may stray from
// the expected count n*p before the test should fail, for n independent trials of
// probability p. An observed count passes when
//
//	|observed - n*p| <= binomialTolerance(t, n, p, k)
//
// The tolerance is k standard deviations wide, where sigma = sqrt(n*p*(1-p)).
// Note that k is a plain multiplier, not a number of events: at n = 10000 and
// p = 0.2 sigma is 40 events, so k = 5 yields a tolerance of 200.
//
// Pick k from how often the assertion will run, not from n:
//
//	k = 3  fails on correct code about 1 run in 370        (local, hand-run only)
//	k = 4  fails on correct code about 1 run in 15,800     (small suite)
//	k = 5  fails on correct code about 1 run in 1,700,000  (safe for CI)
//
// Only sigma depends on n, so raising n tightens the tolerance in absolute terms
// while holding the false-failure rate fixed. That is the right way to buy
// sensitivity; lowering k buys much less of it and costs disproportionately in
// flakiness.
//
// The tolerance reliably detects deviations larger than roughly (k+2)*sigma and
// misses smaller ones. To catch a deviation of delta, expressed as a fraction of
// n, size the test with n >= p*(1-p)*((k+2)/delta)^2.
//
// binomialTolerance aborts the test when the normal approximation behind these
// numbers does not hold. That indicates a badly configured test, not a failing
// subject.
func binomialTolerance(t *testing.T, n int, p, k float64) float64 {
	t.Helper()

	// np and nq are the expected counts of each outcome: n*p occurrences and
	// n*(1-p) non-occurrences. Since a count is bounded below by 0 and above by n,
	// they also measure how far the mean sits from each wall. The normal curve has
	// no walls, so it only approximates the binomial while both are at least k
	// sigma away, otherwise the tolerance reaches past counts that can occur.
	//
	// np >= k^2*(1-p) is the condition "mean - k*sigma >= 0" rearranged.
	np, nq := float64(n)*p, float64(n)*(1-p)
	if minNP := k * k * (1 - p); np < minNP {
		t.Fatalf("binomialTolerance: n*p = %.1f, need >= %.1f at k = %g; raise n above %d",
			np, minNP, k, int(math.Ceil(minNP/p)))
	}
	if minNQ := k * k * p; nq < minNQ {
		t.Fatalf("binomialTolerance: n*(1-p) = %.1f, need >= %.1f at k = %g; raise n above %d",
			nq, minNQ, k, int(math.Ceil(minNQ/(1-p))))
	}

	return k * math.Sqrt(float64(n)*p*(1-p))
}

// uniformMeanTolerance returns the half-width of a symmetric band around the
// expected mean of n samples drawn uniformly from [0, maxDelay).
//
// A single draw from the discrete uniform distribution over k consecutive
// integers has variance (k^2 - 1) / 12. Averaging n independent draws divides
// that variance by n, so the standard error of the sample mean is
// sqrt(variance / n). Multiplying by `sigmas` gives a band the observed mean
// stays inside with a probability set by the normal approximation:
// 2 sigma is ~95%, 3 sigma ~99.7%, 5 sigma about 1 in 3.5 million.
//
// Five sigmas is the usual choice here: wide enough never to flake across
// seeds, tight enough that a genuinely skewed sampler still fails.
func uniformMeanTolerance(t *testing.T, n int, maxDelay int64, sigmas float64) float64 {
	t.Helper()
	if n <= 0 {
		t.Fatalf("uniformMeanTolerance: need at least one sample, got %d", n)
	}
	variance := float64(maxDelay*maxDelay-1) / 12
	return sigmas * math.Sqrt(variance/float64(n))
}

func TestNetworkDropRateMatchesConfig(t *testing.T) {
	const (
		from, to        = 1, 2
		maxDelay        = 10
		lossPPM         = 200_000 // 20%
		total           = 10_000
		interval        = 100
		toleranceSigmas = 5.0
	)

	r := runSend(t, NetworkConfig{Delay: uniformDelay(maxDelay), LossPPM: lossPPM}, 10, from, to, total, interval)

	if len(r.sends) != total {
		t.Fatalf("sent %d messages, want %d", len(r.sends), total)
	}

	sends := len(r.sends)
	dropped := sends - len(r.delivers)
	p := float64(lossPPM) / 1_000_000
	want := float64(sends) * p
	tol := binomialTolerance(t, sends, p, toleranceSigmas)

	if math.Abs(float64(dropped)-want) > tol {
		t.Errorf("dropped %d (%.2f%%), want %.0f +/- %.0f events (%g sigmas, n=%d, p=%.2f)",
			dropped, float64(dropped)/float64(sends)*100, want, tol, toleranceSigmas, sends, p)
	}
	t.Logf("sent=%d delivered=%d dropped=%d tolerance=+/-%.0f", sends, len(r.delivers), dropped, tol)
}

func TestNetworkDuplicationRateMatchesConfig(t *testing.T) {
	const (
		from, to        = 1, 2
		maxDelay        = 10
		dupPPM          = 200_000 // 20%
		total           = 10_000
		interval        = 100
		toleranceSigmas = 5.0
	)

	r := runSend(t, NetworkConfig{Delay: uniformDelay(maxDelay), DuplicationPPM: dupPPM}, 10, from, to, total, interval)

	if len(r.sends) != total {
		t.Fatalf("sent %d messages, want %d", len(r.sends), total)
	}

	// No loss, so every extra delivery is a duplicate.
	duplicated := len(r.delivers) - total
	p := float64(dupPPM) / 1_000_000
	want := float64(total) * p
	tol := binomialTolerance(t, total, p, toleranceSigmas)

	if math.Abs(float64(duplicated)-want) > tol {
		t.Fatalf("duplicated %d (%.2f%%), want %.0f +/- %.0f events",
			duplicated, float64(duplicated)/float64(total)*100, want, tol)
	}
	t.Logf("sent=%d delivered=%d duplicated=%d tolerance=+/-%.0f", total, len(r.delivers), duplicated, tol)
}

func TestNetworkDelayWithinBounds(t *testing.T) {
	const (
		from, to        = 1, 2
		maxDelay        = 30
		total           = 10_000
		interval        = 100 // >> maxDelay, so the FIFO cursor never bites
		toleranceSigmas = 5.0
	)

	r := runSend(t, NetworkConfig{Delay: uniformDelay(maxDelay)}, 10, from, to, total, interval)

	if len(r.delivers) != len(r.sends) {
		t.Fatalf("sends %d, delivers %d", len(r.sends), len(r.delivers))
	}

	// Match on payload, not Seq: an EnSent entry has no scheduled Seq of its
	// own, and Parent points at the timer that caused the send, not the send.
	sentAt := make(map[uint64]Time, total)
	for _, e := range r.sends {
		sentAt[e.Event.Payload.(seqMsg).N] = e.At
	}

	var sum Time
	minD, maxD := Time(math.MaxInt64), Time(math.MinInt64)
	for _, d := range r.delivers {
		n := d.Event.Payload.(seqMsg).N
		at, ok := sentAt[n]
		if !ok {
			t.Fatalf("delivery of seq %d has no matching send", n)
		}
		delay := d.At - at
		if delay < 0 || delay >= maxDelay {
			t.Fatalf("delay %d out of range [0,%d)", delay, maxDelay)
		}
		sum += delay
		minD, maxD = min(minD, delay), max(maxD, delay)
	}

	mean := float64(sum) / float64(len(r.delivers))
	want := float64(maxDelay-1) / 2
	tol := uniformMeanTolerance(t, len(r.delivers), maxDelay, toleranceSigmas)

	if math.Abs(mean-want) > tol {
		t.Errorf("mean delay %.2f, want %.2f +/- %.2f", mean, want, tol)
	}
	t.Logf("n=%d min=%d max=%d mean=%.2f", len(r.delivers), minD, maxD, mean)
}

func TestAsymmetricPartitionBlocksOneDirection(t *testing.T) {
	const (
		a, b     = 1, 2
		maxDelay = 30
		total    = 2_000
		interval = 100
	)
	nc := NetworkConfig{Delay: uniformDelay(maxDelay)}

	// The 2->1 link is blocked in both runs; the runs differ only in which
	// way the traffic flows.
	fwdSim := setupSend(t, nc, 10, a, b, total, interval)
	fwdSim.InjectFault(blockLinkFault{From: b, To: a})
	if err := fwdSim.RunUntilQuiescent(); err != nil {
		t.Fatalf("forward run failed: %v", err)
	}
	fwd := collect(fwdSim, a, b)

	if len(fwd.delivers) != total {
		t.Errorf("1->2 delivered %d, want %d (direction is open)", len(fwd.delivers), total)
	}

	revSim := setupSend(t, nc, 10, b, a, total, interval)
	revSim.InjectFault(blockLinkFault{From: b, To: a})
	if err := revSim.RunUntilQuiescent(); err != nil {
		t.Fatalf("reverse run failed: %v", err)
	}
	rev := collect(revSim, b, a)

	if len(rev.sends) != total {
		t.Fatalf("2->1 recorded %d sends, want %d", len(rev.sends), total)
	}
	if len(rev.delivers) != 0 {
		t.Errorf("2->1 delivered %d, want 0 (direction is blocked)", len(rev.delivers))
	}
	t.Logf("1->2: %d sends %d delivers | 2->1: %d sends %d delivers",
		len(fwd.sends), len(fwd.delivers), len(rev.sends), len(rev.delivers))
}

func TestFIFOModePreservesOrder(t *testing.T) {
	const total, maxDelay = 2_000, 200

	// interval << maxDelay, so messages overlap on the wire and the cursor
	// actually has work to do.
	r := runSend(t, NetworkConfig{Delay: uniformDelay(maxDelay)}, 7, 1, 2, total, 10)

	last := -1
	for i, d := range r.delivers {
		n := int(d.Event.Payload.(seqMsg).N)
		if n <= last {
			t.Fatalf("out of order at position %d: seq %d after %d", i, n, last)
		}
		last = n
	}
	t.Logf("%d deliveries, strictly increasing", len(r.delivers))
}

func TestReorderingModeActuallyReorders(t *testing.T) {
	const total, maxDelay = 2_000, 200

	r := runSend(t, NetworkConfig{Delay: uniformDelay(maxDelay), IsReordering: true}, 7, 1, 2, total, 10)

	inversions, last := 0, -1
	for _, d := range r.delivers {
		n := int(d.Event.Payload.(seqMsg).N)
		if n <= last {
			inversions++
		}
		last = n
	}
	if inversions == 0 {
		t.Fatal("no reordering observed; the FIFO test proves nothing under these parameters")
	}
	t.Logf("%d inversions in %d deliveries", inversions, len(r.delivers))
}

func TestUniformSpreadIsAWidth(t *testing.T) {
	u := Uniform{Base: 20, Spread: 100}
	r := NewStream(1, 1)

	minSeen, maxSeen := Duration(1<<62), Duration(0)
	for range 10_000 {
		d := u.Sample(r)
		minSeen = min(minSeen, d)
		maxSeen = max(maxSeen, d)
	}

	if minSeen < 20 {
		t.Errorf("sample %d below Base", minSeen)
	}
	if maxSeen >= 120 {
		t.Errorf("sample %d at or above Base+Spread", maxSeen)
	}
	if maxSeen <= 100 {
		t.Errorf("max sample %d never exceeded 100; Spread treated as a ceiling",
			maxSeen)
	}
}

func TestZeroSpreadIsConstant(t *testing.T) {
	u := Uniform{Base: 20, Spread: 0}
	r := NewStream(1, 1)
	for range 100 {
		if d := u.Sample(r); d != 20 {
			t.Fatalf("sample = %d, want 20", d)
		}
	}
}

// Last match wins, so a later allow rule punches a hole through an earlier
// block. That is what makes a bridge node expressible, one that reaches both
// sides of a partition while the sides cannot reach each other.
//
// This is the only test that would catch a refactor flipping to first-match,
// which would silently make bridge topologies unreachable rather than failing
// anything.
func TestBridgeNodeCrossesPartition(t *testing.T) {
	s := New(testConfig(1), []int{0, 1, 2, 3}, noop)
	s.Start()

	s.Partition([]int{0, 1}, []int{2, 3})

	if s.network.reachable(0, 2, s.Now()) {
		t.Fatal("setup: partition did not block 0->2")
	}

	// Allow rule added after the block: 1 <-> 2 becomes the bridge.
	s.network.addRule(func(from, to int, _ Time) bool {
		return (from == 1 && to == 2) || (from == 2 && to == 1)
	}, false)

	if !s.network.reachable(1, 2, s.Now()) {
		t.Error("bridge 1->2 blocked; first match is winning")
	}
	if !s.network.reachable(2, 1, s.Now()) {
		t.Error("bridge 2->1 blocked")
	}
	if s.network.reachable(0, 2, s.Now()) {
		t.Error("allow rule leaked to 0->2; the predicate is too broad")
	}
}

func TestOverlappingPartitionsHealIndependently(t *testing.T) {
	s := New(testConfig(1), []int{0, 1, 2, 3}, noop)
	s.Start()

	split := s.Partition([]int{0, 1}, []int{2, 3})
	s.network.isolate(3)

	if s.network.reachable(0, 2, s.Now()) {
		t.Fatal("setup: partition not applied")
	}
	if s.network.reachable(2, 3, s.Now()) {
		t.Fatal("setup: isolate not applied")
	}

	s.Heal(split)

	if !s.network.reachable(0, 2, s.Now()) {
		t.Error("healing the partition did not restore 0->2")
	}
	if s.network.reachable(2, 3, s.Now()) {
		t.Error("healing the partition also removed the isolate rule")
	}
}

// A scheduled heal can fire after a reset already cleared its rule. That is
// normal, not an error.
func TestHealUnknownRuleIsNoOp(t *testing.T) {
	s := New(testConfig(1), []int{0, 1}, noop)
	s.Start()

	s.Heal(RuleID(9999)) // never existed

	if !s.network.reachable(0, 1, s.Now()) {
		t.Fatal("healing an unknown rule broke reachability")
	}
}

func TestIsolateBlocksBothDirections(t *testing.T) {
	s := New(testConfig(1), []int{0, 1, 2}, noop)
	s.Start()

	s.network.isolate(1)

	if s.network.reachable(1, 0, s.Now()) {
		t.Error("isolated node can still send")
	}
	if s.network.reachable(0, 1, s.Now()) {
		t.Error("isolated node can still receive")
	}
	if !s.network.reachable(0, 2, s.Now()) {
		t.Error("isolate leaked to an unrelated pair")
	}
}

// resetConnection models a TCP reset: ordering guarantees restart from scratch,
// so the FIFO cursor must go back to zero.
func TestResetConnectionClearsFIFOCursor(t *testing.T) {
	s := New(testConfig(1), []int{0, 1}, noop)
	s.Start()

	l := s.network.link(0, 1)
	l.lastArrival = 5000

	s.network.resetConnection(0, 1)

	if l.lastArrival != 0 {
		t.Fatalf("lastArrival = %d after reset, want 0", l.lastArrival)
	}
}
