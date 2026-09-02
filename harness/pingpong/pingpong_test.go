package pingpong

import (
	"bytes"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/alipourhabibi/detsim/sim"
)

// same seed twice, same hash
func TestDeterministicSameProcess(t *testing.T) {
	run := func(seed uint64) (uint64, uint64, uint64) {
		config := sim.Config{
			Seed:          seed,
			MaxEvents:     1_000_000,
			TraceLevel:    sim.TraceHashEventsAndState,
			NetworkConfig: sim.NetworkConfig{Delay: sim.DelaySpec{Kind: sim.DelayUniform, Spread: 20}},
		}
		pa, pb, s := NewPingPong(config, 0, 1, 200)
		if err := s.RunUntilQuiescent(); err != nil {
			t.Fatalf("run failed: %v", err)
		}
		return s.Trace().Sum(), pa.node.Count, pb.node.Count
	}

	for _, seed := range []uint64{1, 42, 999} {
		h1, a1, b1 := run(seed)
		h2, a2, b2 := run(seed)

		if h1 != h2 {
			t.Errorf("seed %d: hash mismatch %x != %x", seed, h1, h2)
		}
		if a1 != a2 || b1 != b2 {
			t.Errorf("seed %d: state mismatch (%d,%d) != (%d,%d)", seed, a1, b1, a2, b2)
		}
	}
}

// exec simcheck twice, diff
func TestDeterministicFreshProcess(t *testing.T) {
	bin := filepath.Join(t.TempDir(), "simcheck")
	build := exec.Command("go", "build", "-o", bin, "./cmd/simcheck")
	build.Dir = "../.."
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build failed: %v\n%s", err, out)
	}

	run := func(seed uint64) string {
		cmd := exec.Command(bin, "-seed", strconv.FormatUint(seed, 10))
		var stdout, stderr bytes.Buffer
		cmd.Stdout = &stdout
		cmd.Stderr = &stderr
		if err := cmd.Run(); err != nil {
			t.Fatalf("seed %d: %v\nstderr: %s", seed, err, stderr.String())
		}
		return strings.TrimSpace(stdout.String())
	}

	for _, seed := range []uint64{1, 42, 999} {
		first := run(seed)
		second := run(seed)
		if first != second {
			t.Errorf("seed %d: fresh-process mismatch\n  run 1: %s\n  run 2: %s",
				seed, first, second)
		}
	}
}

// 100 seeds, not all equal
func TestSeedsDiverge(t *testing.T) {
	run := func(seed uint64) (uint64, uint64) {
		config := sim.Config{
			Seed:          seed,
			MaxEvents:     1_000_000,
			TraceLevel:    sim.TraceHashEventsAndState,
			NetworkConfig: sim.NetworkConfig{Delay: sim.DelaySpec{Kind: sim.DelayUniform, Spread: 20}},
		}
		_, _, s := NewPingPong(config, 0, 1, 200)
		if err := s.RunUntilQuiescent(); err != nil {
			t.Fatalf("run failed: %v", err)
		}
		return s.Trace().Sum(), s.EventCount()
	}

	hashes := map[uint64]struct{}{}
	var wantCount uint64

	for seed := range 100 {
		h1, count := run(uint64(seed))
		hashes[h1] = struct{}{}

		if seed == 0 {
			wantCount = count
			continue
		}

		if count != wantCount {
			t.Errorf("seed %d: event count %d, want %d (jitter should change timing, not control flow)",
				seed, count, wantCount)
		}
	}
	if len(hashes) < 2 {
		t.Fatalf("all 100 seeds produced the same hash")
	}
}

// ~1h simulated, <1s wall
func TestVirtualTimeIsFree(t *testing.T) {
	const simHour = sim.Time(60 * 60 * 1000)
	const rounds = 60_000 // ~60ms per round ≈ one simulated hour

	config := sim.Config{
		Seed:          100,
		MaxEvents:     10_000_000,
		TraceLevel:    sim.TraceHashEventsAndState,
		NetworkConfig: sim.NetworkConfig{Delay: sim.DelaySpec{Kind: sim.DelayUniform, Spread: 20}},
	}
	_, _, s := NewPingPong(config, 0, 1, rounds)

	start := time.Now()
	if err := s.RunUntil(simHour); err != nil {
		t.Fatalf("run failed: %v", err)
	}
	elapsed := time.Since(start)

	if s.Now() != simHour {
		t.Errorf("clock at %d, want %d", s.Now(), simHour)
	}
	if elapsed > time.Second {
		t.Fatalf("simulated hour took %v wall time", elapsed)
	}
	t.Logf("%d events over %d ms simulated in %v", s.EventCount(), simHour, elapsed)
}
