package sim

import "testing"

func TestStreamsAreIndependent(t *testing.T) {
	delays := func(withExtraFault bool) []Time {
		cfg := testConfig(5)
		cfg.NetworkConfig = NetworkConfig{
			Delay: DelaySpec{Kind: DelayUniform, Base: 10, Spread: 50},
		}
		s := New(cfg, []int{0, 1},
			func(id int, _ Deps) Handler {
				if id == 0 {
					return &pingOnTimer{peer: 1}
				}
				return NoOpHandler{}
			})
		s.Start()

		if withExtraFault {
			// Consumes the fault stream. Must not move the delay stream.
			s.ScheduleFault(1, NewPauseFault(1, 1))
		}
		if err := s.RunUntil(200); err != nil {
			t.Fatal(err)
		}

		var out []Time
		for _, e := range s.Trace().Entries() {
			if e.Kind == EnEvent && e.Event.Kind == EvDeliver {
				out = append(out, e.Event.At)
			}
		}
		return out
	}

	a, b := delays(false), delays(true)
	n := min(len(a), len(b))
	if n == 0 {
		t.Fatal("no deliveries recorded")
	}
	// The pause changes which deliveries happen, so compare only the arrival
	// times drawn before it takes effect.
	for i := range min(n, 3) {
		if a[i] != b[i] {
			t.Fatalf("delivery %d at t=%d without the fault, t=%d with it; "+
				"the fault stream is shifting the delay stream", i, a[i], b[i])
		}
	}
}

func TestPartitionDoesNotShiftLossStream(t *testing.T) {
	drawsAfter := func(partition bool) int64 {
		cfg := testConfig(11)
		cfg.NetworkConfig = NetworkConfig{LossPPM: 300_000}
		s := New(cfg, []int{0, 1},
			func(id int, _ Deps) Handler {
				if id == 0 {
					return &pingOnTimer{peer: 1}
				}
				return NoOpHandler{}
			})
		s.Start()
		if partition {
			s.Partition([]int{0}, []int{1})
		}
		if err := s.RunUntil(200); err != nil {
			t.Fatal(err)
		}
		return s.streams.loss.Int64N(1 << 40) // stream position after the run
	}

	if drawsAfter(false) != drawsAfter(true) {
		t.Fatal("partitioning changed the loss stream position; loss is being " +
			"sampled conditionally")
	}
}

// State folding walks nodes in sorted order, not map order. Map order would
// differ between runs and the hash would be useless.
func TestStateFoldOrderIsStable(t *testing.T) {
	run := func() uint64 {
		cfg := testConfig(13)
		cfg.TraceLevel = TraceHashEventsAndState
		s := New(cfg, []int{9, 3, 7, 1},
			func(id int, _ Deps) Handler {
				peer := 1
				if id == 1 {
					peer = 3
				}
				return &pingOnTimer{peer: peer}
			})
		s.Start()
		if err := s.RunUntil(200); err != nil {
			t.Fatal(err)
		}
		return s.Hash()
	}

	runA := run()
	runB := run()

	if runA != runB {
		t.Fatal("state hash differs across identical runs; node iteration is " +
			"not deterministic")
	}
}
