package sim

import "testing"

func TestReadingTheHistoryDoesNotCloseIt(t *testing.T) {
	h := newHistory()
	h.Invoke(10, 1, "write", 7)

	if got := h.Ops()[0].Outcome; got != Unknown {
		t.Fatalf("an operation still waiting reads %v, want unknown", got)
	}

	h.Complete(20, 1, 7)

	ops := h.Ops()
	if len(ops) != 1 {
		t.Fatalf("expected 1 op, got %d", len(ops))
	}
	if ops[0].Outcome != Ok {
		t.Fatalf("the answer arrived at t=20 but the history says %v", ops[0].Outcome)
	}
	if ops[0].Returned != 20 {
		t.Fatalf("returned at %d, want 20", ops[0].Returned)
	}
}

// Counts reports the real state, so a harness can still see what is in flight.
func TestCountsReportsOpenOperations(t *testing.T) {
	h := newHistory()
	h.Invoke(10, 1, "write", 7)

	if ok, unknown, open := h.Counts(); ok != 0 || unknown != 0 || open != 1 {
		t.Fatalf("counts = ok %d, unknown %d, open %d; want 0, 0, 1", ok, unknown, open)
	}
}

func TestRunModesDoNotEndTheHistory(t *testing.T) {
	cases := []struct {
		name string
		run  func(*Sim) error
	}{
		{"RunUntil", func(s *Sim) error { return s.RunUntil(50) }},
		{"RunUntilQuiescent", func(s *Sim) error { return s.RunUntilQuiescent() }},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := New(testConfig(1), []int{0, 1}, noop)
			s.Start()

			s.History().Invoke(s.Now(), 0, "write", 1)

			if err := tc.run(s); err != nil {
				t.Fatal(err)
			}

			// Phase two: the reply the client was waiting for.
			s.History().Complete(s.Now(), 0, 1)

			ops := s.History().Ops()
			if len(ops) != 1 {
				t.Fatalf("expected 1 op, got %d", len(ops))
			}
			if ops[0].Outcome != Ok {
				t.Fatalf("op was answered after the phase boundary but reads %v; "+
					"the run mode closed the history", ops[0].Outcome)
			}
		})
	}
}
