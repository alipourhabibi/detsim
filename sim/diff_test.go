package sim

import "testing"

func TestFirstDivergenceUsesChronologicalOrder(t *testing.T) {
	a := NewTrace(TraceHashEvents, 4)
	b := NewTrace(TraceHashEvents, 4)
	for i := range 10 {
		a.Record(Event{At: Time(i), Seq: uint64(i)})
		seq := uint64(i)
		if i == 8 {
			seq = 999 // diverge in the newest half, after the ring wrapped
		}
		b.Record(Event{At: Time(i), Seq: seq})
	}

	d := FirstDivergence(a, b)
	if d == nil {
		t.Fatal("FirstDivergence found nothing; traces differ at seq 8")
	}
	if d.Index != 2 { // entries are seqs 6,7,8,9 -> index 2
		t.Fatalf("divergence at index %d, want 2 (raw ring order used?)", d.Index)
	}
}

// notes keep their content in Reason, so two writes of different keys at the
// same time must not count as equal
func TestFirstDivergenceComparesNotes(t *testing.T) {
	a, b := NewTrace(TraceHashEvents, KeepAll), NewTrace(TraceHashEvents, KeepAll)
	a.Note(EnWrite, 5, 0, "a", 1)
	b.Note(EnWrite, 5, 0, "b", 1)
	if FirstDivergence(a, b) == nil {
		t.Fatal("write a and write b look the same")
	}
}

func TestFirstDivergenceComparesTimerNames(t *testing.T) {
	a, b := NewTrace(TraceHashEvents, KeepAll), NewTrace(TraceHashEvents, KeepAll)
	a.Record(Event{At: 10, Seq: 1, Kind: EvTimer, Target: 0, Source: -1, Name: "election"})
	b.Record(Event{At: 10, Seq: 1, Kind: EvTimer, Target: 0, Source: -1, Name: "heartbeat"})
	if FirstDivergence(a, b) == nil {
		t.Fatal("timer election and timer heartbeat look the same")
	}
}
