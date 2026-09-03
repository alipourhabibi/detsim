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
