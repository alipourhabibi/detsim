package sim

import (
	"bytes"
	"testing"
)

func TestFaultHashAgreesWithEqual(t *testing.T) {
	faults := []Fault{
		crashFault{Node: 1},
		crashFault{Node: 1, WipeDisk: true},
		crashFault{Node: 1, Downtime: 500},
		crashFault{Node: 2},
		pauseFault{Node: 1, Duration: 10},
		pauseFault{Node: 1, Duration: 20},
		restartFault{Node: 1},
		restartFault{Node: 2},
		resumeFault{Node: 1},
		resumeFault{Node: 1, Token: 2},
		healFault{ID: 0},
		healFault{ID: 3},
		isolateFault{Node: 1},
		partitionFault{A: []int{1, 2}, B: []int{3}},
		partitionFault{A: []int{1}, B: []int{2, 3}},
		lossLinkFault{From: 0, To: 1, DropPPM: 100},
		duplicateLinkFault{From: 0, To: 1, DuplicatePPM: 100},
		resetConnectionFault{From: 0, To: 1},
	}

	for i, a := range faults {
		for j, b := range faults {
			sameHash := hashOf(a) == hashOf(b)
			if sameHash != a.Equal(b) {
				t.Errorf("faults[%d]=%v vs faults[%d]=%v: hashEqual=%v Equal=%v",
					i, a, j, b, sameHash, a.Equal(b))
			}
		}
	}
}

func TestHasherFieldWidths(t *testing.T) {
	cases := []struct {
		name  string
		write func(*hasher)
		want  int
	}{
		{"u64", func(h *hasher) { h.u64(1) }, 8},
		{"i64", func(h *hasher) { h.i64(-1) }, 8},
		{"node", func(h *hasher) { h.node(3) }, 8},
		{"dur", func(h *hasher) { h.dur(5) }, 8},
		{"rule", func(h *hasher) { h.rule(2) }, 8},
		{"bool", func(h *hasher) { h.bool(true) }, 1},
		{"nodes", func(h *hasher) { h.nodes([]int{1, 2}) }, 8 + 8 + 8}, // length prefix
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var b bytes.Buffer
			h := hashTag(&b, 'X')
			tc.write(&h)
			if got := b.Len() - 1; got != tc.want { // minus the tag
				t.Fatalf("%s wrote %d bytes, want %d", tc.name, got, tc.want)
			}
		})
	}
}
