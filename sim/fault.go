package sim

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"strconv"
	"strings"
)

// Fault is a simulated event: such as a partition, a crash,
// a dropped link. They are scheduled like other events, they carry a
// timestamp, appear in the trace, and contribute to the run hash.
type Fault interface {
	Hashable
	Apply(s *Sim)
	String() string
}

type hasher struct {
	w   io.Writer
	buf [8]byte
}

func hashTag(w io.Writer, tag byte) hasher {
	w.Write([]byte{tag})
	return hasher{w: w}
}

func (h *hasher) u64(v uint64) {
	binary.LittleEndian.PutUint64(h.buf[:], v)
	h.w.Write(h.buf[:])
}

func (h *hasher) i64(v int64) {
	h.u64(uint64(v))
}

func (h *hasher) node(id int) {
	h.i64(int64(id))
}

func (h *hasher) dur(d Duration) {
	h.i64(int64(d))
}

func (h *hasher) rule(r RuleID) {
	h.u64(uint64(r))
}

func (h *hasher) bool(v bool) {
	var b byte
	if v {
		b = 1
	}
	h.w.Write([]byte{b})
}

func (h *hasher) nodes(ids []int) {
	h.u64(uint64(len(ids))) // length-prefixed: {1,2}|{3} must differ from {1}|{2,3}
	for _, id := range ids {
		h.node(id)
	}
}

// ParseFault reads back what Fault.String wrote. The corpus uses it to save a
// plan as text, so the plan does not depend on the code that drew it.
func ParseFault(s string) (Fault, error) {
	var (
		a, b int
		n    int64
		wipe bool
		pct  float64
		f    Fault
	)
	switch {
	case scan(s, "node %d crashed; wipe disk: %t; downtime: %d", &a, &wipe, &n):
		f = NewCrashFault(a, wipe, Duration(n))
	case scan(s, "node %d paused; duration: %d", &a, &n):
		f = NewPauseFault(a, Duration(n))
	case scan(s, "node %d restarted", &a):
		f = NewRestartFault(a)
	case scan(s, "node %d resumed with token %d", &a, &n):
		f = resumeFault{Node: a, Token: uint64(n)}
	case strings.HasPrefix(s, "partition "):
		f = parsePartition(strings.TrimPrefix(s, "partition "))
	case scan(s, "isolate %d", &a):
		f = NewIsolateFault(a)
	case s == "heal all":
		f = NewHealFault(0)
	case scan(s, "heal rule=%d", &n):
		f = NewHealFault(RuleID(n))
	case scan(s, "drop %d->%d %f%%", &a, &b, &pct):
		f = NewDropLinkFault(a, b, uint32(math.Round(pct*10000)))
	case scan(s, "duplicate %d->%d %f%%", &a, &b, &pct):
		f = NewDuplicateLinkFault(a, b, uint32(math.Round(pct*10000)))
	case scan(s, "reset connection %d->%d", &a, &b):
		f = NewResetConnectionFault(a, b)
	case scan(s, "block(%d->%d)", &a, &b):
		f = blockLinkFault{From: a, To: b}
	case s == "quiesce":
		f = quiesceFault{}
	}

	// Write it back out. Anything that does not come back the same was not
	// read right.
	if f == nil || f.String() != s {
		return nil, fmt.Errorf("sim: cannot read fault %q", s)
	}
	return f, nil
}

func scan(s, format string, args ...any) bool {
	_, err := fmt.Sscanf(s, format, args...)
	return err == nil
}

// parsePartition reads "[0 1]|[2 3]". nil if it is not that.
func parsePartition(s string) Fault {
	left, right, ok := strings.Cut(s, "|")
	if !ok {
		return nil
	}
	a, okA := parseNodes(left)
	b, okB := parseNodes(right)
	if !okA || !okB {
		return nil
	}
	return NewPartitionFault(a, b)
}

func parseNodes(s string) ([]int, bool) {
	s, ok := strings.CutPrefix(s, "[")
	if !ok {
		return nil, false
	}
	s, ok = strings.CutSuffix(s, "]")
	if !ok {
		return nil, false
	}
	var out []int
	for _, field := range strings.Fields(s) {
		id, err := strconv.Atoi(field)
		if err != nil {
			return nil, false
		}
		out = append(out, id)
	}
	return out, true
}
