package sim

import (
	"encoding/binary"
	"io"
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

func (h *hasher) u32(v uint32) {
	binary.LittleEndian.PutUint32(h.buf[:], v)
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
