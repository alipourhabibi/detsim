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

func hashInts(w io.Writer, tag byte, xs ...int) {
	var buf [8]byte
	w.Write([]byte{tag})
	for _, x := range xs {
		binary.LittleEndian.PutUint64(buf[:], uint64(int64(x)))
		w.Write(buf[:])
	}
}
