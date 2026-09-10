package sim

import (
	"fmt"
	"math/rand/v2"
)

type nodeIdx int

// The harness should not reach float rand
type Rand interface {
	Int64N(n int64) int64
}

var _ Rand = (*rand.Rand)(nil)

// Stream ids. APPEND ONLY, never renumber, never reuse. Every id is a
// separate PCG sequence, so adding a stream leaves every existing seed
// producing exactly the run it produced before.
const (
	StreamDelay uint64 = iota + 1
	StreamLoss
	StreamDup
	StreamFault
	StreamWorkload
	// next concern stream goes here
)

// nodeStream is a stable handle. reseedNode swaps the inner *rand.Rand, so a
// Rand handed out before a restart keeps working and starts drawing from the
// new sequence.
type nodeStream struct {
	r *rand.Rand
}

func (n *nodeStream) Int64N(x int64) int64 {
	return n.r.Int64N(x)
}

type streams struct {
	seed uint64

	delay *rand.Rand
	loss  *rand.Rand
	dup   *rand.Rand
	fault *rand.Rand

	// node is indexed by dense node index, not node id.
	node []*nodeStream
}

func newStreams(seed uint64, n int) *streams {
	s := &streams{
		seed:  seed,
		delay: NewStream(seed, StreamDelay),
		loss:  NewStream(seed, StreamLoss),
		dup:   NewStream(seed, StreamDup),
		fault: NewStream(seed, StreamFault),
		node:  make([]*nodeStream, n),
	}
	for i := range s.node {
		s.node[i] = &nodeStream{r: NewStream(seed, nodeStreamID(i, 0))}
	}
	return s
}

func (s *streams) at(i nodeIdx) Rand {
	return s.node[i]
}

// reseedNode gives a restarted node a fresh sequence. Without it a node that
// crash-loops replays the same election timeouts every time, and whole
// interleavings become unreachable.
func (s *streams) reseedNode(i nodeIdx, epoch uint64) {
	s.node[i].r = NewStream(s.seed, nodeStreamID(int(i), epoch)) // swap inner, keep pointer
}

func NewStream(seed, id uint64) *rand.Rand {
	return rand.New(rand.NewPCG(seed, id))
}

const (
	// Node streams start well above the concern ids so the two ranges can never
	// collide, however many concerns get added.
	streamNodeBase uint64 = 1 << 32
	nodeIndexBits         = 24
	maxNodeIndex          = 1<<nodeIndexBits - 1
)

// nodeStreamID mixes index and epoch into one id. The index sits in the low
// bits and the epoch above it, so no two (index, epoch) pairs collide until a
// node has restarted 2^24 times.
func nodeStreamID(i int, epoch uint64) uint64 {
	if i < 0 || i > maxNodeIndex {
		panic(fmt.Sprintf("sim: node index %d out of range", i))
	}
	return streamNodeBase + uint64(i) + epoch<<nodeIndexBits
}
