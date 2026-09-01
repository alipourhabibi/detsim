package sim

import (
	"fmt"
	"math/rand/v2"
)

// The harness should not reach float rand
type Rand interface {
	Int64N(n int64) int64
}

var _ Rand = (*rand.Rand)(nil)

// Stream ids. APPEND ONLY, never renumber, never reuse. Every id is a
// separate PCG sequence, so adding a stream leaves every existing seed
// producing exactly the run it produced before.
const (
	streamDelay uint64 = iota + 1
	streamLoss
	streamDup
	streamFault
	streamBuggify
	streamWorkload
	// next concern stream goes here
)

type streams struct {
	seed uint64

	delay *rand.Rand
	loss  *rand.Rand
	dup   *rand.Rand
	fault *rand.Rand

	// node is indexed by dense node index, not node id.
	node []*rand.Rand
}

func newStreams(seed uint64, n int) *streams {
	s := &streams{
		seed:  seed,
		delay: newStream(seed, streamDelay),
		loss:  newStream(seed, streamLoss),
		dup:   newStream(seed, streamDup),
		fault: newStream(seed, streamFault),
		node:  make([]*rand.Rand, n),
	}
	for i := range s.node {
		s.node[i] = newStream(seed, nodeStreamID(i, 0))
	}
	return s
}

func (s *streams) nodeRand(i int) Rand {
	return s.node[i]
}

// reseedNode gives a restarted node a fresh sequence. Without it a node that
// crash-loops replays the same election timeouts every time, and whole
// interleavings become unreachable.
func (s *streams) reseedNode(i int, epoch uint64) {
	s.node[i] = newStream(s.seed, nodeStreamID(i, epoch))
}

func newStream(seed, id uint64) *rand.Rand {
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
