package sim

import (
	"fmt"
)

/*
1. Delay
2. FIFO (TCP) vs Reordering links
3. Loss
4. Duplication
5. Partition
	a. Symmetric split
	b. Asymmetric
	c. Bridge
	d. Flaky link

Configurable:
1. Delay distribution
2. Loss probability
3. Duplication probability
4. Partition predicate supporting asymmetric splits
5. FIFO/Reordering toggle
*/

type DelayKind uint8

const (
	DelayConstant DelayKind = iota
	DelayUniform
)

type DelaySpec struct {
	Kind   DelayKind // "constant", "uniform"
	Base   Duration  // fixed latency added to every sample
	Spread Duration  // width of the draw, so samples land in [Base, Base+Spread)
}

func (s DelaySpec) build() Delay {
	if s.Base < 0 {
		panic(fmt.Sprintf("sim: DelaySpec.Base %d is negative", s.Base))
	}
	switch s.Kind {
	case DelayConstant:
		return Constant{D: s.Base}
	case DelayUniform:
		if s.Spread < 0 {
			panic(fmt.Sprintf("sim: DelaySpec.Spread %d is negative", s.Spread))
		}
		return Uniform{Base: s.Base, Spread: s.Spread}
	default:
		panic(fmt.Sprintf("sim: unknown delay kind %d", s.Kind))
	}
}

type Delay interface {
	Sample(r Rand) Duration
}

type Constant struct {
	D Duration
}

func (c Constant) Sample(Rand) Duration {
	return c.D
}

type Uniform struct {
	Base   Duration
	Spread Duration
}

func (u Uniform) Sample(r Rand) Duration {
	if u.Spread <= 0 {
		return Duration(u.Base)
	}
	return u.Base + Duration(r.Int64N(int64(u.Spread)))
}

type link struct {
	delay      Delay
	lossPPM    uint32 // parts per million for percentage
	dupPPM     uint32
	reordering bool

	lastArrival Time // FIFO cursor

	epoch uint64 // bumped on connection reset
}

type RuleID uint64

// Reachability is an ordered list of rules, walked in order.
// Each rule has an ID so it can be removed individually
type rule struct {
	id    RuleID
	pred  func(from, to int, at Time) bool
	block bool
}

type Network struct {
	links [][]link // indexed by node index
	index map[int]nodeIdx
	order []int // sorted node ids

	rules  []rule
	nextID RuleID

	config NetworkConfig
}

type NetworkConfig struct {
	Delay          DelaySpec // serializable description, not the Delay interface
	LossPPM        uint32
	DuplicationPPM uint32
	IsReordering   bool // default is FIFO
}

// Creates a new network struct with all links to default values
func newNetwork(
	config NetworkConfig,
	order []int,
	index map[int]nodeIdx,
) *Network {
	links := make([][]link, len(order))
	d := config.Delay.build()
	for i := range links {
		links[i] = make([]link, len(order))
		for j := range len(links[i]) {
			links[i][j] = link{
				delay:      d,
				reordering: config.IsReordering,
				lossPPM:    config.LossPPM,
				dupPPM:     config.DuplicationPPM,
			}
		}

	}
	return &Network{
		links:  links,
		config: config,
		index:  index,
		order:  order,
		nextID: 1, // rule(0): heal everything
	}
}

func (n *Network) idx(id int) nodeIdx {
	i, ok := n.index[id]
	if !ok {
		panic(fmt.Sprintf("sim: unknown node %d", id))
	}
	return i
}

// creates a lookup table for existence of given ids in whole sim ids
func (n *Network) nodeSet(ids []int) []bool {
	lookup := make([]bool, len(n.order))
	for _, id := range ids {
		lookup[n.idx(id)] = true
	}
	return lookup
}

func (n *Network) link(from, to int) *link {
	fromIndex, ok := n.index[from]
	if !ok {
		panic(fmt.Sprintf("sim: no link for %d", from))
	}
	toIndex, ok := n.index[to]
	if !ok {
		panic(fmt.Sprintf("sim: no link for %d", to))
	}
	return &n.links[fromIndex][toIndex]
}

// pred: does this rule match?
// block: what to do on match
// let you writing an allow rule that punches a hole through a block
// a bridge node reaching both sides of a partition,
// which is the topology that produces the nastiest split-brain cases
func (n *Network) reachable(from, to int, at Time) bool {
	ok := true
	for _, r := range n.rules {
		if r.pred(from, to, at) {
			ok = !r.block // last match wins
		}
	}
	return ok
}

func (n *Network) addRule(pred func(from, to int, at Time) bool, block bool) RuleID {
	id := n.nextID
	n.nextID++
	n.rules = append(n.rules, rule{
		id:    id,
		pred:  pred,
		block: block,
	})
	return id
}

func (n *Network) removeRule(id RuleID) {
	for i, r := range n.rules {
		if r.id == id {
			n.rules = append(n.rules[:i], n.rules[i+1:]...)
			break
		}
	}
}

func (n *Network) partition(a, b []int) RuleID {
	setA := n.nodeSet(a)
	setB := n.nodeSet(b)
	for i := range setA {
		if setA[i] && setB[i] {
			panic(fmt.Sprintf("sim: partition: node %d on both sides", n.order[i]))
		}
	}
	return n.addRule(func(from, to int, at Time) bool {
		f, t := n.idx(from), n.idx(to)
		return (setA[f] && setB[t]) || (setA[t] && setB[f])
	}, true)
}

func (n *Network) blockDirection(src, dst []int) RuleID {
	out := n.nodeSet(src)
	in := n.nodeSet(dst)
	return n.addRule(func(from, to int, at Time) bool {
		return out[n.idx(from)] && in[n.idx(to)]
	}, true)
}

func (n *Network) isolate(node int) RuleID {
	i := n.idx(node)
	return n.addRule(func(from, to int, _ Time) bool {
		return n.idx(from) == i || n.idx(to) == i
	}, true)
}

func (n *Network) heal(id RuleID) {
	if id == 0 {
		n.healAll()
		return
	}
	n.removeRule(id)
}

func (n *Network) healAll() {
	n.rules = n.rules[:0]
}

func (n *Network) resetConnection(from, to int) {
	l := n.link(from, to)
	l.epoch++
	l.lastArrival = 0
}

func (n *Network) setLoss(from, to int, ppm uint32) {
	l := n.link(from, to)
	l.lossPPM = ppm
}

func (n *Network) setDup(from, to int, ppm uint32) {
	l := n.link(from, to)
	l.dupPPM = ppm
}

func (n *Network) setReordering(from, to int, on bool) {
	l := n.link(from, to)
	// Turning FIFO back on with a stale cursor would let a new message land
	// before an older one. Start the ordering window here instead.
	if l.reordering && !on {
		l.lastArrival = 0
	}
	l.reordering = on
}
