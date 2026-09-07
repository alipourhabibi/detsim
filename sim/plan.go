package sim

import (
	"cmp"
	"fmt"
	"slices"
	"strings"
)

// Scheduled is one fault at one time
type Scheduled struct {
	At    Time
	Fault Fault
}

// Plan is a complete fault schedule, drawn before the run starts.
type Plan struct {
	Faults []Scheduled
}

func (p *Plan) Apply(s *Sim) {
	for _, f := range p.Faults {
		s.ScheduleFault(f.At, f.Fault)
	}
}

func (p *Plan) Clone() *Plan {
	return &Plan{Faults: slices.Clone(p.Faults)}
}

func (p *Plan) Len() int {
	return len(p.Faults)
}

func (p *Plan) String() string {
	var b strings.Builder
	for _, f := range p.Faults {
		fmt.Fprintf(&b, "t=%d %s\n", f.At, f.Fault)
	}
	return b.String()
}

// describes the shape of a generated schedule.
type PlanConfig struct {
	Until   Time     // generate faults up to this instant
	MeanGap Duration // average spacing; smaller means more chaos
	MaxDown int      // never crash or isolate more than this many at once
	Weights Weights

	// MaxDowntime is the longest a crashed node stays down. Each crash picks a
	// random downtime from 1 to this value.
	//
	// Keep it larger than MeanGap. Then a node is often still down when the
	// next fault arrives, so failures overlap. Two nodes down at once finds
	// bugs that two nodes down one after the other never will.
	MaxDowntime Duration
	MaxPause    Duration // longest a pause lasts
	WipeChance  int64    // 1 in N crashes also wipes the disk; 0 means never
	MaxDropPPM  uint32   // worst loss a drop-link fault can set, in PPM
}

func (c *PlanConfig) setDefaults() {
	if c.MaxDowntime == 0 {
		c.MaxDowntime = c.MeanGap * 3
	}
	if c.MaxPause == 0 {
		c.MaxPause = c.MeanGap * 2
	}
	if c.MaxDropPPM == 0 {
		c.MaxDropPPM = 500_000 // 50%
	}
	if c.WipeChance == 0 {
		c.WipeChance = 20
	}
}

func (c PlanConfig) validate() {
	if c.Until <= 0 {
		panic(fmt.Sprintf("sim: PlanConfig.Until must be positive, got %d", c.Until))
	}
	if c.MeanGap <= 0 {
		panic(fmt.Sprintf("sim: PlanConfig.MeanGap must be positive, got %d", c.MeanGap))
	}
	if c.MaxDown < 0 {
		panic(fmt.Sprintf("sim: PlanConfig.MaxDown must be >= 0, got %d", c.MaxDown))
	}
	if c.MaxDowntime < 0 {
		panic(fmt.Sprintf("sim: MaxDowntime must be >= 0, got %d", c.MaxDowntime))
	}
	if c.MaxPause < 0 {
		panic(fmt.Sprintf("sim: MaxPause must be >= 0, got %d", c.MaxPause))
	}
	if c.WipeChance < 0 {
		panic(fmt.Sprintf("sim: WipeChance must be >= 0, got %d", c.WipeChance))
	}
	if c.MaxDropPPM == 0 || c.MaxDropPPM > 1_000_000 {
		panic(fmt.Sprintf("sim: MaxDropPPM must be 1..1000000, got %d", c.MaxDropPPM))
	}
}

// Weights biases the fault mix.
type Weights struct {
	Crash     int
	Pause     int
	Partition int
	Isolate   int
	DropLink  int
	Heal      int
}

func DefaultWeights() Weights {
	return Weights{
		Crash:     4,
		Pause:     2,
		Partition: 3,
		Isolate:   2,
		DropLink:  1,
		Heal:      3,
	}
}

type faultKind uint8

const (
	kindCrash faultKind = iota
	kindPause
	kindPartition
	kindIsolate
	kindDropLink
	kindHeal
)

type weightedKind struct {
	kind faultKind
	upTo int
}

// The entry list is written out in a fixed order rather than built from a map,
// because the order determines which kind each drawn integer maps to. A map
// here would make the same seed produce different plans on different runs.
func (w Weights) table() ([]weightedKind, int) {
	entries := [...]struct {
		kind   faultKind
		weight int
	}{
		{kindCrash, w.Crash},
		{kindPause, w.Pause},
		{kindPartition, w.Partition},
		{kindIsolate, w.Isolate},
		{kindDropLink, w.DropLink},
		{kindHeal, w.Heal},
	}

	out := make([]weightedKind, 0, len(entries))
	total := 0
	for _, e := range entries {
		if e.weight <= 0 {
			continue
		}
		total += e.weight
		out = append(out, weightedKind{kind: e.kind, upTo: total})
	}
	if total == 0 {
		panic("sim: all fault weights are zero")
	}
	return out, total
}

// pick draws one kind from the table.
func pick(r Rand, table []weightedKind, total int) faultKind {
	x := int(r.Int64N(int64(total)))
	for _, e := range table {
		if x < e.upTo {
			return e.kind
		}
	}
	return table[len(table)-1].kind // unreachable: x < total
}

// GeneratePlan draws a fault schedule from r.
//
// r must be a fresh stream derived from streamFault. Nothing here reads the
// live Sim: the generator maintains its own list of which nodes are down,
// because we do not want it to depend on a running sim
func GeneratePlan(cfg PlanConfig, nodes []int, r Rand) *Plan {
	cfg.setDefaults()
	cfg.validate()
	if len(nodes) == 0 {
		panic("sim: GeneratePlan with no nodes")
	}

	sorted := slices.Clone(nodes)
	slices.Sort(sorted) // never trust the caller's ordering

	table, total := cfg.Weights.table()
	p := &Plan{}

	// list of nodes and when they come back
	downUntil := map[int]Time{}

	for at := Time(0); ; {
		at = at.Add(nextGap(r, cfg.MeanGap))
		if at >= cfg.Until {
			break
		}

		// Expire finished downtime before deciding anything.
		for id, until := range downUntil {
			if until <= at {
				delete(downUntil, id)
			}
		}

		switch pick(r, table, total) {
		case kindCrash:
			downtime := Duration(1 + r.Int64N(int64(cfg.MaxDowntime)))
			wipe := r.Int64N(1+cfg.WipeChance) == 0 // disk loss is rare
			// draw first
			node, ok := pickNode(r, sorted, downMap(downUntil))
			if !ok || len(downUntil) >= cfg.MaxDown {
				continue
			}
			p.Faults = append(p.Faults,
				Scheduled{at, NewCrashFault(node, wipe, downtime)})
			downUntil[node] = at.Add(downtime)

		case kindPause:
			duration := Duration(1 + r.Int64N(int64(cfg.MaxPause)))
			node, ok := pickNode(r, sorted, downMap(downUntil))
			if !ok {
				continue
			}
			p.Faults = append(p.Faults,
				Scheduled{at, NewPauseFault(node, duration)})

		case kindPartition:
			a, b, ok := splitNodes(r, sorted)
			if !ok {
				continue
			}
			p.Faults = append(p.Faults, Scheduled{at, NewPartitionFault(a, b)})

		case kindIsolate:
			node, ok := pickNode(r, sorted, downMap(downUntil))
			if !ok {
				continue
			}
			p.Faults = append(p.Faults, Scheduled{at, NewIsolateFault(node)})

		case kindDropLink:
			ppm := uint32(r.Int64N(int64(cfg.MaxDropPPM)))
			from, to, ok := pickPair(r, sorted)
			if !ok {
				continue
			}
			p.Faults = append(p.Faults,
				Scheduled{at, NewDropLinkFault(from, to, ppm)})

		case kindHeal:
			p.Faults = append(p.Faults, Scheduled{at, NewHealFault(0)})
		}
	}

	slices.SortStableFunc(p.Faults, func(x, y Scheduled) int {
		return cmp.Compare(x.At, y.At)
	})
	return p
}

// return the down nodes with a map of node: true
func downMap(downUntil map[int]Time) map[int]bool {
	out := make(map[int]bool, len(downUntil))
	for id := range downUntil {
		out[id] = true
	}
	return out
}

// pickNode chooses uniformly from nodes not currently excluded.
//
// Returns false when every node is excluded; the caller skips that fault rather
// than forcing one, so the draw count stays independent of cluster state.
func pickNode(r Rand, nodes []int, exclude map[int]bool) (int, bool) {
	avail := make([]int, 0, len(nodes))
	for _, id := range nodes {
		if !exclude[id] {
			avail = append(avail, id)
		}
	}
	if len(avail) == 0 {
		return 0, false
	}
	return avail[r.Int64N(int64(len(avail)))], true
}

// pickPair chooses a distinct ordered pair for directed link faults.
func pickPair(r Rand, nodes []int) (int, int, bool) {
	if len(nodes) < 2 {
		return 0, 0, false
	}
	i := int(r.Int64N(int64(len(nodes))))
	j := int(r.Int64N(int64(len(nodes) - 1)))
	if j >= i {
		j++ // skip i without a retry loop, so the draw count is fixed
	}
	return nodes[i], nodes[j], true
}

// splitNodes partitions the cluster into two non-empty groups.
//
// Group sizes are drawn first, then membership by shuffle. Both groups are
// sorted so two equivalent splits compare equal.
func splitNodes(r Rand, nodes []int) ([]int, []int, bool) {
	n := len(nodes)
	if n < 2 {
		return nil, nil, false
	}

	k := int(r.Int64N(int64(n-1))) + 1 // 1..n-1, both groups non-empty
	perm := shuffled(r, nodes)

	a := slices.Clone(perm[:k])
	b := slices.Clone(perm[k:])
	slices.Sort(a)
	slices.Sort(b)
	return a, b, true
}

// shuffled returns a Fisher-Yates shuffle of in, leaving in untouched.
func shuffled(r Rand, in []int) []int {
	out := slices.Clone(in)
	for i := len(out) - 1; i > 0; i-- {
		j := int(r.Int64N(int64(i + 1)))
		out[i], out[j] = out[j], out[i]
	}
	return out
}

// nextGap picks the wait before the next fault.
//
// It adds three random numbers and halves them. The result is usually near the
// middle, never more than 1.5x mean. So the gaps are never very long. But long
// gaps are useful: the protocol needs quiet time to settle. That is why 1 gap
// in 20 is made 10 times bigger.
//
// It always draws 4 random numbers, even when it does not need the 4th. If it
// drew only sometimes, the same seed would give a different plan.
func nextGap(r Rand, mean Duration) Duration {
	m := int64(mean)
	g := (r.Int64N(m) + r.Int64N(m) + r.Int64N(m)) / 2

	if r.Int64N(20) == 0 {
		g *= 10
	}
	return Duration(max(1, g))
}
