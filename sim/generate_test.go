package sim

import (
	"slices"
	"testing"
)

func testPlanConfig() PlanConfig {
	cfg := PlanConfig{
		Until:   10_000,
		MeanGap: 200,
		MaxDown: 1,
		Weights: DefaultWeights(),
	}
	cfg.setDefaults()
	return cfg
}

var testNodes = []int{0, 1, 2, 3, 4}

// countingRand wraps a stream and counts draws. Used to assert that the number
// of draws is a function of the seed alone, never of state.
type countingRand struct {
	inner Rand
	draws int
}

func (c *countingRand) Int64N(n int64) int64 {
	c.draws++
	return c.inner.Int64N(n)
}

// --- the premise -------------------------------------------------------------

func TestGeneratePlanIsDeterministic(t *testing.T) {
	a := GeneratePlan(testPlanConfig(), testNodes, NewStream(42, StreamFault))
	b := GeneratePlan(testPlanConfig(), testNodes, NewStream(42, StreamFault))

	if a.Len() != b.Len() {
		t.Fatalf("same seed produced %d and %d faults", a.Len(), b.Len())
	}
	if a.String() != b.String() {
		t.Fatalf("same seed produced different plans:\n%s\n---\n%s", a, b)
	}
}

func TestGeneratePlanVariesBySeed(t *testing.T) {
	a := GeneratePlan(testPlanConfig(), testNodes, NewStream(1, StreamFault))
	b := GeneratePlan(testPlanConfig(), testNodes, NewStream(2, StreamFault))

	if a.String() == b.String() {
		t.Fatal("different seeds produced the same plan")
	}
}

// The caller's slice is not ours to sort, and the plan must not depend on the
// order it arrives in.
func TestGeneratePlanIgnoresNodeOrder(t *testing.T) {
	sorted := []int{0, 1, 2, 3, 4}
	jumbled := []int{3, 0, 4, 1, 2}

	a := GeneratePlan(testPlanConfig(), sorted, NewStream(7, StreamFault))
	b := GeneratePlan(testPlanConfig(), jumbled, NewStream(7, StreamFault))

	if a.String() != b.String() {
		t.Fatal("node ordering changed the plan")
	}
	if !slices.Equal(jumbled, []int{3, 0, 4, 1, 2}) {
		t.Fatal("GeneratePlan sorted the caller's slice in place")
	}
}

// --- the draw-count invariant ------------------------------------------------

// Every branch draws before it guards, so the draw count depends on the seed
// alone. If a guard ever moves ahead of a draw, the plan stops being a pure
// function of the seed and the corpus becomes worthless.
//
// MaxDown 0 and MaxDown 5 take completely different paths through the append
// logic and must still consume the stream identically.
func TestGeneratePlanDrawCountIsStateIndependent(t *testing.T) {
	count := func(maxDown int) int {
		cfg := testPlanConfig()
		cfg.MaxDown = maxDown
		r := &countingRand{inner: NewStream(99, StreamFault)}
		GeneratePlan(cfg, testNodes, r)
		return r.draws
	}

	none, all := count(0), count(len(testNodes))
	if none != all {
		t.Fatalf("MaxDown=0 made %d draws, MaxDown=%d made %d; a guard is "+
			"running before a draw", none, len(testNodes), all)
	}
}

// Same property from the other side: excluding nodes must not change how many
// times pickNode draws.
func TestPickNodeDrawsOnceRegardless(t *testing.T) {
	for _, exclude := range []map[int]bool{
		{},
		{0: true, 1: true},
		{0: true, 1: true, 2: true, 3: true, 4: true}, // nothing available
	} {
		r := &countingRand{inner: NewStream(1, StreamFault)}
		pickNode(r, testNodes, exclude)
		if r.draws > 1 {
			t.Fatalf("pickNode made %d draws with %d excluded, want at most 1",
				r.draws, len(exclude))
		}
	}
}

func TestExpGapAlwaysDrawsFourTimes(t *testing.T) {
	r := &countingRand{inner: NewStream(1, StreamFault)}
	nextGap(r, 100)
	if r.draws != 4 {
		t.Fatalf("expGap made %d draws, want exactly 4", r.draws)
	}
}

// --- constraints -------------------------------------------------------------

// MaxDown must hold across the whole plan, not just at the moment each crash is
// drawn. Walk the schedule and track overlapping downtime.
func TestGeneratePlanRespectsMaxDown(t *testing.T) {
	const maxDown = 2
	cfg := testPlanConfig()
	cfg.MaxDown = maxDown

	for seed := uint64(1); seed <= 50; seed++ {
		p := GeneratePlan(cfg, testNodes, NewStream(seed, StreamFault))

		downUntil := map[int]Time{}
		for _, f := range p.Faults {
			for id, until := range downUntil {
				if until <= f.At {
					delete(downUntil, id)
				}
			}
			c, ok := f.Fault.(crashFault)
			if !ok {
				continue
			}
			downUntil[c.Node] = f.At.Add(c.Downtime)
			if len(downUntil) > maxDown {
				t.Fatalf("seed %d: %d nodes down at t=%d, MaxDown is %d\n%s",
					seed, len(downUntil), f.At, maxDown, p)
			}
		}
	}
}

// A zero weight excludes a kind entirely rather than giving it a small chance.
func TestZeroWeightKindNeverGenerated(t *testing.T) {
	cfg := testPlanConfig()
	cfg.Weights = Weights{Crash: 0, Pause: 1, Partition: 1, Heal: 1}

	for seed := uint64(1); seed <= 50; seed++ {
		p := GeneratePlan(cfg, testNodes, NewStream(seed, StreamFault))
		for _, f := range p.Faults {
			if _, isCrash := f.Fault.(crashFault); isCrash {
				t.Fatalf("seed %d generated a crash with Crash weight 0", seed)
			}
		}
	}
}

func TestAllZeroWeightsPanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("all-zero weights did not panic")
		}
	}()
	cfg := testPlanConfig()
	cfg.Weights = Weights{}
	GeneratePlan(cfg, testNodes, NewStream(1, StreamFault))
}

func TestGeneratePlanIsSortedByTime(t *testing.T) {
	p := GeneratePlan(testPlanConfig(), testNodes, NewStream(5, StreamFault))
	for i := 1; i < p.Len(); i++ {
		if p.Faults[i].At < p.Faults[i-1].At {
			t.Fatalf("fault %d at t=%d follows one at t=%d",
				i, p.Faults[i].At, p.Faults[i-1].At)
		}
	}
}

func TestGeneratePlanStaysWithinUntil(t *testing.T) {
	cfg := testPlanConfig()
	p := GeneratePlan(cfg, testNodes, NewStream(5, StreamFault))
	for _, f := range p.Faults {
		if f.At >= cfg.Until {
			t.Fatalf("fault at t=%d, Until is %d", f.At, cfg.Until)
		}
	}
}

// --- selection helpers -------------------------------------------------------

// A partition with an empty side is not a partition. Both groups must be
// non-empty for every draw.
func TestSplitNodesBothSidesNonEmpty(t *testing.T) {
	r := NewStream(1, StreamFault)
	for range 1000 {
		a, b, ok := splitNodes(r, testNodes)
		if !ok {
			t.Fatal("splitNodes failed on a 5-node cluster")
		}
		if len(a) == 0 || len(b) == 0 {
			t.Fatalf("empty side: a=%v b=%v", a, b)
		}
		if len(a)+len(b) != len(testNodes) {
			t.Fatalf("split lost or duplicated nodes: a=%v b=%v", a, b)
		}
		if !slices.IsSorted(a) || !slices.IsSorted(b) {
			t.Fatalf("sides not sorted: a=%v b=%v", a, b)
		}
	}
}

func TestSplitNodesNeedsTwoNodes(t *testing.T) {
	r := NewStream(1, StreamFault)
	if _, _, ok := splitNodes(r, []int{0}); ok {
		t.Fatal("splitNodes succeeded on a single node")
	}
}

func TestPickPairIsDistinct(t *testing.T) {
	r := NewStream(1, StreamFault)
	for range 1000 {
		from, to, ok := pickPair(r, testNodes)
		if !ok {
			t.Fatal("pickPair failed on a 5-node cluster")
		}
		if from == to {
			t.Fatalf("pickPair returned %d twice", from)
		}
	}
}

func TestPickNodeRespectsExclusion(t *testing.T) {
	r := NewStream(1, StreamFault)
	exclude := map[int]bool{0: true, 2: true, 4: true}

	for range 500 {
		id, ok := pickNode(r, testNodes, exclude)
		if !ok {
			t.Fatal("pickNode found nothing with two nodes available")
		}
		if exclude[id] {
			t.Fatalf("pickNode returned excluded node %d", id)
		}
	}
}

func TestPickNodeReportsWhenEmpty(t *testing.T) {
	r := NewStream(1, StreamFault)
	all := map[int]bool{}
	for _, id := range testNodes {
		all[id] = true
	}
	if _, ok := pickNode(r, testNodes, all); ok {
		t.Fatal("pickNode succeeded with every node excluded")
	}
}

func TestShuffledIsAPermutation(t *testing.T) {
	r := NewStream(1, StreamFault)
	for range 200 {
		out := shuffled(r, testNodes)
		sorted := slices.Clone(out)
		slices.Sort(sorted)
		if !slices.Equal(sorted, testNodes) {
			t.Fatalf("shuffle is not a permutation: %v", out)
		}
	}
	if !slices.Equal(testNodes, []int{0, 1, 2, 3, 4}) {
		t.Fatal("shuffled mutated its input")
	}
}

func TestWeightTableSkipsZeroEntries(t *testing.T) {
	table, total := Weights{Crash: 3, Heal: 1}.table()
	if len(table) != 2 {
		t.Fatalf("table has %d entries, want 2", len(table))
	}
	if total != 4 {
		t.Fatalf("total = %d, want 4", total)
	}
	if table[len(table)-1].upTo != total {
		t.Fatalf("last cumulative = %d, want %d", table[len(table)-1].upTo, total)
	}
}

// --- replay ------------------------------------------------------------------

// The test that catches the subtle mistake, and the only one that will: if
// generation ever reads live sim state, the plan stops being replayable and
// nothing else here fails.
func TestPlanReplayIsStable(t *testing.T) {
	plan := GeneratePlan(testPlanConfig(), testNodes, NewStream(31, StreamFault))

	run := func() uint64 {
		s := New(testConfig(31), testNodes,
			func(id int, _ Deps) Handler {
				return &pingOnTimer{peer: (id + 1) % len(testNodes)}
			})
		s.Start()
		plan.Apply(s)
		if err := s.RunUntil(10_000); err != nil {
			t.Fatal(err)
		}
		return s.Hash()
	}

	if a, b := run(), run(); a != b {
		t.Fatalf("the same plan produced %016x then %016x", a, b)
	}
}
