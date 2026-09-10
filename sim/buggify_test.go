package sim

import (
	"fmt"
	"slices"
	"testing"
)

func testBuggifyNames() []string {
	return []string{"a/one", "a/two", "b/three", "b/four", "c/five"}
}

func testBuggifyConfig(percent int) PlanConfig {
	cfg := testPlanConfig()
	cfg.Buggify = testBuggifyNames()
	cfg.BuggifyPercent = percent
	return cfg
}

func TestPickBuggifyIsDeterministic(t *testing.T) {
	a := pickBuggify(42, testBuggifyNames(), 25)
	b := pickBuggify(42, testBuggifyNames(), 25)

	if !slices.Equal(a, b) {
		t.Fatalf("same seed gave %v then %v", a, b)
	}
}

func TestPickBuggifyVariesBySeed(t *testing.T) {
	names := make([]string, 40)
	for i := range names {
		names[i] = fmt.Sprintf("p/%d", i)
	}

	a := pickBuggify(1, names, 25)
	b := pickBuggify(2, names, 25)

	if slices.Equal(a, b) {
		t.Fatal("two seeds turned on the same set of 40 points")
	}
}

// The order the caller lists the names in must not change the result.
func TestPickBuggifyIgnoresNameOrder(t *testing.T) {
	sorted := []string{"a/one", "a/two", "b/three"}
	jumbled := []string{"b/three", "a/one", "a/two"}

	a := pickBuggify(7, sorted, 50)
	b := pickBuggify(7, jumbled, 50)

	if !slices.Equal(a, b) {
		t.Fatalf("name order changed the set: %v vs %v", a, b)
	}
	if !slices.Equal(jumbled, []string{"b/three", "a/one", "a/two"}) {
		t.Fatal("pickBuggify sorted the caller's slice")
	}
}

func TestPickBuggifyPercentZeroPicksNothing(t *testing.T) {
	if on := pickBuggify(1, testBuggifyNames(), 0); len(on) != 0 {
		t.Fatalf("percent 0 turned on %v", on)
	}
}

func TestPickBuggifyPercentHundredPicksAll(t *testing.T) {
	names := testBuggifyNames()
	if on := pickBuggify(1, names, 100); len(on) != len(names) {
		t.Fatalf("percent 100 turned on %d of %d", len(on), len(names))
	}
}

func TestAddingAPointDoesNotMoveTheOthers(t *testing.T) {
	const seed = 13

	few := []string{"a/one", "a/two", "b/three"}
	many := append(slices.Clone(few), "z/new", "z/newer")

	a := pickBuggify(seed, few, 50)
	b := pickBuggify(seed, many, 50)

	// Everything that was on before must still be on.
	for _, name := range a {
		if !slices.Contains(b, name) {
			t.Fatalf("%q was on before the new points and is off now", name)
		}
	}
	// And nothing from the old list turned on that was off before.
	for _, name := range b {
		if slices.Contains(few, name) && !slices.Contains(a, name) {
			t.Fatalf("%q was off before the new points and is on now", name)
		}
	}
}

// The faults must not move either, which follows from the points using no
// stream at all.
func TestBuggifyDoesNotMoveFaultDraws(t *testing.T) {
	const seed = 13

	few := testPlanConfig()
	few.Buggify = []string{"a/one"}
	few.BuggifyPercent = 25

	a := plan(few, testNodes, seed)
	b := plan(testBuggifyConfig(25), testNodes, seed)

	if len(a.Faults) != len(b.Faults) {
		t.Fatalf("more buggify points changed the fault count: %d then %d",
			len(a.Faults), len(b.Faults))
	}
	for i := range a.Faults {
		if a.Faults[i].At != b.Faults[i].At {
			t.Fatalf("fault %d moved from t=%d to t=%d",
				i, a.Faults[i].At, b.Faults[i].At)
		}
		if !a.Faults[i].Fault.Equal(b.Faults[i].Fault) {
			t.Fatalf("fault %d changed:\n%v\n%v", i, a.Faults[i], b.Faults[i])
		}
	}
}

func TestPickBuggifyPercentIsRoughlyRight(t *testing.T) {
	names := make([]string, 1000)
	for i := range names {
		names[i] = fmt.Sprintf("p/%d", i)
	}

	on := pickBuggify(1, names, 25)

	if len(on) < 200 || len(on) > 300 {
		t.Fatalf("25 percent of 1000 turned on %d, want roughly 250", len(on))
	}
}

func TestPickBuggifyOnlyPicksListedNames(t *testing.T) {
	names := testBuggifyNames()
	on := pickBuggify(3, names, 100)

	for _, n := range on {
		if !slices.Contains(names, n) {
			t.Fatalf("turned on %q, which is not in the list", n)
		}
	}
}

func TestShrinkRemovesBuggifyPoints(t *testing.T) {
	// Fails while a/one is on, whatever else is there.
	fails := func(p *Plan) bool {
		return slices.Contains(p.Buggify, "a/one")
	}

	p := &Plan{
		Faults:  []Scheduled{{100, NewPauseFault(0, 10)}},
		Buggify: []string{"a/one", "a/two", "b/three", "b/four"},
	}

	got := Shrink(p, fails)

	if len(got.Buggify) != 1 || got.Buggify[0] != "a/one" {
		t.Fatalf("shrink left %v, want only a/one", got.Buggify)
	}
	if len(got.Faults) != 0 {
		t.Fatalf("shrink kept %d faults that did not matter", len(got.Faults))
	}
}

func TestPlanCloneCopiesBuggify(t *testing.T) {
	p := &Plan{Buggify: []string{"a/one", "a/two"}}
	c := p.Clone()
	c.Buggify[0] = "changed"

	if p.Buggify[0] != "a/one" {
		t.Fatal("Clone shared the buggify slice with the original")
	}
}

// A point that is on fires every visit. A point that is off never fires.
//
// Not a coin flip each time. The node has to live with the same bad thing over
// and over, and that is what finds bugs.
func TestBuggifyFiresEveryVisitWhenOn(t *testing.T) {
	s := New(testConfig(1), []int{0, 1}, noop)
	s.setBuggify([]string{"on"})
	s.Start()

	for range 10 {
		if !s.buggify(0, "on") {
			t.Fatal("a point that is on did not fire")
		}
		if s.buggify(0, "off") {
			t.Fatal("a point that is off fired")
		}
	}

	byName := map[string]BuggifyCount{}
	for _, c := range s.BuggifyCounts() {
		byName[c.Name] = c
	}

	if got := byName["on"]; got.Seen != 10 || got.Fired != 10 {
		t.Fatalf("on: seen=%d fired=%d, want 10 and 10", got.Seen, got.Fired)
	}

	// Seen counts every visit, even when the point is off. That is how you
	// tell a point the code never reached from one that is simply not on.
	if got := byName["off"]; got.Seen != 10 || got.Fired != 0 {
		t.Fatalf("off: seen=%d fired=%d, want 10 and 0", got.Seen, got.Fired)
	}
}
