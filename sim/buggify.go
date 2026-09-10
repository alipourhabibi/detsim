package sim

import (
	"fmt"
	"hash/fnv"
	"slices"
)

// Buggify says whether to take the bad path at a marked place in the protocol.
func (c *Ctx) Buggify(name string) bool {
	c.check()
	return c.sim.buggify(c.self, name)
}

func (s *Sim) buggify(node int, name string) bool {
	// Count every visit, even when off. Without this you cannot tell a point
	// that never fired from a point the code never reached. See BuggifyCounts.
	s.buggifySeen[name]++

	if !s.buggifyOn[name] {
		return false
	}

	s.buggifyFired[name]++
	s.trace.Note(EnBuggify, s.now, node, name, s.current)
	return true
}

// setBuggify turns on the points the Plan chose. Called by Plan.Apply.
func (s *Sim) setBuggify(names []string) {
	clear(s.buggifyOn)
	for _, n := range names {
		s.buggifyOn[n] = true
	}
}

// BuggifyCount is how often one point was reached and how often it fired.
type BuggifyCount struct {
	Name  string
	Seen  int
	Fired int
}

// BuggifyCounts reports every point this run reached.
//
// Seen counts visits. Fired counts the ones that took the bad path. A point
// with Seen of zero means the code never got there in this run.
//
// Sorted by name, not map order.
func (s *Sim) BuggifyCounts() []BuggifyCount {
	out := make([]BuggifyCount, 0, len(s.buggifySeen))
	for name, seen := range s.buggifySeen {
		out = append(out, BuggifyCount{
			Name:  name,
			Seen:  seen,
			Fired: s.buggifyFired[name],
		})
	}
	slices.SortFunc(out, func(a, b BuggifyCount) int {
		switch {
		case a.Name < b.Name:
			return -1
		case a.Name > b.Name:
			return 1
		default:
			return 0
		}
	})
	return out
}

// pickBuggify decides which points are on for this run.
//
// r here is the buggify stream, not the fault stream.
func pickBuggify(seed uint64, names []string, percent int) []string {
	if percent < 0 || percent > 100 {
		panic(fmt.Sprintf("sim: BuggifyPercent must be 0..100, got %d", percent))
	}

	if percent == 0 || len(names) == 0 {
		return nil
	}

	var on []string
	for _, name := range names {
		if buggifyOn(seed, name, percent) {
			on = append(on, name)
		}
	}

	slices.Sort(on)
	return on
}

// buggifyOn is the yes or no for one point.
func buggifyOn(seed uint64, name string, percent int) bool {
	h := fnv.New64a()
	fmt.Fprintf(h, "%d/%s", seed, name)
	return h.Sum64()%100 < uint64(percent)
}
