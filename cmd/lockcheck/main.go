// Command lockcheck hunts for mutual exclusion violations in the lockserver.
//
//	go run ./cmd/lockcheck                          sweep seeds 1..500
//	go run ./cmd/lockcheck -seeds 1..10000          sweep further
//	go run ./cmd/lockcheck -seed 1                  replay one seed
//	go run ./cmd/lockcheck -seed 1 -trace all       show the whole trace
//	go run ./cmd/lockcheck -seed 1 -trace history   what the clients saw
//	go run ./cmd/lockcheck -seed 1 -no-shrink       keep the full schedule
//	go run ./cmd/lockcheck -mermaid                 a diagram to paste in an issue
//	go run ./cmd/lockcheck -out report.txt          write the report to a file
//
// Exits 1 when it finds a violation, so it works in a shell loop. Exit 2 means
// the run was not reproducible, which makes every other result untrustworthy.
package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	lockharness "github.com/alipourhabibi/detsim/harness/lockserver"
	"github.com/alipourhabibi/detsim/protocol/lockserver"
	"github.com/alipourhabibi/detsim/sim"
)

const (
	clients    = 3
	retryMs    = 100
	leaseMs    = 300
	until      = sim.Time(4000)
	healBudget = sim.Duration(10 * (retryMs + leaseMs))
)

var (
	seeds      = flag.String("seeds", "1..500", "seed range to sweep, as from..to")
	seed       = flag.Int64("seed", -1, "replay one seed instead of sweeping")
	traceMode  = flag.String("trace", "storage", "storage, history, all or none")
	out        = flag.String("out", "", "write the report to this file instead of stdout")
	mermaid    = flag.Bool("mermaid", false, "print a mermaid sequence diagram")
	noShrink   = flag.Bool("no-shrink", false, "skip shrinking, keep the full schedule")
	checkMode  = flag.String("check", "all", "which oracle to run: invariant, history, liveness or all")
	corpusPath = flag.String("corpus", "", "file of known-bad seeds")
	rerun      = flag.Bool("rerun", false, "run only the seeds in -corpus")
)

func main() {
	flag.Parse()

	c := &checker{nodes: []int{0}} // 0 is the server
	for i := 1; i <= clients; i++ {
		c.nodes = append(c.nodes, i)
	}

	if *rerun {
		os.Exit(c.rerunCorpus())
	}

	if *seed >= 0 {
		os.Exit(c.runOne(uint64(*seed)))
	}
	os.Exit(c.sweep())
}

// checker holds the cluster from the most recent build.
type checker struct {
	nodes []int
}

func (c *checker) simConfig(seed uint64) sim.Config {
	return sim.Config{
		Seed:       seed,
		MaxEvents:  200_000,
		TraceLevel: sim.TraceHashEvents,
		TraceKeep:  sim.KeepAll,
		NetworkConfig: sim.NetworkConfig{
			Delay:   sim.DelaySpec{Kind: sim.DelayUniform, Base: 2, Spread: 8},
			LossPPM: 20_000, // 2 percent
		},
	}
}

func (c *checker) planConfig() sim.PlanConfig {
	return sim.PlanConfig{
		Until:       until,
		MeanGap:     250,
		MaxDown:     1,
		MaxDowntime: leaseMs / 2,
		MaxPause:    leaseMs * 4 / 3,
		WipeChance:  40,
		MaxDropPPM:  200_000,
		Weights: sim.Weights{
			Crash: 6, Pause: 3, Partition: 2, Isolate: 1, DropLink: 1, Heal: 3,
		},

		Buggify:        lockserver.AllBuggify,
		BuggifyPercent: 25,
	}
}

func (c *checker) createPlan(s uint64, r sim.Rand) *sim.Plan {
	return sim.GeneratePlan(c.planConfig(), c.nodes, s, r)
}

func (c *checker) build(s uint64, p *sim.Plan) (*sim.Sim, *lockharness.Cluster) {
	cl := lockharness.Build(c.simConfig(s), clients, retryMs, leaseMs)
	cl.Sim.Start()
	p.Apply(cl.Sim)
	return cl.Sim, cl
}

func (c *checker) check(s *sim.Sim, cl *lockharness.Cluster) error {
	only := *checkMode

	if only == "liveness" {
		s.ClearInvariants()
	}

	if err := s.RunUntil(until); err != nil {
		return fmt.Errorf("invariant: %w", err)
	}

	if only == "all" || only == "history" {
		if err := cl.CheckHistory(); err != nil {
			return fmt.Errorf("history: %w", err)
		}
	}

	if only == "all" || only == "liveness" {
		// RunHealed advances the clock, and the history check should see
		// the faulty phase only.
		if err := cl.CheckLiveness(healBudget); err != nil {
			return fmt.Errorf("liveness: %w", err)
		}
	}

	return nil
}

// fail shrinks the plan and prints the report. Returns the exit code.
func (c *checker) fail(s uint64, plan *sim.Plan, cl *lockharness.Cluster, err error) int {
	if *noShrink {
		c.report(s, plan, cl, err)
		return 1
	}

	small := sim.Shrink(plan, func(p *sim.Plan) bool {
		sm, scl := c.build(s, p)
		return c.check(sm, scl) != nil
	})
	fmt.Printf("shrunk %d faults to %d, %d buggify to %d\n",
		plan.Len(), small.Len(), plan.BuggifyLen(), small.BuggifyLen())

	sm, scl := c.build(s, small)
	if smallErr := c.check(sm, scl); smallErr != nil {
		c.report(s, small, scl, smallErr)
		return 1
	}

	fmt.Fprintln(os.Stderr, "\nWARNING: the shrunk plan does not reproduce. The run is not deterministic, so nothing here can be trusted.")
	c.report(s, plan, cl, err)
	return 2
}

func (c *checker) report(s uint64, plan *sim.Plan, cl *lockharness.Cluster, err error) {
	w, done := openOut()
	defer done()

	fmt.Fprintf(w, "\nseed %d\n%s\n\n", s, err)

	fmt.Fprintf(w, "schedule (%d faults):\n", plan.Len())
	for _, f := range plan.Faults {
		fmt.Fprintf(w, "t=%-6d %s\n", f.At, f.Fault)
	}
	for _, name := range plan.Buggify {
		fmt.Fprintf(w, "buggify  %s\n", name)
	}
	fmt.Fprintln(w)

	writeBuggify(w, cl)

	h := cl.Sim.History()
	ok, unknown, open := h.Counts()
	fmt.Fprintf(w, "history: %d ops, %d ok, %d unknown, %d open\n\n", h.Len(), ok, unknown, open)

	switch *traceMode {
	case "history":
		fmt.Fprintln(w, "history:")
		fmt.Fprint(w, h)
	case "storage":
		fmt.Fprintln(w, "storage trace:")
		fmt.Fprintln(w, "a write with no sync after it, then a rollback, is the bug.")
		fmt.Fprintln(w)
		cl.Sim.Trace().DumpStorage(w)
	case "all":
		cl.Sim.Trace().Dump(w)
	}

	if *mermaid {
		fmt.Fprintln(w, "\nmermaid:")
		fmt.Fprintln(w)
		cl.Sim.Trace().Mermaid(w, c.nodes)
	}

	if *out != "" {
		fmt.Printf("report written to %s\n", *out)
	}
}

// openOut returns where the report goes plus a close function. The close is a
// no-op for stdout: closing it would break every later Printf.
func openOut() (io.Writer, func()) {
	if *out == "" {
		return os.Stdout, func() {}
	}
	f, err := os.Create(*out)
	if err != nil {
		fmt.Fprintf(os.Stderr, "lockcheck: cannot write %s: %v\n", *out, err)
		os.Exit(2)
	}
	b := bufio.NewWriter(f)
	return b, func() {
		b.Flush()
		f.Close()
	}
}

// replays a single seed
func (c *checker) runOne(s uint64) int {
	plan := c.createPlan(s, sim.NewStream(s, sim.StreamFault))

	sim, cl := c.build(s, plan)
	err := c.check(sim, cl)
	if err == nil {
		fmt.Printf("seed %d: clean, %d faults over %d ms\n", s, plan.Len(), until)
		return 0
	}
	return c.fail(s, plan, cl, err)
}

// sweep walks the range and stops at the first failure.
func (c *checker) sweep() int {
	from, to, err := parseRange(*seeds)
	if err != nil {
		fmt.Fprintf(os.Stderr, "lockcheck: bad -seeds %q: %v\n", *seeds, err)
		return 2
	}

	fmt.Printf("sweeping seeds %d..%d\n", from, to)

	f, all := sim.Sweep(from, to, c.createPlan, c.build, c.check)

	if err := c.saveCorpus(all); err != nil {
		fmt.Fprintf(os.Stderr, "lockcheck: cannot save corpus: %v\n", err)
	}

	if f == nil {
		fmt.Printf("clean: %d seeds, no violation\n", to-from)
		return 0
	}

	fmt.Printf("failed at seed %d\n", f.Seed)
	code := c.fail(f.Seed, f.Plan, f.Ctx, f.Err)
	if code == 1 {
		fmt.Printf("\nreplay with: go run ./cmd/lockcheck -seed %d\n", f.Seed)
	}
	return code
}

func parseRange(s string) (uint64, uint64, error) {
	lo, hi, found := strings.Cut(s, "..")
	if !found {
		n, err := strconv.ParseUint(s, 10, 64)
		return n, n + 1, err
	}
	from, err := strconv.ParseUint(lo, 10, 64)
	if err != nil {
		return 0, 0, err
	}
	to, err := strconv.ParseUint(hi, 10, 64)
	if err != nil {
		return 0, 0, err
	}
	if to <= from {
		return 0, 0, fmt.Errorf("empty range")
	}
	return from, to, nil
}

func (c *checker) saveCorpus(fails []sim.SeedFailure) error {
	if *corpusPath == "" || len(fails) == 0 {
		return nil
	}

	var old []sim.SeedEntry
	if f, err := os.Open(*corpusPath); err == nil {
		old, err = sim.LoadCorpus(f)
		f.Close()
		switch {
		case errors.Is(err, sim.ErrCorpusVersion):
			fmt.Fprintf(os.Stderr, "lockcheck: %v, starting a new corpus\n", err)
			old = nil

		case err != nil:
			return fmt.Errorf("corpus is unreadable, not overwriting: %w", err)
		}
	}

	f, err := os.Create(*corpusPath)
	if err != nil {
		return err
	}
	defer f.Close()

	header := fmt.Sprintf("%s\n%s", c.simConfig(0), c.planConfig())
	return sim.SaveCorpus(f, header, sim.MergeCorpus(old, fails))
}

// rerunCorpus runs only the seeds on file. This is the regression suite
func (c *checker) rerunCorpus() int {
	f, err := os.Open(*corpusPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "lockcheck: %v\n", err)
		return 2
	}
	entries, err := sim.LoadCorpus(f)
	f.Close()
	if err != nil {
		fmt.Fprintf(os.Stderr, "lockcheck: %v\n", err)
		return 2
	}
	seeds := sim.Seeds(entries)

	fmt.Printf("rerunning %d seeds from %s\n", len(seeds), *corpusPath)

	var failed []uint64
	for _, seed := range seeds {
		plan := c.createPlan(seed, sim.NewStream(seed, sim.StreamFault))
		if err := c.check(c.build(seed, plan)); err != nil {
			fmt.Printf("  seed %d still fails: %v\n", seed, err)
			failed = append(failed, seed)
		}
	}

	if len(failed) == 0 {
		fmt.Printf("all %d seeds pass\n", len(seeds))
		return 0
	}
	fmt.Printf("%d of %d seeds still fail\n", len(failed), len(seeds))
	return 1
}

// writeBuggify prints which points the run reached and which fired.
func writeBuggify(w io.Writer, cl *lockharness.Cluster) {
	counts := cl.Sim.BuggifyCounts()
	if len(counts) == 0 {
		return
	}

	fmt.Fprintln(w, "buggify:")
	for _, b := range counts {
		mark := " "
		if b.Seen > 0 && b.Fired == 0 {
			mark = "." // reached, never fired: this point was off
		}
		if b.Seen == 0 {
			mark = "!" // never reached: the code did not get there
		}
		fmt.Fprintf(w, "  %s %-30s seen=%-6d fired=%d\n", mark, b.Name, b.Seen, b.Fired)
	}
	fmt.Fprintln(w)
}
