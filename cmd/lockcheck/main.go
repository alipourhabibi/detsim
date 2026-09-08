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
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	lockharness "github.com/alipourhabibi/detsim/harness/lockserver"
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
	seeds     = flag.String("seeds", "1..500", "seed range to sweep, as from..to")
	seed      = flag.Int64("seed", -1, "replay one seed instead of sweeping")
	traceMode = flag.String("trace", "storage", "storage, history, all or none")
	out       = flag.String("out", "", "write the report to this file instead of stdout")
	mermaid   = flag.Bool("mermaid", false, "print a mermaid sequence diagram")
	noShrink  = flag.Bool("no-shrink", false, "skip shrinking, keep the full schedule")
	checkMode = flag.String("check", "all", "which oracle to run: invariant, history, liveness or all")
)

func main() {
	flag.Parse()

	c := &checker{nodes: []int{0}} // 0 is the server
	for i := 1; i <= clients; i++ {
		c.nodes = append(c.nodes, i)
	}

	if *seed >= 0 {
		os.Exit(c.runOne(uint64(*seed)))
	}
	os.Exit(c.sweep())
}

// checker holds the cluster from the most recent build.
type checker struct {
	nodes   []int
	current *lockharness.Cluster
}

func (c *checker) createPlan(s uint64, r sim.Rand) *sim.Plan {
	config := sim.PlanConfig{
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
	}

	return sim.GeneratePlan(config, c.nodes, r)
}

func (c *checker) build(s uint64, p *sim.Plan) *sim.Sim {
	config := sim.Config{
		Seed:       s,
		MaxEvents:  200_000,
		TraceLevel: sim.TraceHashEvents,
		TraceKeep:  sim.KeepAll,
		NetworkConfig: sim.NetworkConfig{
			Delay:   sim.DelaySpec{Kind: sim.DelayUniform, Base: 2, Spread: 8},
			LossPPM: 20_000, // 2 percent
		},
	}
	cl := lockharness.Build(config, clients, retryMs, leaseMs)
	cl.Sim.Start()
	p.Apply(cl.Sim)
	c.current = cl
	return cl.Sim
}

func (c *checker) check(s *sim.Sim) error {
	only := *checkMode
	cl := c.current

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
func (c *checker) fail(s uint64, plan *sim.Plan, err error) int {
	if *noShrink {
		c.report(s, plan, err)
		return 1
	}

	small := sim.Shrink(plan, func(p *sim.Plan) bool {
		return c.check(c.build(s, p)) != nil // same seed
	})
	fmt.Printf("shrunk %d faults to %d\n", plan.Len(), small.Len())

	// Shrink's last attempt is usually a rejected one, and it is not in the plan
	// So we have to run the winner once more so the trace and the history are right
	if smallErr := c.check(c.build(s, small)); smallErr != nil {
		c.report(s, small, smallErr)
		return 1
	}

	fmt.Fprintln(os.Stderr, "\nWARNING: the shrunk plan does not reproduce. The run is not deterministic, so nothing here can be trusted.")
	c.report(s, plan, err)
	return 2
}

func (c *checker) report(s uint64, plan *sim.Plan, err error) {
	w, done := openOut()
	defer done()

	fmt.Fprintf(w, "\nseed %d\n%s\n\n", s, err)

	fmt.Fprintf(w, "schedule (%d faults):\n", plan.Len())
	for _, f := range plan.Faults {
		fmt.Fprintf(w, "t=%-6d %s\n", f.At, f.Fault)
	}
	fmt.Fprintln(w)

	h := c.current.Sim.History()
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
		c.current.Sim.Trace().DumpStorage(w)
	case "all":
		c.current.Sim.Trace().Dump(w)
	}

	if *mermaid {
		fmt.Fprintln(w, "\nmermaid:")
		fmt.Fprintln(w)
		c.current.Sim.Trace().Mermaid(w, c.nodes)
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

	err := c.check(c.build(s, plan))
	if err == nil {
		fmt.Printf("seed %d: clean, %d faults over %d ms\n", s, plan.Len(), until)
		return 0
	}
	return c.fail(s, plan, err)
}

// sweep walks the range and stops at the first failure.
func (c *checker) sweep() int {
	from, to, err := parseRange(*seeds)
	if err != nil {
		fmt.Fprintf(os.Stderr, "lockcheck: bad -seeds %q: %v\n", *seeds, err)
		return 2
	}

	fmt.Printf("sweeping seeds %d..%d\n", from, to)

	f := sim.Sweep(from, to, c.createPlan, c.build, c.check)
	if f == nil {
		fmt.Printf("clean: %d seeds, no violation\n", to-from)
		return 0
	}

	fmt.Printf("failed at seed %d\n", f.Seed)
	code := c.fail(f.Seed, f.Plan, f.Err)
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
