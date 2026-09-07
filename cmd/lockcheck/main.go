// Command lockcheck hunts for mutual exclusion violations in the lockserver
// protocol.
//
//	go run ./cmd/lockcheck                    sweep 1..500, show the first failure
//	go run ./cmd/lockcheck -seeds 1..10000    sweep further
//	go run ./cmd/lockcheck -seed 1            replay one seed
//	go run ./cmd/lockcheck -seed 1 -trace all show the whole trace, not just storage
//	go run ./cmd/lockcheck -mermaid           print a diagram to paste in an issue
//	go run ./cmd/lockcheck -out report.txt    write the report to a file
//
// Exit status is 1 when a violation is found, so it works in a shell loop.
package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	lockharness "github.com/alipourhabibi/detsim/harness/lockserver"
	"github.com/alipourhabibi/detsim/sim"
)

type settings struct {
	from, to  uint64
	single    int64 // -1 when not set
	clients   int
	retryMs   int64
	leaseMs   int64
	until     sim.Time
	traceMode string
	out       string // "" means stdout
	mermaid   bool
	noShrink  bool
	quiet     bool
}

func parseFlags() settings {
	var (
		s     settings
		seeds string
		seed  int64
	)

	flag.StringVar(&seeds, "seeds", "1..500", "seed range to sweep, as from..to")
	flag.Int64Var(&seed, "seed", -1, "replay one seed instead of sweeping")
	flag.IntVar(&s.clients, "clients", 3, "number of clients")
	flag.Int64Var(&s.retryMs, "retry", 100, "client retry backoff in ms")
	flag.Int64Var(&s.leaseMs, "lease", 300, "how long a client holds the lock in ms")
	flag.Int64Var((*int64)(&s.until), "until", 4000, "how long to run each seed")
	flag.StringVar(&s.traceMode, "trace", "storage",
		"what to print on a failure: storage, all, none")
	flag.StringVar(&s.out, "out", "", "write the report to this file instead of stdout")
	flag.BoolVar(&s.mermaid, "mermaid", false, "print a mermaid sequence diagram")
	flag.BoolVar(&s.noShrink, "no-shrink", false, "skip shrinking, show the full schedule")
	flag.BoolVar(&s.quiet, "quiet", false, "print only the verdict line")
	flag.Parse()

	s.single = seed

	from, to, err := parseRange(seeds)
	if err != nil {
		fmt.Fprintf(os.Stderr, "lockcheck: bad -seeds %q: %v\n", seeds, err)
		os.Exit(2)
	}
	s.from, s.to = from, to

	switch s.traceMode {
	case "storage", "all", "none":
	default:
		fmt.Fprintf(os.Stderr, "lockcheck: -trace must be storage, all or none\n")
		os.Exit(2)
	}

	return s
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

type violation struct {
	At      sim.Time
	Clients []int
	Owner   int
}

func (v *violation) Error() string {
	return fmt.Sprintf("mutual exclusion broken at t=%d: clients %v are all "+
		"inside the critical section, durable owner record says %d",
		v.At, v.Clients, v.Owner)
}

// checker owns one cluster at a time.
type checker struct {
	set     settings
	current *lockharness.Cluster
}

func (c *checker) nodes() []int {
	ids := []int{0} // 0 is the server
	for i := 1; i <= c.set.clients; i++ {
		ids = append(ids, i)
	}
	return ids
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

// planConfig is tuned to the bug this tool hunts.
//
// MaxDowntime is shorter than the lease, so the server is back up while a
// client still believes it owns the lock. MaxPause is longer than the lease, so
// a paused client wakes up holding a lock that was reassigned. Crashes are
// weighted up because the missing fsync only shows after a reboot.
func (c *checker) planConfig() sim.PlanConfig {
	return sim.PlanConfig{
		Until:       c.set.until,
		MeanGap:     250,
		MaxDown:     1,
		MaxDowntime: sim.Duration(c.set.leaseMs / 2),
		MaxPause:    sim.Duration(c.set.leaseMs * 4 / 3),
		WipeChance:  40,
		MaxDropPPM:  200_000,
		Weights: sim.Weights{
			Crash:     6,
			Pause:     3,
			Partition: 2,
			Isolate:   1,
			DropLink:  1,
			Heal:      3,
		},
	}
}

func (c *checker) gen(seed uint64, r sim.Rand) *sim.Plan {
	return sim.GeneratePlan(c.planConfig(), c.nodes(), r)
}

func (c *checker) build(seed uint64, p *sim.Plan) *sim.Sim {
	cl := lockharness.Build(c.simConfig(seed), c.set.clients, c.set.retryMs, c.set.leaseMs)
	cl.Sim.Start()
	p.Apply(cl.Sim)
	c.current = cl
	return cl.Sim
}

// check steps one event at a time and looks at the invariant after each one.
func (c *checker) check(_ *sim.Sim) error {
	cl := c.current
	for cl.Sim.Now() < c.set.until {
		before := cl.Sim.EventCount()

		if err := cl.Sim.Step(1); err != nil {
			return err
		}
		if cl.Sim.EventCount() == before {
			return nil // queue drained
		}

		if h := cl.Holders(); len(h) > 1 {
			return &violation{At: cl.Sim.Now(), Clients: h, Owner: cl.ServerOwner()}
		}
	}
	return nil
}

// replay is what Shrink asks after every removal: does this smaller plan still
// break the invariant?
func (c *checker) replay(seed uint64, p *sim.Plan) bool {
	return c.check(c.build(seed, p)) != nil
}

// openOut returns where the report goes, plus a close function that is a no-op
// for stdout.
func (c *checker) openOut() (io.Writer, func()) {
	if c.set.out == "" {
		return os.Stdout, func() {}
	}
	f, err := os.Create(c.set.out)
	if err != nil {
		fmt.Fprintf(os.Stderr, "lockcheck: cannot write %s: %v\n", c.set.out, err)
		os.Exit(2)
	}
	b := bufio.NewWriter(f)
	return b, func() {
		b.Flush()
		f.Close()
	}
}

func (c *checker) report(seed uint64, plan *sim.Plan, err error) {
	w, closeOut := c.openOut()
	defer closeOut()

	fmt.Fprintf(w, "\nseed %d\n", seed)
	fmt.Fprintf(w, "%s\n\n", err)

	fmt.Fprintf(w, "schedule (%d faults):\n", plan.Len())
	fmt.Fprint(w, indent(plan.String()))
	fmt.Fprintln(w)

	if c.set.quiet {
		return
	}

	switch c.set.traceMode {
	case "storage":
		fmt.Fprintln(w, "durability trace:")
		fmt.Fprintln(w, "  a write with no sync after it, then a rollback, is the bug.")
		fmt.Fprintln(w)
		c.current.Sim.Trace().DumpStorage(w)
	case "all":
		fmt.Fprintln(w, "full trace:")
		c.current.Sim.Trace().Dump(w)
	}

	if c.set.mermaid {
		fmt.Fprintln(w, "\nmermaid (paste into a markdown viewer):")
		c.current.Sim.Trace().Mermaid(w, c.nodes())
	}

	if c.set.out != "" {
		fmt.Printf("report written to %s\n", c.set.out)
	}
}

func indent(s string) string {
	var b strings.Builder
	for line := range strings.Lines(s) {
		if strings.TrimSpace(line) != "" {
			b.WriteString("  " + line)
		}
	}
	return b.String()
}

func main() {
	set := parseFlags()
	c := &checker{set: set}

	if set.single >= 0 {
		os.Exit(runOne(c, uint64(set.single)))
	}
	os.Exit(sweep(c))
}

// runOne replays a single seed. Use it after a sweep names one, or to check
// that a seed from a bug report still reproduces.
func runOne(c *checker, seed uint64) int {
	plan := c.gen(seed, sim.NewStream(seed, sim.StreamFault))
	err := c.check(c.build(seed, plan))

	if err == nil {
		fmt.Printf("seed %d: no violation in %d faults over %d ms\n",
			seed, plan.Len(), c.set.until)
		return 0
	}

	small := plan
	if !c.set.noShrink {
		small = sim.Shrink(plan, func(p *sim.Plan) bool { return c.replay(seed, p) })
		// Rebuild from the small plan so the trace matches the schedule printed.
		// Shrink's last candidate is not necessarily the one it kept.
		err = c.check(c.build(seed, small))
		fmt.Printf("shrunk %d faults to %d\n", plan.Len(), small.Len())
	}

	c.report(seed, small, err)
	return 1
}

// sweep walks the seed range and stops at the first failure.
func sweep(c *checker) int {
	started := time.Now()
	fmt.Printf("sweeping seeds %d..%d, %d clients, lease %dms\n",
		c.set.from, c.set.to, c.set.clients, c.set.leaseMs)

	f := sim.Sweep(c.set.from, c.set.to, c.gen, c.build, c.check)
	if f == nil {
		fmt.Printf("clean: %d seeds in %s, no violation\n",
			c.set.to-c.set.from, time.Since(started).Round(time.Millisecond))
		return 0
	}

	fmt.Printf("failed at seed %d after %s\n",
		f.Seed, time.Since(started).Round(time.Millisecond))

	small := f.Plan
	err := f.Err
	if !c.set.noShrink {
		small = sim.Shrink(f.Plan, func(p *sim.Plan) bool { return c.replay(f.Seed, p) })
		err = c.check(c.build(f.Seed, small))
		fmt.Printf("shrunk %d faults to %d\n", f.Plan.Len(), small.Len())

		if err == nil {
			fmt.Fprintln(os.Stderr,
				"\nWARNING: the shrunk plan does not reproduce. The run is not "+
					"deterministic, which makes every result here untrustworthy.")
			c.report(f.Seed, f.Plan, f.Err)
			return 2
		}
	}

	c.report(f.Seed, small, err)
	fmt.Printf("\nreplay with: go run ./cmd/lockcheck -seed %d\n", f.Seed)
	return 1
}
