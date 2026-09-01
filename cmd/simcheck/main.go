package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/alipourhabibi/detsim/harness/pingpong"
	"github.com/alipourhabibi/detsim/sim"
)

// Usage: simcheck -seed=N
// Prints: <hash> <eventcount>
func main() {
	seed := flag.Uint64("seed", 1, "simulation seed")
	rounds := flag.Uint64("rounds", 200, "ping-pong rounds")
	flag.Parse()

	config := sim.Config{Seed: *seed, MaxEvents: 1_000_000}
	_, _, s := pingpong.NewPingPong(config, 0, 1, *rounds)

	if err := s.RunUntilQuiescent(); err != nil {
		fmt.Fprintf(os.Stderr, "run failed: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("%016x %d\n", s.Hash(), s.EventCount())
}
