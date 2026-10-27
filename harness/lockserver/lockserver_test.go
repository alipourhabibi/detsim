package lockserver

import (
	"testing"

	"github.com/alipourhabibi/detsim/sim"
)

// a crashed client is not inside the critical section, even if the dead
// driver still says Holding
func TestCrashedClientIsNotAHolder(t *testing.T) {
	c := Build(sim.Config{Seed: 1, MaxEvents: 100_000}, 2, 100, 300)
	c.Sim.Start()
	for c.Sim.Now() < 4000 && len(c.Holders()) == 0 {
		if err := c.Sim.Step(1); err != nil {
			t.Fatal(err)
		}
	}
	h := c.Holders()
	if len(h) != 1 {
		t.Fatalf("no client got the lock, holders %v", h)
	}
	c.Sim.InjectFault(sim.NewCrashFault(h[0], false, 0))
	if err := c.Sim.Step(1); err != nil {
		t.Fatal(err)
	}
	if got := c.Holders(); len(got) != 0 {
		t.Fatalf("holders %v after n%d crashed", got, h[0])
	}
}
