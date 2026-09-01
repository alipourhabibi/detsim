package maslave

import (
	"slices"
	"testing"

	"github.com/alipourhabibi/detsim/sim"
)

const (
	masterID = 1
	endTime  = 100_000
)

func slaveIDs() []int { return []int{2, 3, 4} }

func TestMasterPingerDeterminism(t *testing.T) {

	type result struct {
		hash          uint64
		events        uint64
		pingsSent     uint64
		pingsReceived uint64
		pongsReceived uint64
		round         uint64
		slavePings    []uint64
	}

	run := func(seed uint64) result {

		config := sim.Config{
			Seed:      seed,
			MaxEvents: 1_000_000,
			NetworkConfig: sim.NetworkConfig{
				MaxMsgDelay: 10,
			},
		}
		s, drivers := NewMasterSlavePinger(config, masterID, slaveIDs())
		if err := s.RunUntil(endTime); err != nil {
			t.Fatalf("seed %d: run failed: %v", seed, err)
		}

		m := drivers[masterID].Node()

		r := result{
			hash:          s.Hash(),
			events:        s.EventCount(),
			pingsSent:     m.PingsSent,
			pingsReceived: m.PingsReceived,
			pongsReceived: m.PongsReceived,
			round:         m.Round,
		}

		for _, sl := range slaveIDs() {
			r.slavePings = append(r.slavePings, drivers[sl].Node().PingsReceived)
		}
		return r
	}

	for _, seed := range []uint64{1, 10, 42} {
		a := run(seed)
		b := run(seed)

		if a.hash != b.hash {
			t.Errorf("seed %d: hash %x != %x", seed, a.hash, b.hash)
		}
		if a.events != b.events {
			t.Errorf("seed %d: event count %d != %d", seed, a.events, b.events)
		}
		if a.pingsSent != b.pingsSent ||
			a.pingsReceived != b.pingsReceived ||
			a.pongsReceived != b.pongsReceived ||
			a.round != b.round {
			t.Errorf("seed %d: master state differs\n run 1: %v\n run 2: %v\n", seed, a, b)
		}
		if !slices.Equal(a.slavePings, b.slavePings) {
			t.Errorf("seed %d: slave counts %v != %v", seed, a.slavePings, b.slavePings)
		}

		if want := a.round * uint64(len(slaveIDs())); a.pingsSent != want {
			t.Errorf("seed %d: pingsSent %d, want %d (rounds * slaves)", seed, a.pingsSent, want)
		}
		if a.pongsReceived > a.pingsSent {
			t.Errorf("seed %d: received %d pongs but only sent %d pings",
				seed, a.pongsReceived, a.pingsSent)
		}
		if gap := a.pingsSent - a.pongsReceived; gap > uint64(len(slaveIDs())) {
			t.Errorf("seed %d: %d pings unanswered, at most %d can be in flight",
				seed, gap, len(slaveIDs()))
		}
		if a.pingsReceived != 0 {
			t.Errorf("seed %d: master received %d pings, nobody pings the master",
				seed, a.pingsReceived)
		}

		t.Logf("seed %d: hash=%x events=%d rounds=%d sent=%d pongs=%d slaves=%v",
			seed, a.hash, a.events, a.round, a.pingsSent, a.pongsReceived, a.slavePings)
	}
}
