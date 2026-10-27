package pingpong

import "testing"

type fakeNet struct{ n int }

func (f *fakeNet) Send(int, Ball) { f.n++ }

// Equal must agree with HashInto, which includes IsPing
func TestBallEqualChecksIsPing(t *testing.T) {
	if (Ball{Round: 3, IsPing: true}).Equal(Ball{Round: 3, IsPing: false}) {
		t.Fatal("ping and pong of the same round are Equal")
	}
}

// a node answers Count pings, not Count+1
func TestPingDeliverStopsAtCount(t *testing.T) {
	net := &fakeNet{}
	p := New(1, 0, 3, 0, net, false, 50)
	for range 10 {
		p.PingDeliver(0, Ball{IsPing: true})
	}
	if p.Rounds != 3 || net.n != 3 {
		t.Fatalf("rounds=%d pongs=%d, want 3 and 3", p.Rounds, net.n)
	}
}
