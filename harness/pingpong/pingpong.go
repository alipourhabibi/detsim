package pingpong

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/alipourhabibi/detsim/protocol/pingpong"
	"github.com/alipourhabibi/detsim/sim"
)

const (
	Interval = 50
)

type transport struct {
	turn *sim.Turn // shared with driver turn
}

func (t *transport) Send(to int, msg pingpong.Ball) {
	t.turn.Ctx().Send(to, msg)
}

type driver struct {
	node *pingpong.PingPong
	turn *sim.Turn
}

func (d *driver) StateDigest(_ *sim.Ctx, w io.Writer) {
	var buf [17]byte
	buf[0] = 'p'
	binary.LittleEndian.PutUint64(buf[1:9], d.node.Rounds) // Count is config, Rounds is state
	binary.LittleEndian.PutUint64(buf[9:17], d.node.Ticks)
	w.Write(buf[:])
}

func (d *driver) OnRestart(ctx *sim.Ctx) {
	d.turn.Enter(ctx)
	defer d.turn.Leave()
	ctx.SetTimer("tick", sim.Duration(d.node.Interval))
	d.node.Start()
}

func (d *driver) OnTimer(ctx *sim.Ctx, name string) {
	d.turn.Enter(ctx)
	defer d.turn.Leave()
	if d.node.OnTick() {
		ctx.SetTimer(name, sim.Duration(d.node.Interval))
	}
}

func (d *driver) OnMessage(ctx *sim.Ctx, from int, msg sim.Message) {
	d.turn.Enter(ctx)
	defer d.turn.Leave()

	ballMsg, ok := msg.(pingpong.Ball)
	if !ok {
		panic(fmt.Sprintf("harness: got %T, want pingpong.Ball", msg))
	}

	if ballMsg.IsPing {
		d.node.PingDeliver(from, ballMsg)
	} else {
		d.node.PongDeliver(from, ballMsg)
	}
}

// Node returns the protocol state of node id. Call it again after a restart,
// the old one is gone.
func Node(s *sim.Sim, id int) *pingpong.PingPong {
	return s.Handler(id).(*driver).node
}

// NewPingPong builds the sim but does not start it, so the caller can add
// invariants or use StartWith first.
func NewPingPong(config sim.Config, a, b int, rounds uint64) *sim.Sim {
	newNode := func(peer int, rounds uint64, starter bool) sim.NodeFactory {
		return func(id int) sim.Handler {
			tn := &sim.Turn{}
			return &driver{
				node: pingpong.New(id, peer, rounds, 2*rounds, &transport{tn}, starter, Interval),
				turn: tn,
			}
		}
	}

	return sim.New(config, []int{a, b}, nil,
		sim.WithFactory(a, newNode(b, rounds, true)),
		sim.WithFactory(b, newNode(a, rounds, false)),
	)
}
