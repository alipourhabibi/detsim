package pingpong

import (
	"encoding/binary"
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

func (d *driver) StateDigest(w io.Writer) {
	var buf [17]byte
	buf[0] = 'p'
	binary.LittleEndian.PutUint64(buf[1:9], d.node.Count)
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
		panic("harness: msg is not of type sim.Message")
	}

	if ballMsg.IsPing {
		d.node.PingDeliver(from, ballMsg)
	} else {
		d.node.PongDeliver(from, ballMsg)
	}
}

func (d *driver) Node() *pingpong.PingPong {
	return d.node
}

func NewPingPong(config sim.Config, a, b int, rounds uint64) (*driver, *driver, *sim.Sim) {
	drivers := map[int]*driver{}
	newNode := func(peer int, rounds uint64, starter bool) sim.NodeFactory {
		return func(id int, deps sim.Deps) sim.Handler {
			tn := &sim.Turn{}
			nd := &driver{
				node: pingpong.New(id, peer, rounds, 2*rounds, &transport{tn}, starter, Interval),
				turn: tn,
			}
			drivers[id] = nd
			return nd
		}
	}

	s := sim.New(config, []int{a, b}, nil,
		sim.WithFactory(a, newNode(b, rounds, true)),
		sim.WithFactory(b, newNode(a, rounds, false)),
	)
	s.Start()

	return drivers[a], drivers[b], s
}
