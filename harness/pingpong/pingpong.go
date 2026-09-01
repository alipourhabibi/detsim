package pingpong

import (
	"github.com/alipourhabibi/detsim/protocol/pingpong"
	"github.com/alipourhabibi/detsim/sim"
)

const (
	Interval = 50
)

type turn struct {
	ctx *sim.Ctx
}

func (t *turn) enter(ctx *sim.Ctx) {
	t.ctx = ctx
}

func (t *turn) leave() {
	t.ctx = nil
}

func (t *turn) get() *sim.Ctx {
	if t.ctx == nil {
		panic("harness: protocol acted outside a callback")
	}
	return t.ctx
}

type transport struct {
	turn *turn // shared with driver turn
}

func (t *transport) Send(to int, msg pingpong.Ball) {
	t.turn.get().Send(to, msg)
}

type driver struct {
	node *pingpong.PingPong
	turn *turn
}

func (d *driver) OnRestart(ctx *sim.Ctx) {
	d.turn.enter(ctx)
	defer d.turn.leave()
	ctx.SetTimer("tick", sim.Time(d.node.Interval))
	d.node.Start()
}

func (d *driver) OnTimer(ctx *sim.Ctx, name string) {
	d.turn.enter(ctx)
	defer d.turn.leave()
	if d.node.OnTick() {
		ctx.SetTimer(name, sim.Time(d.node.Interval))
	}
}

func (d *driver) OnMessage(ctx *sim.Ctx, from int, msg sim.Message) {
	d.turn.enter(ctx)
	defer d.turn.leave()

	ballMsg, ok := msg.(pingpong.Ball)
	if !ok {
		panic("harness; msg is not of type sim.Message")
	}

	if ballMsg.IsPing {
		d.node.PingDevlier(from, ballMsg)
	} else {
		d.node.PongDevlier(from, ballMsg)
	}
}

func (d *driver) Node() *pingpong.PingPong {
	return d.node
}

func NewPingPong(config sim.Config, a, b int, rounds uint64) (*driver, *driver, *sim.Sim) {
	drivers := map[int]*driver{}
	newNode := func(peer int, rounds uint64, starter bool) sim.NodeFactory {
		return func(id int, deps sim.Deps) sim.Handler {
			tn := &turn{}
			nd := &driver{
				node: pingpong.New(id, peer, rounds, 2*rounds, &transport{tn}, starter, Interval),
				turn: tn,
			}
			drivers[id] = nd
			return nd
		}
	}

	s := sim.New(config, &sim.Wiring{}, []int{a, b})
	s.Register(a, newNode(b, rounds, true))
	s.Register(b, newNode(a, rounds, false))
	s.Start()

	return drivers[a], drivers[b], s
}
