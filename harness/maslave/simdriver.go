package maslave

import (
	"fmt"

	"github.com/alipourhabibi/detsim/protocol/maslave"
	"github.com/alipourhabibi/detsim/sim"
)

const (
	tickPeriod = 100
)

type Driver struct {
	node *maslave.Node
	turn *turn
}

func (d *Driver) OnRestart(ctx *sim.Ctx) {
	d.turn.enter(ctx)
	defer d.turn.leave()
	d.node.Start()
}

func (d *Driver) OnTimer(ctx *sim.Ctx, name string) {
	d.turn.enter(ctx)
	defer d.turn.leave()
	d.node.OnTick(name)
}

func (d *Driver) OnMessage(ctx *sim.Ctx, from int, msg sim.Message) {
	d.turn.enter(ctx)
	defer d.turn.leave()
	d.node.OnMessage(from, msg)
}

func (d *Driver) Node() *maslave.Node {
	return d.node
}

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
	turn *turn
}

func (t transport) Send(to int, msg any) {
	m, ok := msg.(sim.Message)
	if !ok {
		panic(fmt.Sprintf("maslavesim: %T is not a sim.Message", msg))
	}
	t.turn.get().Send(to, m)
}

type clock struct {
	turn *turn
}

func (c clock) Now() int64 { return int64(c.turn.get().Now()) }

func (c clock) SetTimer(name string, after int64) {
	c.turn.get().SetTimer(name, sim.Time(after))
}

func NewMasterSlavePinger(config sim.Config, masterNode int, slaveNodes []int) (*sim.Sim, map[int]*Driver) {
	drivers := make(map[int]*Driver, len(slaveNodes)+1)
	newNode := func(peers []int, role maslave.Role) sim.NodeFactory {
		return func(id int, deps sim.Deps) sim.Handler {
			tn := &turn{}
			drv := &Driver{
				turn: tn,
				node: maslave.New(id, peers, role, tickPeriod, transport{tn}, clock{tn}),
			}
			drivers[id] = drv
			return drv
		}
	}

	s := sim.New(config, &sim.Wiring{
		Factory: newNode(nil, maslave.Slave),
	}, append([]int{masterNode}, slaveNodes...))
	s.Register(masterNode, newNode(slaveNodes, maslave.Master))
	s.Start()

	return s, drivers
}
