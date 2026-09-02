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
	turn *sim.Turn
}

func (d *Driver) OnRestart(ctx *sim.Ctx) {
	d.turn.Enter(ctx)
	defer d.turn.Leave()
	d.node.Start()
}

func (d *Driver) OnTimer(ctx *sim.Ctx, name string) {
	d.turn.Enter(ctx)
	defer d.turn.Leave()
	d.node.OnTick(name)
}

func (d *Driver) OnMessage(ctx *sim.Ctx, from int, msg sim.Message) {
	d.turn.Enter(ctx)
	defer d.turn.Leave()
	d.node.OnMessage(from, msg)
}

func (d *Driver) Node() *maslave.Node {
	return d.node
}

type transport struct {
	turn *sim.Turn
}

func (t transport) Send(to int, msg any) {
	m, ok := msg.(sim.Message)
	if !ok {
		panic(fmt.Sprintf("maslavesim: %T is not a sim.Message", msg))
	}
	t.turn.Ctx().Send(to, m)
}

type clock struct {
	turn *sim.Turn
}

func (c clock) Now() int64 { return int64(c.turn.Ctx().Now()) }

func (c clock) SetTimer(name string, after int64) {
	c.turn.Ctx().SetTimer(name, sim.Duration(after))
}

func NewMasterSlavePinger(config sim.Config, masterNode int, slaveNodes []int) (*sim.Sim, map[int]*Driver) {
	drivers := make(map[int]*Driver, len(slaveNodes)+1)
	newNode := func(peers []int, role maslave.Role) sim.NodeFactory {
		return func(id int, deps sim.Deps) sim.Handler {
			tn := &sim.Turn{}
			drv := &Driver{
				turn: tn,
				node: maslave.New(id, peers, role, tickPeriod, transport{tn}, clock{tn}),
			}
			drivers[id] = drv
			return drv
		}
	}

	s := sim.New(
		config,
		append([]int{masterNode}, slaveNodes...),
		newNode(nil, maslave.Slave),
		sim.WithFactory(
			masterNode, newNode(slaveNodes, maslave.Master),
		),
	)
	s.Start()

	return s, drivers
}
