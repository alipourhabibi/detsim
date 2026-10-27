package maslave

import (
	"encoding/binary"
	"fmt"
	"io"

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

func (d *Driver) StateDigest(_ *sim.Ctx, w io.Writer) {
	var buf [33]byte
	buf[0] = 'm'
	binary.LittleEndian.PutUint64(buf[1:], d.node.Round)
	binary.LittleEndian.PutUint64(buf[9:], d.node.PingsSent)
	binary.LittleEndian.PutUint64(buf[17:], d.node.PingsReceived)
	binary.LittleEndian.PutUint64(buf[25:], d.node.PongsReceived)
	w.Write(buf[:])
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

// Node returns the protocol state of node id. Call it again after a restart,
// the old one is gone.
func Node(s *sim.Sim, id int) *maslave.Node {
	return s.Handler(id).(*Driver).node
}

// NewMasterSlavePinger builds the sim but does not start it, so the caller
// can add invariants or use StartWith first.
func NewMasterSlavePinger(config sim.Config, masterNode int, slaveNodes []int) *sim.Sim {
	newNode := func(peers []int, role maslave.Role) sim.NodeFactory {
		return func(id int) sim.Handler {
			tn := &sim.Turn{}
			return &Driver{
				turn: tn,
				node: maslave.New(id, peers, role, tickPeriod, transport{tn}, clock{tn}),
			}
		}
	}

	return sim.New(
		config,
		append([]int{masterNode}, slaveNodes...),
		newNode(nil, maslave.Slave),
		sim.WithFactory(
			masterNode, newNode(slaveNodes, maslave.Master),
		),
	)
}
