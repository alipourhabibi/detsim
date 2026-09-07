package lockserver

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/alipourhabibi/detsim/protocol/lockserver"
	"github.com/alipourhabibi/detsim/sim"
)

type transport struct {
	turn *sim.Turn
}

func (t *transport) Send(to int, msg lockserver.Msg) {
	t.turn.Ctx().Send(to, msg)
}

type storage struct {
	turn *sim.Turn
}

func (s *storage) Get(key string) ([]byte, bool) {
	return s.turn.Ctx().Get(key)
}

func (s *storage) Put(key string, value []byte) {
	s.turn.Ctx().Put(key, value)
}

func (s *storage) Sync() {
	s.turn.Ctx().Sync()
}

type timers struct {
	turn *sim.Turn
}

func (t *timers) SetTimer(name string, afterMs int64) {
	t.turn.Ctx().SetTimer(name, sim.Duration(afterMs))
}

func (t *timers) CancelTimer(name string) {
	t.turn.Ctx().CancelTimer(name)
}

type ServerDriver struct {
	node *lockserver.Server
	turn *sim.Turn
}

func (d *ServerDriver) StateDigest(w io.Writer) {
	var buf [9]byte
	buf[0] = 's'
	binary.LittleEndian.PutUint64(buf[1:], uint64(int64(d.node.LastGranted)))
	w.Write(buf[:])
}

func (d *ServerDriver) StateString() string {
	return fmt.Sprintf("granted=%d", d.node.LastGranted)
}

func (d *ClientDriver) StateDigest(w io.Writer) {
	var buf [10]byte
	buf[0] = 'c'
	if d.node.Holding {
		buf[1] = 1
	}
	binary.LittleEndian.PutUint64(buf[2:], d.node.Attempt())
	w.Write(buf[:])
}

func (d *ClientDriver) StateString() string {
	if d.node.Holding {
		return fmt.Sprintf("HOLDING attempt=%d", d.node.Attempt())
	}
	return fmt.Sprintf("waiting attempt=%d", d.node.Attempt())
}

func (d *ServerDriver) Node() *lockserver.Server {
	return d.node
}

func (d *ServerDriver) OnRestart(ctx *sim.Ctx) {
	d.turn.Enter(ctx)
	defer d.turn.Leave()
	d.node.Start()
}

func (d *ServerDriver) OnTimer(ctx *sim.Ctx, name string) {
	d.turn.Enter(ctx)
	defer d.turn.Leave()
	// the server has no timers
}

func (d *ServerDriver) OnMessage(ctx *sim.Ctx, from int, msg sim.Message) {
	d.turn.Enter(ctx)
	defer d.turn.Leave()

	switch m := msg.(type) {
	case lockserver.Acquire:
		d.node.OnAcquire(from, m)
	case lockserver.Release:
		d.node.OnRelease(from)
	default:
		panic(fmt.Sprintf("lockserver: server got %T", msg))
	}
}

type ClientDriver struct {
	node *lockserver.Client
	turn *sim.Turn
}

func (d *ClientDriver) Node() *lockserver.Client {
	return d.node
}

func (d *ClientDriver) OnRestart(ctx *sim.Ctx) {
	d.turn.Enter(ctx)
	defer d.turn.Leave()
	d.node.Start()
}

func (d *ClientDriver) OnTimer(ctx *sim.Ctx, name string) {
	d.turn.Enter(ctx)
	defer d.turn.Leave()
	d.node.OnTimer(name)
}

func (d *ClientDriver) OnMessage(ctx *sim.Ctx, from int, msg sim.Message) {
	d.turn.Enter(ctx)
	defer d.turn.Leave()

	switch m := msg.(type) {
	case lockserver.Granted:
		d.node.OnGranted(m)
	default:
		panic(fmt.Sprintf("lockserver: client got %T", msg))
	}
}

type Cluster struct {
	Sim     *sim.Sim
	Server  int
	Clients []int

	server  *ServerDriver
	clients map[int]*ClientDriver
}

// Holders returns every client currently inside the critical section.
//
// This is the safety invariant of a lock service: the list never has more than
// one entry. If it does, mutual exclusion is broken, and whatever the lock was
// protecting has two writers.
func (c *Cluster) Holders() []int {
	var out []int
	for _, id := range c.Clients { // sorted list, not a map: order must be stable
		d := c.clients[id]
		if d != nil && d.node.Holding {
			out = append(out, id)
		}
	}
	return out
}

// ServerOwner reads the durable owner record straight from the server's disk.
func (c *Cluster) ServerOwner() int {
	v, ok := c.Sim.Get(c.Server, "lock.owner")
	if !ok {
		return lockserver.NoHolder
	}
	return int(int64(binary.LittleEndian.Uint64(v)))
}

// Build makes a cluster: node 0 is the server, the rest are clients.
func Build(cfg sim.Config, clientCount int, retryMs, holdMs int64) *Cluster {
	const serverID = 0

	ids := []int{serverID}
	var clientIDs []int
	for i := 1; i <= clientCount; i++ {
		ids = append(ids, i)
		clientIDs = append(clientIDs, i)
	}

	c := &Cluster{
		Server:  serverID,
		Clients: clientIDs,
		clients: map[int]*ClientDriver{},
	}

	serverFactory := func(id int, deps sim.Deps) sim.Handler {
		turn := &sim.Turn{}
		d := &ServerDriver{
			node: lockserver.NewServer(id, &transport{turn}, &storage{turn}),
			turn: turn,
		}
		c.server = d
		return d
	}

	clientFactory := func(id int, deps sim.Deps) sim.Handler {
		turn := &sim.Turn{}
		d := &ClientDriver{
			node: lockserver.NewClient(id, serverID, retryMs, holdMs,
				&transport{turn}, &timers{turn}),
			turn: turn,
		}
		c.clients[id] = d
		return d
	}

	opts := []sim.Option{sim.WithFactory(serverID, serverFactory)}
	c.Sim = sim.New(cfg, ids, clientFactory, opts...)
	return c
}
