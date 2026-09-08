package lockserver

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/alipourhabibi/detsim/protocol/lockserver"
	"github.com/alipourhabibi/detsim/sim"
)

const (
	opAcquire = "acquire"
	opRelease = "release"
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
	node    *lockserver.Client
	turn    *sim.Turn
	history *sim.History
}

func (d *ClientDriver) Node() *lockserver.Client {
	return d.node
}

func (d *ClientDriver) OnRestart(ctx *sim.Ctx) {
	d.turn.Enter(ctx)
	defer d.turn.Leave()

	d.node.Start()
	d.history.Invoke(ctx.Now(), d.node.Id, opAcquire, d.node.Attempt())
}

func (d *ClientDriver) OnTimer(ctx *sim.Ctx, name string) {
	d.turn.Enter(ctx)
	defer d.turn.Leave()

	prev := d.node.Attempt()

	d.node.OnTimer(name)

	switch name {
	case "retry":
		// The protocol gave up on prev and sent a new one.
		d.history.Abandon(d.node.Id, prev)
		d.history.Invoke(ctx.Now(), d.node.Id, opAcquire, d.node.Attempt())

	case "lease":
		// The lease ran out. The protocol sent a Release and schedule retry.
		// No reply is coming, so it is Unknown from the moment it is sent.
		d.history.Invoke(ctx.Now(), d.node.Id, opRelease, prev)

		// The protocol ignores answers to old attempts, in OnGranted. This does the
		// same, so both agree on which answers count.
		d.history.Abandon(d.node.Id, prev)
	}
}

func (d *ClientDriver) OnMessage(ctx *sim.Ctx, from int, msg sim.Message) {
	d.turn.Enter(ctx)
	defer d.turn.Leave()

	m, ok := msg.(lockserver.Granted)
	if !ok {
		panic(fmt.Sprintf("lockserver: client got %T", msg))
	}

	d.history.Complete(ctx.Now(), d.node.Id, m.Attempt)

	d.node.OnGranted(m)
}

type Cluster struct {
	Sim     *sim.Sim
	Server  int
	Clients []int

	server  *ServerDriver
	clients map[int]*ClientDriver

	leaseMs int64 // needed by CheckHistory to tell a leak from a truncated run
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
		leaseMs: holdMs,
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
			turn:    turn,
			history: c.Sim.History(),
		}
		c.clients[id] = d
		return d
	}

	opts := []sim.Option{sim.WithFactory(serverID, serverFactory)}
	c.Sim = sim.New(cfg, ids, clientFactory, opts...)

	c.Sim.AddInvariant(c.checkMutualExclusion)

	return c
}

func (c *Cluster) checkMutualExclusion(*sim.Sim) error {
	h := c.Holders()
	if len(h) <= 1 {
		return nil
	}
	return fmt.Errorf("clients %v are all inside the critical section, "+
		"durable owner record says %d", h, c.ServerOwner())
}

func (c *Cluster) CheckHistory() error {
	now := c.Sim.Now()
	spans := c.Sim.History().Spans(opAcquire, opRelease, now)

	if a, b, found := sim.Overlapping(spans); found {
		return fmt.Errorf("client %d held the lock from t=%d (op%d) while "+
			"client %d was granted it at t=%d (op%d)",
			a.Client, a.From, a.Op.ID, b.Client, b.From, b.Op.ID)
	}

	// A grant is a leak only if the client had time to release and did not
	cutoff := now - sim.Time(c.leaseMs)

	for _, s := range sim.Unclosed(spans, now) {
		if s.From > cutoff {
			continue // OK as still within its lease when the run ended
		}
		if c.Sim.Status(s.Client) != sim.Crashed {
			return fmt.Errorf("client %d was granted the lock at t=%d (op%d) "+
				"and never gave it back", s.Client, s.From, s.Op.ID)
		}
	}
	return nil
}

// CheckLiveness clears every fault, gives the cluster time, and asks whether
// the lock service still works.
func (c *Cluster) CheckLiveness(budget sim.Duration) error {
	before := c.Sim.Now()

	if err := c.Sim.RunHealed(budget); err != nil {
		return err
	}

	// Was the lock granted to anyone after healing?
	for _, op := range c.Sim.History().Ops() {
		if op.Kind == opAcquire && op.Outcome == sim.Ok && op.Returned > before {
			return nil
		}
	}

	return fmt.Errorf("no client was granted the lock in the %d after healing; "+
		"the service is stuck", budget)
}
