package sim

import (
	"container/heap"
	"errors"
	"fmt"
	"slices"
)

var ErrMaxEvents = errors.New("reached max events")

type Config struct {
	Seed      uint64
	MaxEvents uint64 // hard cap; guards runaway zero-delay loops

	TraceLevel Level
	TraceKeep  int // 0 none, KeepAll everything, N last N

	NetworkConfig NetworkConfig
}

// Funcs and interfaces. Supplied by the caller, never serialized.
type Wiring struct {
	Factory NodeFactory
}

type Sim struct {
	now     Time
	cfg     Config
	seq     uint64
	current uint64 // Seq of the event being executed, for Parent linking
	events  uint64
	wiring  *Wiring
	started bool

	queue   eventQueue
	streams *streams
	trace   *Trace
	network *Network
	effects Effects

	nodes map[int]*Node
	index map[int]int // node id -> dense index, for per-node streams
	order []int       // id in order

	lastRule RuleID
}

// Create a simulator with default network and nodes config
func New(cfg Config, w *Wiring, nodes []int) *Sim {

	order := slices.Clone(nodes)
	slices.Sort(order)

	index := make(map[int]int, len(order))
	nodeMap := make(map[int]*Node, len(order))
	for i, id := range order {
		if _, dup := index[id]; dup {
			panic(fmt.Sprintf("sim: duplicate node id %d", id))
		}
		index[id] = i
		nodeMap[id] = newNode(id, w.Factory)
	}

	return &Sim{
		cfg:     cfg,
		wiring:  w,
		streams: newStreams(cfg.Seed, len(order)),
		trace:   NewTrace(cfg.TraceLevel, cfg.TraceKeep),
		network: newNetwork(cfg.NetworkConfig, order),
		nodes:   nodeMap,
		order:   order,
		index:   index,
	}
}

// Register overrides the default factory for one node. Must be called before
// Start. Panics if id is unknown or Start has already run.
func (s *Sim) Register(id int, f NodeFactory) {
	if s.started {
		panic("sim: Register after Start")
	}
	s.node(id).factory = f
}

// Start builds every handler in sorted id order, then calls OnRestart on each
// so nodes can arm their initial timers.
//
// First boot and restart are the same situation: durable state exists, volatile
// state doesn't. One callback covers both, which keeps the restart path from
// being the undertested one.
func (s *Sim) Start() {
	if s.started {
		panic("sim: Start called twice")
	}
	s.started = true

	for _, id := range s.order {
		s.node(id).build(s.deps(id))
	}
	for _, id := range s.order {
		n := s.node(id)
		s.effects.reset()
		ctx := &Ctx{sim: s, self: id, out: &s.effects, gen: s.events}
		n.handler.OnRestart(ctx)
		s.drain(id)
	}
}

func (s *Sim) ScheduleFault(at Time, f Fault) uint64 {
	return s.push(Event{At: at, Kind: EvFault, Source: -1, Target: -1, Payload: f})
}

func (s *Sim) InjectFault(f Fault) uint64 {
	return s.ScheduleFault(s.now, f)
}

func (s *Sim) pushDeliver(from, to int, at Time, msg Message) uint64 {
	return s.push(Event{
		At: at, Kind: EvDeliver, Source: from, Target: to,
		Epoch: s.node(to).epoch, Payload: msg,
	})
}

func (s *Sim) pushTimer(node int, name string, at Time, token uint64) uint64 {
	return s.push(Event{
		At: at, Kind: EvTimer, Source: -1, Target: node,
		Epoch: s.node(node).epoch, Token: token, Name: name,
	})
}

func (s *Sim) pushRestart(node int, at Time) uint64 {
	return s.push(Event{
		At: at, Kind: EvRestart, Source: -1, Target: node,
		Epoch: s.node(node).epoch,
	})
}

// Run modes.
func (s *Sim) RunUntil(t Time) error {
	for {
		next, ok := s.queue.Peek()
		if !ok || next.At > t {
			s.now = t // time passed even though nothing happened
			return nil
		}

		_, err := s.run()
		if err != nil {
			return err
		}
	}
}

func (s *Sim) Step(n int) error {
	for range n {
		ran, err := s.run()
		if err != nil {
			return err
		}
		if !ran {
			return nil // ran out early; not an error
		}
	}
	return nil
}

func (s *Sim) RunUntilQuiescent() error {
	for {
		ran, err := s.run()
		if err != nil {
			return err
		}
		if !ran {
			return nil
		}
	}
}

// RunHealed is the liveness harness: remove every fault, bring everyone up,
// then run for a bounded stretch. Liveness is a run mode, not a predicate
// that's why it isn't expressed as an invariant.
func (s *Sim) RunHealed(budget Time) error {
	s.network.healAll()
	for _, id := range s.order {
		n := s.node(id)
		switch n.status {
		case Crashed:
			s.pushRestart(id, s.now)
		case Paused:
			resumeFault{Node: id}.Apply(s)
		}
	}
	return s.RunUntil(s.now + budget)
}

func (s *Sim) Now() Time {
	return s.now
}

func (s *Sim) Hash() uint64 {
	return s.trace.Sum()
}

func (s *Sim) EventCount() uint64 {
	return s.events
}

func (s *Sim) Trace() *Trace {
	return s.trace
}

func (s *Sim) Nodes() []int {
	return s.order
}

func (s *Sim) Up(id int) bool {
	return s.node(id).status == Healthy
}

func (s *Sim) node(id int) *Node {
	n, ok := s.nodes[id]
	if !ok {
		panic(fmt.Sprintf("sim: node %d does not exists in sim", id))
	}
	return n
}

func (s *Sim) idx(id int) int {
	i, ok := s.index[id]
	if !ok {
		panic(fmt.Sprintf("sim: unknown node %d", id))
	}
	return i
}

// deps is rebuilt on every restart, never cached: reseedNode replaces the
// stream pointer, so a stale Deps would hand out a dead RNG.
func (s *Sim) deps(id int) Deps {
	return Deps{Id: id, Store: s.node(id).storage, Rand: s.nodeRand(id)}
}

func (s *Sim) nodeRand(id int) Rand {
	return s.streams.nodeRand(s.idx(id))
}

func (s *Sim) nodeNow(id int) Time {
	return s.node(id).clock.now(s.now)
}

// push is the ONLY function that inserts into the heap. It stamps Seq and
// Parent; everything kind-specific is stamped by the typed helpers below,
// because the stamping rules differ per kind and a generic entry point either
// forgets them or grows a switch that reimplements the helpers anyway.
func (s *Sim) push(e Event) uint64 {
	if e.At < s.now {
		panic(fmt.Sprintf("sim: schedule at %d before now %d (kind=%v)", e.At, s.now, e.Kind))
	}
	s.seq++
	e.Seq = s.seq
	e.Parent = s.current
	heap.Push(&s.queue, e)
	return e.Seq
}

// requeue re-inserts a deferred event at the resume time.
//
// It takes a FRESH Seq. Order among the backlog was originally (At, Seq),
// but resume collapses every At to now, so the old Seq alone would reorder
// them. The deferred slice is already in correct pop order, so re-pushing in
// slice order with new increasing Seqs is what actually preserves it.
func (s *Sim) requeue(e Event) {
	e.At = s.now
	s.seq++
	e.Seq = s.seq
	e.Parent = s.current
	heap.Push(&s.queue, e)
}

// run - runs one step
// return false if does nothing
func (s *Sim) run() (bool, error) {
	if s.queue.Len() == 0 {
		return false, nil
	}

	if s.cfg.MaxEvents > 0 && s.events >= s.cfg.MaxEvents {
		return false, ErrMaxEvents
	}

	evt := heap.Pop(&s.queue).(Event)
	if evt.At < s.now {
		panic(fmt.Sprintf("sim: popped event at: %d is before now: %d", evt.At, s.now))
	}
	s.events++
	s.now = evt.At
	s.current = evt.Seq
	defer func() {
		s.current = 0
	}()

	// Harness level, never reaches handler
	if evt.Kind == EvFault {
		s.trace.Record(evt)
		evt.Payload.(Fault).Apply(s)
		return true, nil
	}

	n := s.node(evt.Target)

	// reasons event won't run
	switch {
	case evt.Epoch != n.epoch:
		// Scheduled for a node generation that no longer exists.
		s.trace.Dropped(evt, "stale epoch")
		return true, nil

	case evt.Kind == EvTimer && evt.Token != n.timers[evt.Name]:
		s.trace.Dropped(evt, "timer superseded")
		return true, nil

	case evt.Kind == EvDeliver && !s.network.reachable(evt.Source, evt.Target, s.now):
		// Partition formed while this was on the wire.
		s.trace.Dropped(evt, "partitioned in flight")
		return true, nil

	case n.status == Crashed && evt.Kind != EvRestart:
		// Sent during downtime, so it carries the current epoch and passed
		// above. Both checks are needed: epoch catches sent-before-crash,
		// this catches sent-while-down. EvRestart is exempt, it is the thing
		// that ends the downtime.
		s.trace.Dropped(evt, "node down")
		return true, nil

	case n.status == Paused:
		n.deferred = append(n.deferred, evt)
		s.trace.Deferred(evt)
		return true, nil
	}

	s.trace.Record(evt)

	// dispatch
	s.effects.reset()

	ctx := &Ctx{
		sim:  s,
		self: evt.Target,
		out:  &s.effects,
		gen:  s.events,
	}

	switch evt.Kind {
	case EvRestart:
		n.restart(s.deps(evt.Target))
		s.streams.reseedNode(s.idx(evt.Target), n.epoch)
		s.trace.Note(EnRestart, s.now, evt.Target, "")
		n.handler.OnRestart(ctx)
	case EvDeliver:
		n.handler.OnMessage(ctx, evt.Source, evt.Payload.(Message))
	case EvTimer:
		n.handler.OnTimer(ctx, evt.Name)
	default:
		panic(fmt.Sprintf("sim: unroutable event kind: %v", evt.Kind))
	}

	s.drain(evt.Target)
	s.foldState()
	return true, nil

}

var drainOrder = [...]EffectKind{EfPut, EfSync, EfCancelTimer, EfSetTimer, EfSend}

// drain is where the actual things happen in the node
func (s *Sim) drain(node int) {
	for _, kind := range drainOrder {
		for i := range s.effects.buf {
			if s.effects.buf[i].Kind == kind {
				s.apply(node, &s.effects.buf[i])
			}
		}
	}
}

func (s *Sim) apply(node int, ef *Effect) {
	n := s.node(node)
	switch ef.Kind {
	case EfPut:
		n.storage.Put(ef.Key, ef.Value)
		s.trace.Note(EnNote, s.now, node, "save "+ef.Key)
	case EfSync:
		n.storage.Sync()
	case EfCancelTimer:
		n.bumpTimer(ef.Name) // bumping the token invalidates the pending fire
	case EfSetTimer:
		// Bump first, so an earlier fire for this name goes stale. That makes
		// SetTimer reset rather than stack, which is what Raft wants from
		// every AppendEntries.
		token := n.bumpTimer(ef.Name)
		s.pushTimer(node, ef.Name, s.now+ef.After, token)
	case EfSend:
		s.send(node, ef.To, ef.Msg)
	default:
		panic(fmt.Sprintf("sim: unknown effect kind %v", ef.Kind))
	}
}

func (s *Sim) send(from, to int, payload Message) {
	if from == to {
		panic("sim: can not send to itself")
	}

	s.trace.Sent(s.now, s.current, from, to, payload)

	l := s.network.link(from, to)

	loss := s.streams.loss.Int64N(1_000_000) < int64(l.lossPPM)
	dup := s.streams.dup.Int64N(1_000_000) < int64(l.dupPPM)

	base := Event{At: s.now, Kind: EvDeliver, Source: from, Target: to, Payload: payload}

	if !s.network.reachable(from, to, s.now) {
		s.trace.Dropped(base, "unreachable at send")
		return
	}

	if loss {
		s.trace.Dropped(base, "loss")
		return
	}
	s.scheduleDeliver(from, to, payload)
	if dup {
		s.scheduleDeliver(from, to, payload)
		s.trace.Note(EnNote, s.now, to, "duplicated")
	}

}

func (s *Sim) scheduleDeliver(from, to int, payload Message) {
	link := s.network.link(from, to)
	at := s.now + link.delay.Sample(s.streams.delay)

	// In FIFO the ordering is preserved
	if !link.reordering {
		at = max(at, link.lastArrival+1)
		link.lastArrival = at
	}

	s.pushDeliver(from, to, at, payload) // stamps dest epoch
}

// foldState mixes node state into the hash, catching two runs with an
// identical event sequence but divergent state. Expensive: off by default.
func (s *Sim) foldState() {
	if s.cfg.TraceLevel < TraceHashEventsAndState {
		return
	}
	for _, id := range s.order { // sorted, not map order
		if d, ok := s.node(id).handler.(StateDigester); ok {
			s.trace.FoldState(id, d)
		}
	}
}
