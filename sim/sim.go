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
	NewStore      func(id int) Store
}

func (s *Sim) requireDigester(id int) {
	if s.cfg.TraceLevel < TraceHashEventsAndState {
		return
	}
	h := s.node(id).handler
	if _, ok := h.(StateDigester); !ok {
		panic(fmt.Sprintf("sim: node %d handler %T does not implement "+
			"StateDigester, required at TraceLevel %v", id, h, s.cfg.TraceLevel))
	}
}

type Sim struct {
	now     Time
	cfg     Config
	seq     uint64
	current uint64 // Seq of the event being executed, for Parent linking
	events  uint64
	started bool

	queue   eventQueue
	streams *streams

	trace   *Trace
	network *Network
	effects Effects

	nodes map[int]*node
	index map[int]nodeIdx // node id -> dense index, for per-node streams
	order []int           // id in order

	lastRule RuleID
}

type Option func(*Sim)

// overrides the default factory for one node.
func WithFactory(id int, f NodeFactory) Option {
	return func(s *Sim) {
		if f == nil {
			panic(fmt.Sprintf("sim: WithFactory(%d): nil factory", id))
		}
		s.node(id).factory = f
	}
}

// Create a simulator with default network and nodes config
func New(cfg Config, nodes []int, factory NodeFactory, opts ...Option) *Sim {

	if len(nodes) == 0 {
		panic("sim: no nodes")
	}

	if cfg.TraceKeep < KeepAll {
		panic(fmt.Sprintf("sim: TraceKeep must be >= %d (KeepAll), got %d", KeepAll, cfg.TraceKeep))
	}

	order := slices.Clone(nodes)
	slices.Sort(order)

	newStore := cfg.NewStore
	if newStore == nil {
		newStore = func(int) Store {
			return NewMemStore()
		}
	}

	index := make(map[int]nodeIdx, len(order))
	nodeMap := make(map[int]*node, len(order))

	for i, id := range order {

		if id < 0 {
			panic(fmt.Sprintf("sim: node id %d is negative; -1 is reserved as the no-node sentinel", id))
		}

		if i > maxNodeIndex {
			panic(fmt.Sprintf("sim: %d nodes exceeds the limit of %d", len(order), maxNodeIndex+1))
		}

		if _, dup := index[id]; dup {
			panic(fmt.Sprintf("sim: duplicate node id %d", id))
		}

		st := newStore(id)
		if st == nil {
			panic(fmt.Sprintf("sim: NewStore returned nil for node %d", id))
		}

		index[id] = nodeIdx(i)
		nodeMap[id] = newNode(id, factory, st)
	}

	s := &Sim{
		cfg:     cfg,
		streams: newStreams(cfg.Seed, len(order)),
		trace:   NewTrace(cfg.TraceLevel, cfg.TraceKeep),
		network: newNetwork(cfg.NetworkConfig, order, index),
		nodes:   nodeMap,
		order:   order,
		index:   index,
	}

	for _, o := range opts {
		o(s)
	}

	for _, id := range s.order {
		if s.node(id).factory == nil {
			panic(fmt.Sprintf("sim: node %d has no factory: pass one to New or use WithFactory", id))
		}
	}

	return s

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
		s.requireDigester(id)
	}
	for _, id := range s.order {
		n := s.node(id)
		s.effects.reset()
		ctx := &Ctx{sim: s, self: id, out: &s.effects, gen: s.events}
		n.handler.OnRestart(ctx)
		s.drain(id)
	}

	s.reportStates()
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

// RunHealed is the liveness harness: clear every fault, bring everyone up, run
// for a bounded stretch. Liveness has no instant to point at, so it is a run
// mode rather than a predicate.
func (s *Sim) RunHealed(budget Duration) error {
	// Scheduled, not applied: repairs belong in the trace and the hash like any
	// other fault.
	s.ScheduleFault(s.now, NewHealFault(0)) // rule 0: heal everything

	for _, id := range s.order {
		switch s.node(id).status {
		case Crashed:
			s.ScheduleFault(s.now, NewRestartFault(id))
		case Paused:
			s.ScheduleFault(s.now, NewResumeFault(id))
		}
	}

	return s.RunUntil(s.now.Add(budget))
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

// Partition splits the cluster immediately and returns the RuleID for healing it.
func (s *Sim) Partition(a, b []int) RuleID {
	id := s.network.partition(a, b)
	s.trace.Note(EnPartition, s.now, -1, fmt.Sprintf("partition %v|%v rule=%d", a, b, id), s.current)
	return id
}

// Heal removes one rule. Heal(0) removes everything.
func (s *Sim) Heal(id RuleID) {
	s.network.heal(id)
	s.trace.Note(EnHealed, s.now, -1, fmt.Sprintf("heal rule=%d", id), s.seq)
}

// Handler returns a node's handler for assertions. nil while the node is
// crashed. Read only: calling into it outside a callback has no Ctx.
func (s *Sim) Handler(id int) Handler {
	return s.node(id).handler
}

// Get reads a node's durable store directly, bypassing the handler. For
// assertions about what survived a crash.
func (s *Sim) Get(id int, key string) ([]byte, bool) {
	return s.node(id).storage.Get(key)
}

func (s *Sim) Keys(id int) []string {
	return s.node(id).storage.Keys()
}

// Status reports whether a node is healthy, crashed or paused. Up() only
// distinguishes healthy from everything else.
func (s *Sim) Status(id int) Status {
	return s.node(id).status
}

func (s *Sim) node(id int) *node {
	n, ok := s.nodes[id]
	if !ok {
		panic(fmt.Sprintf("sim: node %d does not exist in sim", id))
	}
	return n
}

func (s *Sim) idx(id int) nodeIdx {
	i, ok := s.index[id]
	if !ok {
		panic(fmt.Sprintf("sim: unknown node %d", id))
	}
	return nodeIdx(i)
}

// deps is rebuilt on every restart, never cached: reseedNode replaces the
// stream pointer, so a stale Deps would hand out a dead RNG.
func (s *Sim) deps(id int) Deps {
	return Deps{Id: id, Store: s.node(id).storage, Rand: s.nodeRand(id)}
}

func (s *Sim) nodeRand(id int) Rand {
	return s.streams.at(s.idx(id))
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
		s.foldState()
		s.reportStates() // a crash is exactly when beliefs change
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
		s.trace.Dropped(evt, "node down")
		return true, nil

	case n.status == Paused:
		n.deferred = append(n.deferred, evt)
		s.trace.Deferred(evt)
		return true, nil
	}

	s.trace.Record(evt)

	// dispatching...

	s.effects.reset()

	ctx := &Ctx{
		sim:  s,
		self: evt.Target,
		out:  &s.effects,
		gen:  s.events,
	}

	switch evt.Kind {
	case EvRestart:
		if !n.reboot() {
			s.trace.Dropped(evt, "restart of a node that is not crashed")
			return true, nil
		}
		i := s.idx(evt.Target)
		s.streams.reseedNode(i, n.epoch)
		n.build(s.deps(evt.Target))
		s.requireDigester(evt.Target)
		s.trace.Note(EnRestart, s.now, evt.Target, "", evt.Seq)
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
	s.reportStates()
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
		s.trace.Note(EnDurableWrite, s.now, node, ef.Key, s.current)
	case EfSync:
		n.storage.Sync()
		s.trace.Note(EnSync, s.now, node, "", s.current)
	case EfCancelTimer:
		n.bumpTimer(ef.Name) // bumping the token invalidates the pending fire
		s.trace.Note(EnTimerCancelled, s.now, node, ef.Name, s.current)
	case EfSetTimer:
		// Bump first, so an earlier fire for this name goes stale. That makes
		// SetTimer reset rather than stack, which is what Raft wants from
		// every AppendEntries.
		token := n.bumpTimer(ef.Name)
		s.pushTimer(node, ef.Name, s.now.Add(ef.After), token)
		s.trace.Note(EnTimerSet, s.now, node, ef.Name, s.current)
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
		s.trace.Note(EnNote, s.now, to, "duplicated", s.current)
	}

}

func (s *Sim) scheduleDeliver(from, to int, payload Message) {
	link := s.network.link(from, to)
	at := s.now.Add(link.delay.Sample(s.streams.delay))

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
		h := s.node(id).handler
		if h == nil {
			// Crashed. There is no volatile state to fold, and that absence is
			// itself part of the run: fold a marker so a crashed node and a
			// live one with empty state hash differently.
			s.trace.FoldDown(id)
			continue
		}
		s.trace.FoldState(id, h.(StateDigester))
	}
}

// reportStates records belief changes. Runs at every trace level above off
func (s *Sim) reportStates() {
	if s.cfg.TraceLevel == TraceOff {
		return
	}
	for _, id := range s.order {
		h := s.node(id).handler
		if h == nil {
			s.trace.ReportState(s.now, s.current, id, "down")
			continue
		}
		if r, ok := h.(StateReporter); ok {
			s.trace.ReportState(s.now, s.current, id, r.StateString())
		}
	}
}

// States returns what every node currently believes, for assertions and for
// printing on failure. Nodes that do not implement StateReporter are omitted.
func (s *Sim) States() map[int]string {
	out := make(map[int]string, len(s.order))
	for _, id := range s.order {
		h := s.node(id).handler
		if h == nil {
			out[id] = "down"
			continue
		}
		if r, ok := h.(StateReporter); ok {
			out[id] = r.StateString()
		}
	}
	return out
}
