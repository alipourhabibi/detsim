// A partitioned node keeps running: its timers fire, its state machine advances,
// it just can’t reach anyone. A paused node is frozen: no timers fire while paused,
// then they all fire at once on resume. Different bugs. Don’t confuse them while reading a trace.

package sim

import "fmt"

/*
Crash:
- volatile state is discarded
- pending timers cancel
- message in flight toward node are dropped
- message in flight from node are kept

Restart:
- node reconstruct from durable storage
- fresh volatile state
- restart delay
- occasionally wipe durable storage

Pause:
- node keeps everything, just do not respond
- then resume with lots of stale message waiting
- best for split brain bugs

Clock skew:
- nodes disagree about the time
- clock drifts, jump when NTP corrects and can go backwards
- each node a clock offset and a drift rate, so node.Now() differs from simulator time.

Fault scheduling:
- scripted: exactly at certain t a crash/pause/partition/... happens
- random from seed: fault injector draws from RNG.
Bias the random schedules towards interesting moments, for example leader election
in raft rather than idle times or ...
This is intuition behind FoundationDB's BUGGIFY.
Lets the protocol code itself mark the dangerous points.
*/

/*
 Build

Durable per-node storage. Crash, restart (with configurable downtime and optional disk loss),
and pause. A fault injector that can run scripted or seeded-random.

Acceptance: a scripted fault sequence produces an identical trace across runs and across processes.
You can articulate, in a written comment, your durable/volatile split and your partition-healing semantics.
*/

type Status uint8

const (
	Healthy Status = iota
	Crashed
	Paused
)

func (s Status) String() string {
	switch s {
	case Healthy:
		return "Healthy"
	case Crashed:
		return "Crashed"
	case Paused:
		return "Paused"
	}
	return fmt.Sprintf("Status Unknown(%d)", uint8(s))
}

type node struct {
	id      int
	status  Status
	handler Handler
	storage Store
	factory NodeFactory // kept for restart build
	clock   nodeClock

	// epoch increments on crash and again on restart. Every event carries the
	// destination's epoch at schedule time; a mismatch on pop means discard.
	epoch uint64

	// timers maps name -> current token. SetTimer bumps the token, which
	// invalidates any pending fire under that name. CancelTimer bumps it too.
	timers map[string]uint64

	// deferred holds events popped while paused. Replayed on resume with their
	// original Seq preserved, so relative order is exactly what it was.
	deferred []Event

	resumeAt   Time
	pauseToken uint64
}

func newNode(id int, f NodeFactory, st Store) *node {
	return &node{
		id:      id,
		factory: f,
		timers:  map[string]uint64{},
		storage: st,
	}
}

// Build a node from scratch
// It is called once at Register time and also at each Restart
type NodeFactory func(id int, deps Deps) Handler

type Deps struct {
	Id    int
	Store Store
	Rand  Rand
}

type nodeClock struct {
	offset Duration // fixed disagreement with sim time
}

func (c nodeClock) now(simNow Time) Time {
	return simNow.Add(c.offset)
}

// bump epoch, drop handler, optionally wipe
func (n *node) crash(wipeDisk bool) bool {
	if n.status == Crashed {
		return false
	}
	n.epoch++
	n.status = Crashed
	n.handler = nil
	n.deferred = nil
	n.resumeAt = 0

	clear(n.timers)

	if wipeDisk {
		n.storage.Wipe()
	} else {
		n.storage.Rollback()
	}
	return true
}

// bump epoch, change status,
// NOTE: no build yet
func (n *node) reboot() bool {
	if n.status != Crashed {
		return false
	}

	n.epoch++
	if n.epoch%2 != 0 {
		panic(fmt.Sprintf("sim: node %d epoch %d is odd after restart", n.id, n.epoch))
	}
	n.status = Healthy
	return true
}

// freezes the node until `until`
// return Token identifying this pause.
// used for scheduled resumes
func (n *node) pause(until Time) (uint64, bool) {
	if n.status != Healthy {
		return 0, false // a crashed node has nothing to freeze
	}
	n.pauseToken++
	n.status = Paused
	n.resumeAt = until
	return n.pauseToken, true
}

// returns deferred, clears it
func (n *node) resume(token uint64) ([]Event, bool) {
	if n.status != Paused {
		return nil, false
	}
	if token != 0 && token != n.pauseToken {
		return nil, false
	}
	n.status = Healthy
	n.resumeAt = 0
	held := n.deferred
	n.deferred = nil
	return held, true
}

func (n *node) bumpTimer(name string) uint64 {
	n.timers[name]++
	return n.timers[name]
}

func (n *node) build(deps Deps) {
	if n.factory == nil {
		panic("sim: node has no factory")
	}
	n.handler = n.factory(n.id, deps)
}
