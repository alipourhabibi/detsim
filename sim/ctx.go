package sim

import (
	"fmt"
	"slices"
)

// Ctx is one turn in the simulation, during which the harness acts as one node.
type Ctx struct {
	sim  *Sim
	self int
	out  *Effects
	gen  uint64 // sim.events at creation; it guards the harness from use after callback

	// pending holds this turn's writes, so a read sees them. nil value = deleted.
	pending map[string][]byte

	readonly bool
}

func (c *Ctx) check() {
	if c.gen != c.sim.events {
		panic("sim: Ctx used outside the callback it was passed to")
	}
}

// checkWrite guards the effect methods.
func (c *Ctx) checkWrite() {
	c.check()
	if c.readonly {
		panic("sim: this Ctx is read-only (state digest or report)")
	}
}

// --- reads: answered now ---

func (c *Ctx) Self() int {
	c.check()
	return c.self
}

// skew-aware; not sim.now directly
func (c *Ctx) Now() Time {
	c.check()
	return c.sim.nodeNow(c.self)
}

// this node's stream
func (c *Ctx) Rand() Rand {
	c.check()
	return c.sim.nodeRand(c.self)
}

// Get reads this node's storage, including writes made earlier in this turn.
// Other nodes see those writes only after the turn ends.
func (c *Ctx) Get(key string) ([]byte, bool) {
	c.check()
	if v, ok := c.pending[key]; ok {
		if v == nil {
			return nil, false // deleted in this turn
		}
		return append([]byte(nil), v...), true
	}
	return c.sim.node(c.self).storage.Get(key)
}

// Keys lists this node's keys, including this turn's writes. Sorted.
func (c *Ctx) Keys() []string {
	c.check()

	set := map[string]struct{}{}
	for _, k := range c.sim.node(c.self).storage.Keys() {
		set[k] = struct{}{}
	}
	for k, v := range c.pending {
		if v == nil {
			delete(set, k) // deleted in this turn
		} else {
			set[k] = struct{}{}
		}
	}

	out := make([]string, 0, len(set))
	for k := range set {
		out = append(out, k)
	}
	slices.Sort(out)
	return out
}

// --- effects: sends and timers happen when the turn ends ---
//
// Writes go to this node's storage view right away, so a later Get in the same
// turn sees them. Other nodes see nothing until the callback returns.
// Sync decides what survives a crash.

func (c *Ctx) Send(to int, msg Message) {
	c.checkWrite()
	if to == c.self {
		panic(fmt.Sprintf("sim: node %d sent to itself", to))
	}
	if _, ok := c.sim.index[to]; !ok {
		panic(fmt.Sprintf("sim: node %d sent to unknown node %d", c.self, to))
	}
	c.out.buf = append(c.out.buf, Effect{Kind: EfSend, To: to, Msg: msg})
}

func (c *Ctx) SetTimer(name string, after Duration) {
	c.checkWrite()
	c.out.buf = append(c.out.buf, Effect{Kind: EfSetTimer, Name: name, After: after})
}

func (c *Ctx) CancelTimer(name string) {
	c.checkWrite()
	c.out.buf = append(c.out.buf, Effect{Kind: EfCancelTimer, Name: name})
}

func (c *Ctx) Put(key string, value []byte) {
	c.checkWrite()
	// copy: the harness may reuse its buffer
	v := append([]byte(nil), value...)
	if c.pending == nil {
		c.pending = map[string][]byte{}
	}
	c.pending[key] = v
	c.out.buf = append(c.out.buf, Effect{
		Kind:  EfPut,
		To:    c.self,
		Key:   key,
		Value: v,
	})
}

func (c *Ctx) Delete(key string) {
	c.checkWrite()
	if c.pending == nil {
		c.pending = map[string][]byte{}
	}
	c.pending[key] = nil
	c.out.buf = append(c.out.buf, Effect{
		Kind: EfDelete,
		To:   c.self,
		Key:  key,
	})
}

func (c *Ctx) Sync() {
	c.checkWrite()
	c.out.buf = append(c.out.buf, Effect{Kind: EfSync})
}
