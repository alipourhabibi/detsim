package sim

// Ctx is one turn in the simulation, during which the harness acts as one node.
type Ctx struct {
	sim  *Sim
	self int
	out  *Effects
	gen  uint64 // sim.events at creation; guards use-after-callback
}

func (c *Ctx) check() {
	if c.gen != c.sim.events {
		panic("sim: Ctx used outside the callback it was passed to")
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

// durable read
func (c *Ctx) Get(key string) ([]byte, bool) {
	c.check()
	return c.sim.node(c.self).storage.Get(key)
}

// --- effects: recorded now, applied when the turn ends ---
//
// Send does not send; Save does not write. The sim carries them out after the
// callback, writes before sends, so a node can't reply to a vote it hasn't
// persisted.

func (c *Ctx) Send(to int, msg Message) {
	c.check()
	c.out.buf = append(c.out.buf, Effect{Kind: EfSend, To: to, Msg: msg})
}

func (c *Ctx) SetTimer(name string, after Duration) {
	c.check()
	c.out.buf = append(c.out.buf, Effect{Kind: EfSetTimer, Name: name, After: after})
}

func (c *Ctx) CancelTimer(name string) {
	c.check()
	c.out.buf = append(c.out.buf, Effect{Kind: EfCancelTimer, Name: name})
}

func (c *Ctx) Put(key string, value []byte) {
	c.check()
	c.out.buf = append(c.out.buf, Effect{
		Kind: EfPut,
		To:   c.self,
		Key:  key,
		// should copy it, a reusing buffer by harness, may corrupt earlier writes
		Value: append([]byte(nil), value...),
	})
}

func (c *Ctx) Sync() {
	c.check()
	c.out.buf = append(c.out.buf, Effect{Kind: EfSync})
}
