package sim

// Turn holds the Ctx for the duration of one callback. Protocol code that
// reaches for a Ctx outside a callback panics instead of silently acting on a
// stale one.
type Turn struct {
	ctx *Ctx
}

func (t *Turn) Enter(ctx *Ctx) {
	t.ctx = ctx
}

func (t *Turn) Leave() {
	t.ctx = nil
}

func (t *Turn) Ctx() *Ctx {
	if t.ctx == nil {
		panic("sim: protocol acted outside a callback")
	}
	return t.ctx
}
