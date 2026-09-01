package sim

// Handler is what the harness implements.
// The sim calls them. The harness never reaches the queue, network or RNG.
type Handler interface {
	OnMessage(ctx *Ctx, from int, msg Message)
	OnTimer(ctx *Ctx, name string)
	OnRestart(ctx *Ctx)
}

type NoOpHandler struct{}

func (NoOpHandler) OnMessage(*Ctx, int, Message) {}
func (NoOpHandler) OnTimer(*Ctx, string)         {}
func (NoOpHandler) OnRestart(*Ctx)               {}
