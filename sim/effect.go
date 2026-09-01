package sim

import "fmt"

type EffectKind uint8

const (
	EfPut EffectKind = iota
	EfSync
	EfCancelTimer
	EfSetTimer
	EfSend
)

func (k EffectKind) String() string {
	switch k {
	case EfPut:
		return "put"
	case EfSync:
		return "sync"
	case EfCancelTimer:
		return "cancelTimer"
	case EfSetTimer:
		return "setTimer"
	case EfSend:
		return "send"
	}
	return fmt.Sprintf("EfUnknown(%d)", uint8(k))
}

func (e Effect) String() string {
	switch e.Kind {
	case EfPut:
		return fmt.Sprintf("put %q (%d bytes)", e.Key, len(e.Value))
	case EfSync:
		return "sync"
	case EfCancelTimer:
		return fmt.Sprintf("cancelTimer %q", e.Name)
	case EfSetTimer:
		return fmt.Sprintf("setTimer %q after=%d", e.Name, e.After)
	case EfSend:
		return fmt.Sprintf("send ->%d %v", e.To, e.Msg)
	default:
		return fmt.Sprintf("%v", e.Kind)
	}
}

type Effect struct {
	Kind  EffectKind
	To    int
	Msg   Message
	Name  string
	After Time
	Key   string
	Value []byte
}

// Effects is the buffer Ctx writes into. One per Sim, reused across events.
type Effects struct {
	buf []Effect
}

func (e *Effects) reset() {
	clear(e.buf) // GC
	e.buf = e.buf[:0]
}

func (e *Effects) len() int {
	return len(e.buf)
}
