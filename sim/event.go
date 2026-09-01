package sim

import "fmt"

// Virtual time in milliseconds
type Time int64

type EventKind uint8

const (
	EvDeliver EventKind = iota // a message arrives
	EvTimer                    // a timer fires
	EvRestart                  // a crashed node comes back up
	EvFault                    // the harness changes the simulator
)

type Event struct {
	At     Time
	Seq    uint64 // assigned by Sim at insertion; makes ordering total
	Kind   EventKind
	Target int
	Source int    // sender; -1 for timers
	Parent uint64 // Seq of the event that caused this one; 0 if root

	Epoch uint64 // destination's epoch at schedule time; stale => discard
	Token uint64 // EvTimer only: the timer's token at schedule time
	Name  string // EvTimer only: timer name

	Payload any // message body or timer name
}

func (k EventKind) String() string {
	switch k {
	case EvDeliver:
		return "EventDeliver"
	case EvTimer:
		return "EventTimer"
	case EvRestart:
		return "EventRestart"
	case EvFault:
		return "EventFault"
	}
	return fmt.Sprintf("EvUnknown(%d)", uint8(k))
}

func (e Event) String() string {
	switch e.Kind {
	case EvDeliver:
		return fmt.Sprintf("t=%d #%d deliver %d->%d ep=%d %v", e.At, e.Seq, e.Source, e.Target, e.Epoch, e.Payload)
	case EvTimer:
		return fmt.Sprintf("t=%d #%d timer %q node=%d tok=%d", e.At, e.Seq, e.Name, e.Target, e.Token)
	case EvRestart:
		return fmt.Sprintf("t=%d #%d restart node=%d ep=%d", e.At, e.Seq, e.Target, e.Epoch)
	case EvFault:
		return fmt.Sprintf("t=%d #%d fault %v", e.At, e.Seq, e.Payload)
	default:
		return fmt.Sprintf("t=%d #%d %v", e.At, e.Seq, e.Kind)
	}
}
