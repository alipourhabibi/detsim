package sim

import "fmt"

// Virtual time in milliseconds
// Time is an absolute instant in virtual milliseconds.
type Time int64

// Duration is a span of virtual milliseconds. Distinct from Time so a duration
// can never be passed where an instant is expected.
type Duration int64

type EventKind uint8

const (
	EvDeliver EventKind = iota // a message arrives
	EvTimer                    // a timer fires
	EvRestart                  // a crashed node comes back up
	EvFault                    // the harness changes the simulator
)

func (t Time) Add(d Duration) Time {
	return t + Time(d)
}

func (t Time) Sub(u Time) Duration {
	return Duration(t - u)
}

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

	Payload Hashable // message body or timer name
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

// Describe formats the event without the time and seq prefix
func (e Event) Describe() string {
	switch e.Kind {
	case EvDeliver:
		return fmt.Sprintf("deliver %d->%d ep=%d %v", e.Source, e.Target, e.Epoch, e.Payload)
	case EvTimer:
		return fmt.Sprintf("timer %q tok=%d", e.Name, e.Token)
	case EvRestart:
		return fmt.Sprintf("restart ep=%d", e.Epoch)
	case EvFault:
		return fmt.Sprintf("fault %v", e.Payload)
	default:
		return e.Kind.String()
	}
}

func (e Event) String() string {
	return fmt.Sprintf("t=%d #%d %s", e.At, e.Seq, e.Describe())
}
