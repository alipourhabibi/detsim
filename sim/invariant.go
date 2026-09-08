package sim

import "fmt"

// Invariant is a safety check, run after every event. It must only read: no
// scheduling, no sending, no writing. It runs a lot, so keep it cheap.
type Invariant func(s *Sim) error

type InvariantError struct {
	At     Time
	Seq    uint64
	Err    error
	States string
}

func (e *InvariantError) Error() string {
	return fmt.Sprintf("invariant broken at t=%d #%d: %v\n%s",
		e.At, e.Seq, e.Err, e.States)
}

func (e *InvariantError) Unwrap() error { return e.Err }

// AddInvariant registers a safety check, run after every event.
//
// It must only read. If it sends, schedules or writes, it changes the run it is
// watching and the run stops being a function of the seed. Nothing enforces
// this.
//
// It runs a lot, so keep it cheap.
func (s *Sim) AddInvariant(i Invariant) {
	if s.started {
		panic("sim: AddInvariant after Start")
	}
	s.invariants = append(s.invariants, i)
}

func (s *Sim) runInvariants() error {
	for _, inv := range s.invariants {
		err := inv(s)
		if err != nil {
			return &InvariantError{
				At:     s.now,
				Seq:    s.current,
				Err:    err,
				States: s.StatesString(),
			}
		}
	}

	return nil
}

// ClearInvariants removes every registered invariant.
func (s *Sim) ClearInvariants() {
	s.invariants = nil
}
