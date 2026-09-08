package sim

import (
	"fmt"
	"io"
)

var (
	_ Fault = crashFault{}
	_ Fault = pauseFault{}
	_ Fault = restartFault{}
	_ Fault = resumeFault{}
)

type crashFault struct {
	Node     int
	WipeDisk bool
	Downtime Duration
}
type pauseFault struct {
	Node     int
	Duration Duration
}

type restartFault struct {
	Node int
}

type resumeFault struct {
	Node  int
	Token uint64
}

func NewCrashFault(nodeId int, wipeDisk bool, downtime Duration) Fault {
	return crashFault{
		Node:     nodeId,
		WipeDisk: wipeDisk,
		Downtime: downtime,
	}
}

func (f crashFault) HashInto(w io.Writer) {
	h := hashTag(w, 'C')
	h.node(f.Node)
	h.bool(f.WipeDisk)
	h.dur(f.Downtime)
}

func (f crashFault) Equal(other any) bool {
	o, ok := other.(crashFault)
	return ok && f == o
}

// node_fault.go
func (f crashFault) Apply(s *Sim) {
	res := s.node(f.Node).crash(f.WipeDisk)
	if !res.Crashed {
		s.trace.Note(EnNote, s.now, f.Node, "crash ignored: already down", s.current)
		return
	}

	s.trace.Note(EnCrash, s.now, f.Node, "", s.current)
	s.history.abandonClient(f.Node)
	if res.Wiped {
		s.trace.Note(EnDiskWiped, s.now, f.Node, "", s.current)
	} else if res.Lost > 0 {
		s.trace.Note(EnRollback, s.now, f.Node, fmt.Sprintf("%d unsynced keys lost", res.Lost), s.current)
	}

	if f.Downtime > 0 {
		s.pushRestart(f.Node, s.now.Add(f.Downtime))
	}
}

func (c crashFault) String() string {
	return fmt.Sprintf("node %d crashed; wipe disk: %t", c.Node, c.WipeDisk)
}

func NewPauseFault(nodeId int, duration Duration) Fault {
	return pauseFault{
		Node:     nodeId,
		Duration: duration,
	}
}

func (f pauseFault) HashInto(w io.Writer) {
	h := hashTag(w, 'Z')
	h.node(f.Node)
	h.dur(f.Duration)
}

func (f pauseFault) Equal(other any) bool {
	o, ok := other.(pauseFault)
	return ok && f == o
}

func (c pauseFault) Apply(s *Sim) {
	if c.Duration <= 0 {
		panic(fmt.Sprintf("sim: pause duration must be positive, got %d", c.Duration))
	}
	tok, ok := s.node(c.Node).pause(s.now.Add(c.Duration))
	if !ok {
		s.trace.Note(EnNote, s.now, c.Node, "pause ignored: node not healthy", s.current)
		return
	}
	s.trace.Note(EnPause, s.now, c.Node, "", s.current)
	s.ScheduleFault(s.now.Add(c.Duration), resumeFault{Node: c.Node, Token: tok})
}

func (c pauseFault) String() string {
	return fmt.Sprintf("node %d paused; duration: %d", c.Node, c.Duration)
}

func NewRestartFault(nodeId int) Fault {
	return restartFault{
		Node: nodeId,
	}
}

func (f restartFault) HashInto(w io.Writer) {
	h := hashTag(w, 'R')
	h.node(f.Node)
}

func (f restartFault) Equal(other any) bool {
	o, ok := other.(restartFault)
	return ok && f == o
}

func (c restartFault) Apply(s *Sim) {
	s.pushRestart(c.Node, s.now)
}

func (c restartFault) String() string {
	return fmt.Sprintf("node %d restarted", c.Node)
}

func NewResumeFault(nodeId int) Fault {
	return resumeFault{
		Node: nodeId,
	}
}

func (f resumeFault) HashInto(w io.Writer) {
	h := hashTag(w, 'M')
	h.node(f.Node)
	h.u64(f.Token)
}

func (f resumeFault) Equal(other any) bool {
	o, ok := other.(resumeFault)
	return ok && f == o
}

func (c resumeFault) Apply(s *Sim) {
	events, ok := s.node(c.Node).resume(c.Token)
	if !ok {
		s.trace.Dropped(Event{At: s.now, Kind: EvFault, Target: c.Node},
			"resume superseded")
		return
	}
	s.trace.Note(EnResume, s.now, c.Node, "", s.current)
	for _, e := range events {
		s.requeue(e)
	}
}

func (c resumeFault) String() string {
	return fmt.Sprintf("node %d resumed with token %d", c.Node, c.Token)
}
