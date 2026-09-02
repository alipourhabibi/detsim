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
	Downtime Time
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

func NewCrashFault(nodeId int, wipeDisk bool, downtime Time) crashFault {
	return crashFault{
		Node:     nodeId,
		WipeDisk: wipeDisk,
		Downtime: downtime,
	}
}

func (f crashFault) HashInto(w io.Writer) {
	wd := 0
	if f.WipeDisk {
		wd = 1
	}
	hashInts(w, 'C', f.Node, wd, int(f.Downtime))
}

func (c crashFault) Equal(other any) bool {
	o, ok := other.(crashFault)
	if !ok {
		return false
	}
	return o.Node == c.Node && o.WipeDisk == c.WipeDisk
}

func (f crashFault) Apply(s *Sim) {
	if !s.node(f.Node).crash(f.WipeDisk) {
		s.trace.Note(EnNote, s.now, f.Node, "crash ignored: already down")
		return
	}
	s.trace.Note(EnCrash, s.now, f.Node, "")
	if f.WipeDisk {
		s.trace.Note(EnDiskWiped, s.now, f.Node, "")
	}
	if f.Downtime > 0 {
		s.pushRestart(f.Node, s.now+f.Downtime)
	}
}

func (c crashFault) String() string {
	return fmt.Sprintf("node %d crashed; wipe disk: %t", c.Node, c.WipeDisk)
}

func NewPauseFault(nodeId int, duration Duration) pauseFault {
	return pauseFault{
		Node:     nodeId,
		Duration: duration,
	}
}

func (f pauseFault) HashInto(w io.Writer) {
	hashInts(w, 'Z', f.Node, int(f.Duration))
}

func (c pauseFault) Equal(other any) bool {
	o, ok := other.(pauseFault)
	if !ok {
		return false
	}
	return o.Node == c.Node && o.Duration == c.Duration
}

func (c pauseFault) Apply(s *Sim) {
	if c.Duration <= 0 {
		panic(fmt.Sprintf("sim: pause duration must be positive, got %d", c.Duration))
	}
	tok, ok := s.node(c.Node).pause(s.now.Add(c.Duration))
	if !ok {
		s.trace.Note(EnNote, s.now, c.Node, "pause ignored: node not healthy")
		return
	}
	s.trace.Note(EnPause, s.now, c.Node, "")
	s.ScheduleFault(s.now.Add(c.Duration), resumeFault{Node: c.Node, Token: tok})
}

func (c pauseFault) String() string {
	return fmt.Sprintf("node %d paused; duration: %d", c.Node, c.Duration)
}

func NewRestartFault(nodeId int) restartFault {
	return restartFault{
		Node: nodeId,
	}
}

func (c restartFault) HashInto(w io.Writer) {
	hashInts(w, 'R', c.Node)
}

func (c restartFault) Equal(other any) bool {
	o, ok := other.(restartFault)
	if !ok {
		return false
	}
	return o.Node == c.Node
}

func (c restartFault) Apply(s *Sim) {
	s.pushRestart(c.Node, s.now)
}

func (c restartFault) String() string {
	return fmt.Sprintf("node %d restarted", c.Node)
}

func NewResumeFault(nodeId int) resumeFault {
	return resumeFault{
		Node: nodeId,
	}
}

func (c resumeFault) HashInto(w io.Writer) {
	hashInts(w, 'M', c.Node, int(c.Token))
}

func (c resumeFault) Equal(other any) bool {
	o, ok := other.(resumeFault)
	if !ok {
		return false
	}
	return o.Node == c.Node && o.Token == c.Token
}

func (c resumeFault) Apply(s *Sim) {
	events, ok := s.node(c.Node).resume(c.Token)
	if !ok {
		s.trace.Dropped(Event{At: s.now, Kind: EvFault, Target: c.Node},
			"resume superseded")
		return
	}
	s.trace.Note(EnResume, s.now, c.Node, "")
	for _, e := range events {
		s.requeue(e)
	}
}

func (c resumeFault) String() string {
	return fmt.Sprintf("node %d resumed with token %d", c.Node, c.Token)
}
