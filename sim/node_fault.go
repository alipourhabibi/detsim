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
}
type pauseFault struct {
	Node     int
	Duration Time
}

type restartFault struct {
	Node int
}

type resumeFault struct {
	Node int
}

func newCrashFault(nodeId int, WipeDisk bool) crashFault {
	return crashFault{
		Node: nodeId,
	}
}

func (c crashFault) HashInto(w io.Writer) {
	wd := 0
	if c.WipeDisk {
		wd = 1
	}
	hashInts(w, 'C', c.Node, wd)
}

func (c crashFault) Equal(other any) bool {
	o, ok := other.(crashFault)
	if !ok {
		return false
	}
	return o.Node == c.Node && o.WipeDisk == c.WipeDisk
}

func (c crashFault) Apply(s *Sim) {
	s.node(c.Node).crash(c.WipeDisk)
}

func (c crashFault) String() string {
	return fmt.Sprintf("node %d crashed; wipe disk: %t", c.Node, c.WipeDisk)
}

func newPauseFault(nodeId int, duration Time) pauseFault {
	return pauseFault{
		Node:     nodeId,
		Duration: duration,
	}
}

func (c pauseFault) HashInto(w io.Writer) {
	hashInts(w, 'C', c.Node, int(c.Duration))
}

func (c pauseFault) Equal(other any) bool {
	o, ok := other.(pauseFault)
	if !ok {
		return false
	}
	return o.Node == c.Node && o.Duration == c.Duration
}

func (c pauseFault) Apply(s *Sim) {
	s.node(c.Node).pause(c.Duration)
}

func (c pauseFault) String() string {
	return fmt.Sprintf("node %d pauseed; duration: %d", c.Node, c.Duration)
}

func newRestartFault(nodeId int) restartFault {
	return restartFault{
		Node: nodeId,
	}
}

func (c restartFault) HashInto(w io.Writer) {
	hashInts(w, 'C', c.Node)
}

func (c restartFault) Equal(other any) bool {
	o, ok := other.(restartFault)
	if !ok {
		return false
	}
	return o.Node == c.Node
}

func (c restartFault) Apply(s *Sim) {
	deps := s.deps(c.Node)
	s.node(c.Node).restart(deps)
}

func (c restartFault) String() string {
	return fmt.Sprintf("node %d restarted", c.Node)
}

func newResumeFault(nodeId int) resumeFault {
	return resumeFault{
		Node: nodeId,
	}
}

func (c resumeFault) HashInto(w io.Writer) {
	hashInts(w, 'C', c.Node)
}

func (c resumeFault) Equal(other any) bool {
	o, ok := other.(resumeFault)
	if !ok {
		return false
	}
	return o.Node == c.Node
}

func (c resumeFault) Apply(s *Sim) {
	events := s.node(c.Node).resume()
	for _, e := range events {
		s.requeue(e)
	}
}

func (c resumeFault) String() string {
	return fmt.Sprintf("node %d resumed", c.Node)
}
