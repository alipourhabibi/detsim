package maslave

import (
	"encoding/binary"
	"io"
)

const TickTimer = "tick"

// consumed
type Transport interface {
	Send(to int, msg any)
}

type Clock interface {
	Now() int64
	SetTimer(name string, after int64)
}

type Role uint8

const (
	Master Role = iota
	Slave
)

type Ping struct {
	Round uint64
}

func (t Ping) HashInto(w io.Writer) {
	var buf [9]byte
	buf[0] = 'p'
	binary.LittleEndian.PutUint64(buf[1:], t.Round)
	w.Write(buf[:])
}

func (t Ping) Equal(other any) bool {
	o, ok := other.(Ping)
	if !ok {
		return false
	}
	return t.Round == o.Round
}

type Pong struct {
	Round uint64
}

func (t Pong) HashInto(w io.Writer) {
	var buf [9]byte
	buf[0] = 'o'
	binary.LittleEndian.PutUint64(buf[1:], t.Round)
	w.Write(buf[:])
}

func (t Pong) Equal(other any) bool {
	o, ok := other.(Pong)
	if !ok {
		return false
	}
	return t.Round == o.Round
}

type Node struct {
	ID    int
	Peers []int
	Role  Role

	PingsSent     uint64
	PingsReceived uint64
	PongsReceived uint64
	Round         uint64

	transport  Transport
	clock      Clock
	tickPeriod int64
}

func New(
	id int,
	peers []int,
	role Role,
	tickPeriod int64,
	transport Transport,
	clock Clock,
) *Node {
	return &Node{
		ID:         id,
		Peers:      peers,
		Role:       role,
		transport:  transport,
		clock:      clock,
		tickPeriod: tickPeriod,
	}
}

func (n *Node) OnMessage(from int, msg any) {
	switch m := msg.(type) {
	case Ping:
		n.PingsReceived++
		n.transport.Send(from, Pong{Round: m.Round})
	case Pong:
		n.PongsReceived++
	}
}

func (n *Node) OnTick(name string) {
	n.Round++
	for _, p := range n.Peers {
		n.PingsSent++
		n.transport.Send(p, Ping{
			Round: n.Round,
		})
	}
	n.clock.SetTimer(name, n.tickPeriod)
}

// Start arms the first tick. Only the master ticks; slaves are purely
// reactive, so arming a timer for them would be a timer that never matters.
func (n *Node) Start() {
	if n.Role == Master {
		n.clock.SetTimer(TickTimer, n.tickPeriod)
	}
}
