package pingpong

import (
	"encoding/binary"
	"io"
)

type Transport interface {
	Send(to int, msg Ball)
}

type tick struct {
	N uint64
}

func (t tick) HashInto(w io.Writer) {
	var buf [9]byte
	buf[0] = 't'
	binary.LittleEndian.PutUint64(buf[1:], t.N)
	w.Write(buf[:])
}

func (t tick) Equal(other any) bool {
	o, ok := other.(tick)
	if !ok {
		return false
	}
	return t.N == o.N
}

type Ball struct {
	Round  uint64
	IsPing bool
}

func (b Ball) HashInto(w io.Writer) {
	var buf [10]byte
	buf[0] = 'b'
	binary.LittleEndian.PutUint64(buf[1:9], b.Round)
	if b.IsPing {
		buf[9] = 1
	}
	w.Write(buf[:])
}

func (b Ball) Equal(other any) bool {
	o, ok := other.(Ball)
	if !ok {
		return false
	}
	return b.Round == o.Round
}

type PingPong struct {
	Id       int
	Peer     int
	Count    uint64
	Rounds   uint64
	Ticks    uint64
	MaxTicks uint64

	transport Transport

	serves bool

	Interval uint64
}

func New(
	id int,
	peer int,
	count uint64,
	maxTicks uint64,
	transport Transport,
	serves bool,
	interval uint64,
) *PingPong {
	return &PingPong{
		Id:        id,
		Peer:      peer,
		Count:     count,
		MaxTicks:  maxTicks,
		transport: transport,
		serves:    serves,
		Interval:  interval,
	}
}

func (p *PingPong) OnTick() bool {
	if p.MaxTicks <= p.Ticks {
		return false
	}
	p.Ticks++
	p.transport.Send(p.Peer, Ball{Round: p.Rounds, IsPing: true})
	return true
}

func (p *PingPong) PingDevlier(from int, msg Ball) {
	if p.Rounds > p.Count {
		return
	}
	p.Rounds++
	p.transport.Send(from, Ball{Round: p.Rounds, IsPing: false})
}

func (p *PingPong) PongDevlier(from int, msg Ball) {

}

// in the protocol
func (p *PingPong) Start() {
	if p.serves {
		p.transport.Send(p.Peer, Ball{Round: 0, IsPing: true})
	}
}
