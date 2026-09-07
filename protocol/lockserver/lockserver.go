// Package lockserver is a small distributed lock service, like the ones people
// build on top of etcd, ZooKeeper or Consul.
//
// One server owns the lock. Clients acquire it, hold it while they do work,
// then release it. Whatever the lock protects, only one client may be inside at
// a time. That is the whole contract, and it is the reason the service exists.
//
// The server keeps the lock owner on disk, because it must survive a power
// loss. There is a bug in how it does that. Look for the word BUG below.
package lockserver

import (
	"encoding/binary"
	"fmt"
	"io"
)

const NoHolder = -1

// Acquire asks for the lock. Fencing is by attempt number: the client bumps it
// on every retry, so a late reply to an old attempt can be recognised and
// thrown away.
type Acquire struct {
	Attempt uint64
}

// Granted is the lease. The client may enter the critical section.
type Granted struct {
	Attempt uint64
}

// Release gives the lock back.
type Release struct{}

func (m Acquire) String() string {
	return fmt.Sprintf("Acquire#%d", m.Attempt)
}

func (m Granted) String() string {
	return fmt.Sprintf("Granted#%d", m.Attempt)
}

func (m Release) String() string {
	return "Release"
}

type Msg interface {
	HashInto(w io.Writer)
	Equal(other any) bool
}

func writeTagged(w io.Writer, tag byte, n uint64) {
	var buf [9]byte
	buf[0] = tag
	binary.LittleEndian.PutUint64(buf[1:], n)
	w.Write(buf[:])
}

func (m Acquire) HashInto(w io.Writer) {
	writeTagged(w, 'a', m.Attempt)
}

func (m Acquire) Equal(o any) bool {
	x, ok := o.(Acquire)
	return ok && x == m
}

func (m Granted) HashInto(w io.Writer) {
	writeTagged(w, 'g', m.Attempt)
}

func (m Granted) Equal(o any) bool {
	x, ok := o.(Granted)
	return ok && x == m
}

func (m Release) HashInto(w io.Writer) {
	writeTagged(w, 'r', 0)
}

func (m Release) Equal(o any) bool {
	_, ok := o.(Release)
	return ok
}

type Transport interface {
	Send(to int, msg Msg)
}

// Storage is the write-ahead log for the lock owner.
//
// Put is write(2). It copies bytes into the kernel page cache and returns.
// Nothing is on the platter yet.
//
// Sync is fsync(2). It blocks until the kernel has pushed those bytes to stable
// storage. It costs milliseconds on spinning rust and tens of microseconds on
// an SSD, which is why people are tempted to skip it. Postgres has an fsync
// setting. Redis has appendfsync. MySQL has innodb_flush_log_at_trx_commit.
// Every one of them exists because fsync is the bottleneck, and every one of
// them corrupts your data if you turn it off and lose power.
//
// A process crash is survivable without fsync: the kernel still holds the page
// cache and flushes it later. Power loss, kernel panic and a destroyed VM are
// not. Those are what CrashFault models.
type Storage interface {
	Get(key string) ([]byte, bool)
	Put(key string, value []byte)
	Sync()
}

type Timers interface {
	SetTimer(name string, afterMs int64)
	CancelTimer(name string)
}

const keyOwner = "lock.owner"

// Server is the lock manager. The owner record is durable state: it is the only
// thing standing between a reboot and two clients in the critical section at
// once, so it lives on disk and never in a field.
type Server struct {
	Id int

	LastGranted int

	transport Transport
	storage   Storage
}

func NewServer(id int, t Transport, st Storage) *Server {
	return &Server{Id: id, transport: t, storage: st, LastGranted: NoHolder}
}

func (s *Server) Start() {
	s.LastGranted = NoHolder // soft state, gone on reboot
}

func (s *Server) Owner() int {
	v, ok := s.storage.Get(keyOwner)
	if !ok {
		return NoHolder
	}
	return int(int64(binary.LittleEndian.Uint64(v)))
}

func (s *Server) recordOwner(id int) {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], uint64(int64(id)))
	s.storage.Put(keyOwner, buf[:])

	s.LastGranted = id

	// BUG: the write is never flushed. There is no s.storage.Sync() here.
	//
	// Put is write(2), not fsync(2). The record sits in the page cache and the
	// server acknowledges the grant anyway. If the machine loses power inside
	// that window, the record is gone but the client has already entered the
	// critical section.
	//
	// On reboot the server reads an empty owner record, concludes the lock is
	// free, and grants it to somebody else. Two clients now hold the same lock.
	// If it guards a database they both write. If it elects a leader you have
	// split brain.
	//
	// This is the classic acknowledge-before-durable bug. MongoDB shipped it
	// for years. Kafka's acks=1 is the same trade made on purpose.
	//
	// The fix is one line: s.storage.Sync()
	//
	// It costs an fsync per grant, which caps throughput at a few thousand
	// grants per second instead of hundreds of thousands. That is the price of
	// the guarantee, and it is not optional for a lock service.
}

func (s *Server) OnAcquire(from int, m Acquire) {
	owner := s.Owner()
	if owner != NoHolder && owner != from {
		return // held by someone else. The client backs off and retries.
	}
	s.recordOwner(from)
	s.transport.Send(from, Granted{Attempt: m.Attempt})
}

func (s *Server) OnRelease(from int) {
	if s.Owner() != from {
		return // not the owner, so this is a stale release. Ignore it.
	}
	s.recordOwner(NoHolder)
}

// Client acquires the lock, holds it for a fixed lease, releases, and starts
// over.
//
// Every field here is soft state. After a crash the client forgets it ever held
// the lock and starts from scratch.
type Client struct {
	Id     int
	Server int

	// Holding means this client believes it is inside the critical section.
	// Two clients with Holding true at the same instant is the safety
	// violation the whole service exists to prevent.
	Holding bool

	attempt uint64

	RetryMs int64 // backoff before asking again
	HoldMs  int64 // lease length: how long the client stays inside

	transport Transport
	timers    Timers
}

func (c Client) Attempt() uint64 {
	return c.attempt
}

func NewClient(id, server int, retryMs, holdMs int64, t Transport, tm Timers) *Client {
	return &Client{
		Id:        id,
		Server:    server,
		RetryMs:   retryMs,
		HoldMs:    holdMs,
		transport: t,
		timers:    tm,
	}
}

func (c *Client) Start() {
	c.Holding = false
	c.acquire()
}

func (c *Client) acquire() {
	c.attempt++
	c.transport.Send(c.Server, Acquire{Attempt: c.attempt})
	c.timers.SetTimer("retry", c.RetryMs)
}

func (c *Client) OnGranted(m Granted) {
	// Fencing check. A Granted for attempt 3 can still be in flight while the
	// client is on attempt 7, and the server may have handed the lock to
	// someone else in between. Accepting the stale reply would put two clients
	// in the critical section without any crash at all.
	if m.Attempt != c.attempt {
		return
	}
	c.Holding = true
	c.timers.CancelTimer("retry")
	c.timers.SetTimer("lease", c.HoldMs)
}

func (c *Client) OnTimer(name string) {
	switch name {
	case "retry":
		c.acquire()
	case "lease":
		// Lease expired. Leave the critical section, release, and queue up
		// another attempt so the workload keeps running.
		c.Holding = false
		c.transport.Send(c.Server, Release{})
		c.timers.SetTimer("retry", c.RetryMs)
	}
}
