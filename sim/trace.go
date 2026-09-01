package sim

import (
	"encoding/binary"
	"fmt"
	"hash"
	"hash/fnv"
	"io"
)

// Everything that can be hashed into the trace.
type Hashable interface {
	HashInto(w io.Writer)
	Equal(other any) bool
}

// StateDigester lets a node fold its state into the run hash, so two runs with
// an identical event sequence but divergent node state still differ.
type StateDigester interface {
	StateDigest(w io.Writer)
}

// A protocol message. Message is Hashable so payloads are checked at compile time
type Message interface {
	Hashable
}

type EntryKind uint8

const (
	EnEvent    EntryKind = iota // an event was dispatched
	EnDropped                   // and why
	EnDeferred                  // paused node
	EnSent
	EnDuplicated
	EnTimerSet
	EnTimerCancelled
	EnDurableWrite
	EnSync
	EnCrash
	EnRestart
	EnDiskWiped
	EnPause
	EnResume
	EnPartition
	EnHealed
	EnBuggify
	EnNote
	EnFault
)

func (k EntryKind) String() string {
	switch k {
	case EnEvent:
		return "event"
	case EnDropped:
		return "dropped"
	case EnDeferred:
		return "deferred"
	case EnSent:
		return "sent"
	case EnNote:
		return "note"
	case EnCrash:
		return "crash"
	case EnRestart:
		return "restart"
	case EnPause:
		return "pause"
	case EnResume:
		return "resume"
	case EnFault:
		return "fault"
	default:
		return "unknown"
	}
}

func (e Entry) String() string {
	s := fmt.Sprintf("[%s] %s", e.Kind, e.Event)
	if e.Reason != "" {
		s += "  (" + e.Reason + ")"
	}
	return s
}

type Trace struct {
	h       hash.Hash64 // FNV-1a from hash/fnv
	buf     []byte      // scratch for hashing, avoids per-event alloc
	level   Level
	entries []Entry

	keep  int
	head  int    // ring write position
	count uint64 // total entries ever recorded; entries may hold fewer
	// seq   uint64 // per-entry counter; distinct from Event.Seq, which several
	// entries can share when one event drains many effects
}

type Entry struct {
	Kind   EntryKind
	At     Time
	Seq    uint64
	Parent uint64
	Source int
	Target int
	Reason string
	Event  Event
}

const KeepAll = -1

type Level uint8

const (
	TraceOff Level = iota
	TraceHashEvents
	TraceHashEventsAndState
)

func NewTrace(level Level, keep int) *Trace {
	t := &Trace{
		h:     fnv.New64a(),
		buf:   make([]byte, 0, 64),
		level: level,
		keep:  keep,
	}
	if keep > 0 {
		t.entries = make([]Entry, keep)
	}
	return t
}

func (t *Trace) hashEvents(e Event) {
	t.buf = t.buf[:0]
	t.buf = binary.LittleEndian.AppendUint64(t.buf, uint64(e.At))
	t.buf = binary.LittleEndian.AppendUint64(t.buf, e.Seq)
	t.buf = append(t.buf, byte(e.Kind))
	t.buf = binary.LittleEndian.AppendUint64(t.buf, uint64(int64(e.Target)))
	t.buf = binary.LittleEndian.AppendUint64(t.buf, uint64(int64(e.Source)))
	t.buf = binary.LittleEndian.AppendUint64(t.buf, e.Parent)
	t.h.Write(t.buf)

	if e.Payload != nil {
		hp, ok := e.Payload.(Hashable)
		if !ok {
			panic(fmt.Sprintf("sim: payload %T does not implement Hashable (seq=%d)",
				e.Payload, e.Seq))
		}
		hp.HashInto(t.h)
	}
}

func (t *Trace) storeEntry(e Entry) {
	// t.seq++
	// e.Seq = t.seq
	t.count++

	switch t.keep {
	case 0:
		return
	case KeepAll:
		t.entries = append(t.entries, e)
	default:
		t.entries[t.head] = e
		t.head = (t.head + 1) % t.keep
	}
}

func (t *Trace) Record(e Event) {
	t.emit(EnEvent, e, "")
}

func (t *Trace) Sum() uint64 {
	return t.h.Sum64()
}

func (t *Trace) Len() int {
	return len(t.entries)
}

func (t *Trace) Entries() []Entry {
	if t.keep != KeepAll && t.keep > 0 && t.count >= uint64(t.keep) {
		out := make([]Entry, t.keep)
		out = append(out, t.entries[t.head:]...)
		out = append(out, t.entries[:t.head]...)
	}
	if t.keep > 0 {
		return t.entries[:t.count] // ring not full
	}
	return t.entries
}

func (t *Trace) Dump(w io.Writer) error {
	for _, e := range t.entries {
		if _, err := fmt.Fprintln(w, e); err != nil {
			return err
		}
	}
	return nil
}

func (t *Trace) Filter(pred func(Entry) bool) []Entry {
	out := make([]Entry, 0, len(t.Entries()))
	for _, en := range t.Entries() {
		if pred(en) {
			out = append(out, en)
		}
	}
	return out
}

// Dropped notes an event that was popped but never ran, or a message that was
// never scheduled at all. Every early return in the run loop and in send()
// should call this, a drop that leaves no trace is the single thing that
// makes a failing run unreadable.
func (t *Trace) Dropped(e Event, reason string) {
	t.emit(EnDropped, e, reason)
}

// Deferred notes an event held back because its node is paused. Without it, a
// pause looks like a black hole followed by a burst from nowhere.
func (t *Trace) Deferred(e Event) {
	t.emit(EnDeferred, e, "")
}

// Sent notes a message leaving a node. Sends are effects, not events, so they
// never enter the heap and would otherwise vanish from the trace entirely.
//
// at and parent have to be passed in: Trace doesn't know the clock, and parent
// is what links the send back to the event whose handler emitted it.
func (t *Trace) Sent(at Time, parent uint64, from, to int, msg Message) {
	t.emit(EnSent, Event{
		At:      at,
		Parent:  parent,
		Source:  from,
		Target:  to,
		Payload: msg,
	}, "")
}

// Note records something with no event behind it: a crash, a restart, a
// partition, a buggify point firing.
//
// Notes do not fold into the hash. The fault event that caused one is already
// hashed, so hashing both would double-count, and it would mean adding a note
// call somewhere invalidates every stored corpus hash.
func (t *Trace) Note(kind EntryKind, at Time, target int, reason string) {
	if t.level == TraceOff {
		return
	}
	t.storeEntry(Entry{Kind: kind, At: at, Reason: reason, Event: Event{At: at, Target: target}})
}

// FoldState mixes a node's state into the hash. Called after the drain, once
// per node, only at TraceHashEventsAndState and above.
//
// This is what catches two runs that produce an identical event sequence while
// the nodes end up in different states.
func (t *Trace) FoldState(id int, d StateDigester) {
	if t.level < TraceHashEventsAndState {
		return
	}
	t.buf = t.buf[:0]
	t.buf = append(t.buf, 'S')
	t.buf = binary.LittleEndian.AppendUint64(t.buf, uint64(int64(id)))
	t.h.Write(t.buf)
	d.StateDigest(t.h)
}

func (t *Trace) emit(kind EntryKind, e Event, reason string) {
	if t.level == TraceOff {
		return
	}
	t.hashEvents(e)
	t.storeEntry(Entry{Kind: kind, At: e.At, Reason: reason, Event: e})
}
