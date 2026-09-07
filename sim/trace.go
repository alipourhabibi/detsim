package sim

import (
	"encoding/binary"
	"fmt"
	"hash"
	"hash/fnv"
	"io"
	"strconv"
	"strings"
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

// StateReporter gives a short human-readable snapshot of what a node currently
// believes: its role, its term, who it thinks the leader is.
type StateReporter interface {
	StateString() string
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
	EnRollback // unsynced writes thrown away by a crash
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
	EnState // a node changed what it believes
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
	case EnDuplicated:
		return "duplicated"
	case EnTimerSet:
		return "timerSet"
	case EnTimerCancelled:
		return "timerCancelled"
	case EnDurableWrite:
		return "durableWrite"
	case EnSync:
		return "sync"
	case EnRollback:
		return "rollback"
	case EnCrash:
		return "crash"
	case EnRestart:
		return "restart"
	case EnDiskWiped:
		return "diskWiped"
	case EnPause:
		return "pause"
	case EnResume:
		return "resume"
	case EnPartition:
		return "partition"
	case EnHealed:
		return "healed"
	case EnBuggify:
		return "buggify"
	case EnNote:
		return "note"
	case EnFault:
		return "fault"
	case EnState:
		return "state"
	default:
		return "unknown"
	}

}

func (e Entry) String() string {
	node := e.Target
	if e.Kind == EnSent || e.Kind == EnDuplicated {
		node = e.Source // the sender is the one doing something
	}
	nodeStr := "-"
	if node >= 0 {
		nodeStr = strconv.Itoa(node)
	}

	var what string
	switch e.Kind {
	case EnEvent, EnDropped, EnDeferred:
		what = e.Event.Describe()
	case EnSent, EnDuplicated:
		what = fmt.Sprintf("%v -> %d", e.Event.Payload, e.Target)
	default:
		what = e.Reason
	}

	out := fmt.Sprintf("t=%-6d #%-4d n%-3s %-15s %s",
		e.At, e.Seq, nodeStr, e.Kind, what)

	// Only EnDropped and EnDeferred carry a reason separate from their content.
	if e.Reason != "" && (e.Kind == EnDropped || e.Kind == EnDeferred) {
		out += "  (" + e.Reason + ")"
	}
	return out
}

// DumpStorage prints only the entries that decide durability: writes, syncs,
// rollbacks, wipes, crashes and restarts. For an acknowledge-before-durable
// bug this is the entire investigation.
func (t *Trace) DumpStorage(w io.Writer) error {
	for _, e := range t.Entries() {
		switch e.Kind {
		case EnDurableWrite, EnSync, EnRollback, EnDiskWiped, EnCrash, EnRestart:
			if _, err := fmt.Fprintln(w, e); err != nil {
				return err
			}
		}
	}
	return nil
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

	lastState map[int]string // last reported state per node, for change detection
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
		h:         fnv.New64a(),
		buf:       make([]byte, 0, 64),
		level:     level,
		keep:      keep,
		lastState: map[int]string{},
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
		e.Payload.HashInto(t.h)
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
	if t.keep > 0 && t.count >= uint64(t.keep) {
		return t.keep
	}
	return int(t.count)
}

func (t *Trace) Entries() []Entry {
	if t.keep <= 0 {
		return t.entries
	}
	if t.count < uint64(t.keep) { // ring not full yet
		return t.entries[:t.count]
	}
	out := make([]Entry, 0, t.keep)
	out = append(out, t.entries[t.head:]...)
	out = append(out, t.entries[:t.head]...)
	return out
}

func (t *Trace) Dump(w io.Writer) error {
	for _, e := range t.Entries() {
		if _, err := fmt.Fprintln(w, e); err != nil {
			return err
		}
	}
	return nil
}

func (t *Trace) Filter(pred func(Entry) bool) []Entry {
	all := t.Entries()
	out := make([]Entry, 0, len(all))
	for _, en := range all {
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

// Note records something with no event behind it: a write, a crash, a pause.
// seq is the Seq of the event whose handler caused it, so effects group with
// their cause in the output.
func (t *Trace) Note(kind EntryKind, at Time, node int, detail string, seq uint64) {
	if t.level == TraceOff {
		return
	}
	t.storeEntry(Entry{
		Kind:   kind,
		At:     at,
		Seq:    seq,
		Target: node,
		Source: -1, // no peer involved
		Reason: detail,
	})
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
	t.storeEntry(Entry{
		Kind:   kind,
		At:     e.At,
		Seq:    e.Seq,
		Parent: e.Parent,
		Source: e.Source,
		Target: e.Target,
		Reason: reason,
		Event:  e,
	})
}

// Lanes writes a space-time diagram: one column per node, time downward,
// messages as arrows between columns.
//
// This is the standard way to draw a distributed run. Reading down a column
// gives one node's history. Reading an arrow gives a causal link.
func (t *Trace) Lanes(w io.Writer, nodes []int) error {
	col := make(map[int]int, len(nodes))
	for i, id := range nodes {
		col[id] = i
	}
	const width = 14

	// header
	fmt.Fprintf(w, "%-8s", "time")
	for _, id := range nodes {
		fmt.Fprintf(w, "%-*s", width, fmt.Sprintf("node %d", id))
	}
	fmt.Fprintln(w)

	for _, e := range t.Entries() {
		line := []rune(strings.Repeat(" ", width*len(nodes)))

		put := func(c int, s string) {
			for i, r := range []rune(s) {
				if c*width+i < len(line) {
					line[c*width+i] = r
				}
			}
		}

		switch e.Kind {
		case EnEvent:
			if e.Event.Kind == EvDeliver {
				// arrow from sender's column to receiver's column
				from, to := col[e.Event.Source], col[e.Event.Target]
				lo, hi := min(from, to), max(from, to)
				for c := lo*width + 6; c < hi*width+6; c++ {
					line[c] = '-'
				}
				if to > from {
					line[hi*width+6] = '>'
				} else {
					line[lo*width+6] = '<'
				}
				put(to, fmt.Sprintf("%v", e.Event.Payload))
			} else {
				put(col[e.Event.Target], "* "+e.Event.Name)
			}

		case EnDurableWrite:
			put(col[e.Target], "write "+e.Reason)
		case EnSync:
			put(col[e.Target], "SYNC")
		case EnRollback:
			put(col[e.Target], "ROLLBACK")
		case EnCrash:
			put(col[e.Target], "## CRASH")
		case EnRestart:
			put(col[e.Target], "## up")
		case EnPause, EnResume, EnDiskWiped:
			put(col[e.Target], e.Kind.String())
		case EnDropped:
			put(col[e.Event.Target], "x "+e.Reason)
		default:
			continue // sends are implied by their delivery
		}

		fmt.Fprintf(w, "%-8d%s\n", e.At, strings.TrimRight(string(line), " "))
	}
	return nil
}

// Only the entries that carry the story: deliveries, durability, lifecycle.
// Retries are collapsed, because twenty identical Acquire arrows say nothing
// that one arrow and a count does not.
func (t *Trace) Mermaid(w io.Writer, nodes []int) error {
	fmt.Fprintln(w, "sequenceDiagram")
	for _, id := range nodes {
		fmt.Fprintf(w, "  participant n%d\n", id)
	}

	var (
		lastAt    Time = -1
		repeatMsg string
		repeatN   int
	)

	flush := func() {
		if repeatN > 1 {
			fmt.Fprintf(w, "  Note over n0: (%s x%d)\n", repeatMsg, repeatN)
		}
		repeatN, repeatMsg = 0, ""
	}

	for _, e := range t.Entries() {
		// A time marker whenever the clock moves, so gaps are visible.
		if e.At != lastAt {
			flush()
			fmt.Fprintf(w, "  Note over n%d: t=%d\n", nodes[0], e.At)
			lastAt = e.At
		}

		switch e.Kind {
		case EnEvent:
			if e.Event.Kind != EvDeliver {
				continue
			}
			label := fmt.Sprintf("%v", e.Event.Payload)

			// Collapse a run of identical retries.
			if label == repeatMsg {
				repeatN++
				continue
			}
			flush()
			repeatMsg, repeatN = label, 1

			fmt.Fprintf(w, "  n%d->>n%d: %s\n", e.Event.Source, e.Event.Target, label)

		case EnDurableWrite:
			flush()
			fmt.Fprintf(w, "  Note right of n%d: write %s\n", e.Target, e.Reason)
		case EnSync:
			flush()
			fmt.Fprintf(w, "  Note right of n%d: SYNC\n", e.Target)
		case EnRollback:
			flush()
			fmt.Fprintf(w, "  Note over n%d: ROLLBACK %s\n", e.Target, e.Reason)
		case EnCrash:
			flush()
			fmt.Fprintf(w, "  Note over n%d: CRASH\n", e.Target)
		case EnRestart:
			flush()
			fmt.Fprintf(w, "  Note over n%d: restart\n", e.Target)
		case EnTimerSet:
			if e.Reason == "lease" { // entering the critical section
				flush()
				fmt.Fprintf(w, "  activate n%d\n", e.Target)
			}
		case EnTimerCancelled:
			if e.Reason == "lease" {
				flush()
				fmt.Fprintf(w, "  deactivate n%d\n", e.Target)
			}
		}
	}
	flush()
	return nil
}

// FoldDown folds a marker for a crashed node.
//
// A crashed node has no handler and so no state to digest. Skipping it would
// make a run where node 2 is down hash the same as one where node 2 is up with
// empty state, so a fixed marker goes in instead.
func (t *Trace) FoldDown(node int) {
	if t.level < TraceHashEventsAndState {
		return
	}
	var buf [9]byte
	buf[0] = 'D' // down
	binary.LittleEndian.PutUint64(buf[1:], uint64(node))
	t.h.Write(buf[:])
}

// A snapshot every event would drown the trace. What is worth reading is the
// transition: the instant a node changed its mind.
func (t *Trace) ReportState(at Time, seq uint64, node int, state string) {
	if t.level == TraceOff {
		return
	}
	if t.lastState == nil {
		t.lastState = map[int]string{}
	}
	if t.lastState[node] == state {
		return
	}
	prev := t.lastState[node]
	t.lastState[node] = state

	detail := state
	if prev != "" {
		detail = prev + " -> " + state
	}
	t.storeEntry(Entry{
		Kind:   EnState,
		At:     at,
		Seq:    seq,
		Target: node,
		Source: -1,
		Reason: detail,
	})
}
