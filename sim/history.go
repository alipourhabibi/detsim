package sim

import (
	"cmp"
	"fmt"
	"slices"
	"strings"
)

// A history is what the clients saw: every operation they started, when they
// started it, and what came back.
//
// It is not the trace. The trace says what really happened inside the system.
// The history says only what could be observed from outside. Checks written
// against a history are the ones a real user could make.
//
// The simulator records. It does not know what your operations mean, so it
// stores whatever names and keys you give it. Deciding whether a history is
// correct is your harness's job.

type Outcome uint8

const (
	// Open means the client is still waiting. Every operation starts here.
	Open Outcome = iota

	// Ok means the answer came back and the operation succeeded.
	Ok

	// Unknown means the client never got an answer. This is not a failure. The
	// operation may have happened on the server or it may not, and the client
	// cannot tell.
	Unknown
)

func (o Outcome) String() string {
	switch o {
	case Open:
		return "open"
	case Ok:
		return "ok"
	case Unknown:
		return "unknown"
	default:
		return fmt.Sprintf("Outcome(%d)", uint8(o))
	}
}

// Op is one client operation.
type Op struct {
	ID   uint64 // counts up over the whole run, across all clients
	Node int
	Kind string // your name for it: "acquire", "read", "write"
	Key  uint64 // whatever you match replies by: an attempt or request number

	Invoked  Time // when the client sent it
	Returned Time // when the answer came back. Zero unless Outcome is Ok
	Outcome  Outcome

	Value any // optional. The argument or result, for checkers that need it
}

func (o Op) String() string {
	if o.Outcome == Ok {
		return fmt.Sprintf("op%-4d c%d %-10s key=%-4d %d..%d ok",
			o.ID, o.Node, o.Kind, o.Key, o.Invoked, o.Returned)
	}
	return fmt.Sprintf("op%-4d c%d %-10s key=%-4d %d..? %s",
		o.ID, o.Node, o.Kind, o.Key, o.Invoked, o.Outcome)
}

// History collects the operations of every client.
type History struct {
	ops    []Op
	open   map[opKey]int // where an unfinished op sits in ops
	nextID uint64
}

type opKey struct {
	client int
	key    uint64
}

func newHistory() *History {
	return &History{open: map[opKey]int{}}
}

// Invoke records that a node started an operation.
//
// Nothing is known about the result yet. The operation stays Open until
// Complete or Abandon is called with the same client and key, or until the
// client crashes, or until the run ends.
//
// Reusing a key that is still open abandons the old one first. That is almost
// always what you want: a client that reuses a request number has given up on
// the first.
func (h *History) Invoke(at Time, node int, kind string, key uint64) {
	if _, dup := h.open[opKey{node, key}]; dup {
		h.Abandon(node, key)
	}

	h.nextID++
	h.ops = append(h.ops, Op{
		ID:      h.nextID,
		Node:    node,
		Kind:    kind,
		Key:     key,
		Invoked: at,
		Outcome: Open,
	})
	h.open[opKey{node, key}] = len(h.ops) - 1
}

// InvokeValue is Invoke with an argument attached, for checkers that need to
// know what was written or read.
func (h *History) InvokeValue(at Time, client int, kind string, key uint64, v any) {
	h.Invoke(at, client, kind, key)
	h.ops[len(h.ops)-1].Value = v
}

// Complete records that the answer arrived.
//
// A reply with no matching open operation is ignored. That happens when an
// answer to an abandoned request finally turns up, and a protocol with a
// fencing check throws those away too.
func (h *History) Complete(at Time, client int, key uint64) {
	i, ok := h.open[opKey{client, key}]
	if !ok {
		return
	}
	h.ops[i].Returned = at
	h.ops[i].Outcome = Ok
	delete(h.open, opKey{client, key})
}

// CompleteValue is Complete with a result attached.
func (h *History) CompleteValue(at Time, client int, key uint64, v any) {
	i, ok := h.open[opKey{client, key}]
	if !ok {
		return
	}
	h.ops[i].Value = v
	h.Complete(at, client, key)
}

// Abandon records that the client stopped waiting.
//
// The operation becomes Unknown, not failed. The server may have done it. The
// client will never find out.
func (h *History) Abandon(client int, key uint64) {
	i, ok := h.open[opKey{client, key}]
	if !ok {
		return
	}
	h.ops[i].Outcome = Unknown
	delete(h.open, opKey{client, key})
}

// abandonClient marks every open operation of one client Unknown.
//
// The sim calls this when a node crashes. The client's memory is gone, so it
// can never match a reply again, and anything it was waiting for is now
// unknowable
func (h *History) abandonClient(client int) {
	for k, i := range h.open {
		if k.client == client {
			h.ops[i].Outcome = Unknown
			delete(h.open, k)
		}
	}
}

// Ops returns the operations in the order they were started.
//
// An operation still waiting is reported Unknown: at this moment the client
// has no answer, and that is all a checker may assume. The history itself is
// not changed, so an answer that arrives later still counts.
func (h *History) Ops() []Op {
	out := make([]Op, len(h.ops))
	copy(out, h.ops)
	for i := range out {
		if out[i].Outcome == Open {
			out[i].Outcome = Unknown
		}
	}
	return out
}

// Counts is a quick summary. A run with no Ok operations usually means the
// harness is not calling Complete.
func (h *History) Counts() (ok, unknown, open int) {
	for _, o := range h.ops {
		switch o.Outcome {
		case Ok:
			ok++
		case Unknown:
			unknown++
		case Open:
			open++
		}
	}
	return
}

func (h *History) Len() int {
	return len(h.ops)
}

func (h *History) String() string {
	var b strings.Builder
	for _, o := range h.ops {
		fmt.Fprintln(&b, o)
	}
	return b.String()
}

// Span is a window of time: one client was doing something from From to To.
//
// The history does not know what your operations mean. You tell it which kind
// opens a span and which kind closes it. The rest works for any protocol.
type Span struct {
	Client int
	From   Time
	To     Time // when it ended, or the end of the run
	Op     Op   // the operation that opened it
}

// Spans pairs operations into windows.
//
// open is the kind that starts a window. close is the kind that ends it. A
// window goes from the moment the open got its answer, to that same client's
// next close, or to the end of the run.
//
// Only Ok operations open a window. An Unknown may or may not have happened,
// so it proves nothing on its own.
//
// That makes this check weaker than it looks. If the server granted the lock
// and the reply was lost, the client never learned it, the operation is
// Unknown, and no window is built for it. A real violation hiding behind a lost
// reply will not be found here.
//
// There are three ways to treat Unknowns. This takes the first:
//
//	ignore them     what happens here. Cheap, and misses those bugs.
//	flag them       build the windows twice, once counting Unknowns as
//	                successes, and print the difference as a suspicion rather
//	                than a failure.
//	try both ways   ask whether any combination of happened and not-happened
//	                is legal. The search is exponential in the number of
//	                Unknowns, which is what a linearizability checker does
func (h *History) Spans(open, close string, endOfRun Time) []Span {
	var out []Span
	for i, o := range h.ops {
		if o.Kind != open || o.Outcome != Ok {
			continue
		}
		s := Span{Client: o.Node, From: o.Returned, To: endOfRun, Op: o}
		for _, later := range h.ops[i+1:] {
			if later.Node == o.Node && later.Kind == close {
				s.To = later.Invoked
				break
			}
		}
		out = append(out, s)
	}
	slices.SortFunc(out, func(a, b Span) int {
		return cmp.Compare(a.From, b.From)
	})
	return out
}

// Overlapping finds the first two windows from different clients that share
// any time. It returns false if there are none.
//
// Two clients doing the same thing at the same time is the bug a lock is
// supposed to stop. These two windows are the proof.
func Overlapping(spans []Span) (a, b Span, found bool) {
	for i := 1; i < len(spans); i++ {
		prev, cur := spans[i-1], spans[i]
		if cur.Client != prev.Client && cur.From < prev.To {
			return prev, cur, true
		}
	}
	return Span{}, Span{}, false
}

// Unclosed finds windows that never ended. A window that reaches the end of
// the run had no close after it.
//
// Careful: this alone does not mean a bug. The run may have stopped while a
// client was still working. The caller decides.
func Unclosed(spans []Span, endOfRun Time) []Span {
	var out []Span
	for _, s := range spans {
		if s.To == endOfRun {
			out = append(out, s)
		}
	}
	return out
}
