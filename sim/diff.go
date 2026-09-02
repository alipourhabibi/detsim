package sim

import (
	"fmt"
	"io"
	"strings"
)

type Divergence struct {
	Index    int
	A, B     Entry
	AMissing bool
	BMissing bool
}

// First index where two full traces disagree. nil if identical.
func FirstDivergence(a, b *Trace) *Divergence {
	ea, eb := a.Entries(), b.Entries()
	minEntries := min(len(ea), len(eb))
	for index := range minEntries {
		if !entriesEqual(ea[index], eb[index]) {
			return &Divergence{
				Index: index,
				A:     ea[index],
				B:     eb[index],
			}
		}
	}
	if len(ea) != len(eb) {
		d := &Divergence{Index: minEntries}
		if len(ea) > minEntries {
			d.A = ea[minEntries]
			d.BMissing = true
		}
		if len(eb) > minEntries {
			d.B = eb[minEntries]
			d.AMissing = true
		}
		return d
	}
	return nil
}

func (d *Divergence) Report(w io.Writer, ea, eb []Entry, ctx int) error {

	var sb strings.Builder
	fmt.Fprintf(&sb, "diverged at index %d\n\n", d.Index)

	fmt.Fprintln(&sb, "shared prefix:")
	for i := max(d.Index-ctx, 0); i < d.Index; i++ {
		fmt.Fprintf(&sb, "  %s\n", ea[i])
	}

	fmt.Fprintln(&sb, "\ndiverging entry:")
	fmt.Fprintf(&sb, "  A: %s\n", side(d.A, d.AMissing))
	fmt.Fprintf(&sb, "  B: %s\n", side(d.B, d.BMissing))

	fmt.Fprintln(&sb, "\nafter:")
	for i := d.Index + 1; i <= d.Index+ctx; i++ {
		fmt.Fprintf(&sb, "  A: %s\n  B: %s\n", at(ea, i), at(eb, i))
	}

	_, err := io.WriteString(w, sb.String())
	return err
}

func side(e Entry, missing bool) string {
	if missing {
		return "<end of trace>"
	}
	return e.String()
}

func at(es []Entry, i int) string {
	if i < 0 || i >= len(es) {
		return "<end>"
	}
	return es[i].String()
}

// Same hash? identical. Same count, different hash? content diverged.
// Different count? control flow diverged. Different hunts.
func CompareRuns(a, b *Sim) string {
	if a.trace.Sum() == b.trace.Sum() {
		return "identical"
	}
	if a.events == b.events {
		return "content diverged"
	}
	return "flow diverged"
}

func entriesEqual(x, y Entry) bool {
	// Kind matters: the same event dropped in one run and dispatched in the
	// other has an identical Event and is very much not equal.
	//
	// Reason does not: it is prose for humans, and it is deliberately left out
	// of the hash for the same reason.
	if x.Kind != y.Kind || x.At != y.At {
		return false
	}
	return eventsEqual(x.Event, y.Event)
}

func eventsEqual(x, y Event) bool {
	if x.At != y.At || x.Seq != y.Seq || x.Kind != y.Kind ||
		x.Target != y.Target || x.Source != y.Source || x.Parent != y.Parent {
		return false
	}
	if x.Payload == nil || y.Payload == nil {
		return x.Payload == nil && y.Payload == nil
	}
	return x.Payload.(Hashable).Equal(y.Payload)
}
