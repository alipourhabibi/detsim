package sim

import (
	"fmt"
	"io"
	"slices"
)

var (
	_ Fault = partitionFault{}
	_ Fault = isolateFault{}
	_ Fault = healFault{}
	_ Fault = resetConnectionFault{}
	_ Fault = lossLinkFault{}
)

type partitionFault struct {
	A []int
	B []int
}

func NewPartitionFault(a, b []int) Fault {
	sa := slices.Sorted(slices.Values(a))
	sb := slices.Sorted(slices.Values(b))
	if len(sa) > 0 && len(sb) > 0 && sa[0] > sb[0] {
		sa, sb = sb, sa
	}
	return partitionFault{A: sa, B: sb}
}

func (f partitionFault) Apply(s *Sim) {
	s.lastRule = s.network.partition(f.A, f.B)
	s.trace.Note(EnPartition, s.now, -1,
		fmt.Sprintf("partition %v|%v rule=%d", f.A, f.B, s.lastRule))
}

func (f partitionFault) HashInto(w io.Writer) {
	hashInts(w, 'P', f.A...)
	hashInts(w, '|', f.B...)
}

func (f partitionFault) Equal(other any) bool {
	o, ok := other.(partitionFault)
	return ok && slices.Equal(f.A, o.A) && slices.Equal(f.B, o.B)
}

func (f partitionFault) String() string {
	return fmt.Sprintf("partition %v|%v", f.A, f.B)
}

type isolateFault struct {
	Node int
}

func NewIsolateFault(node int) Fault {
	return isolateFault{Node: node}
}

func (f isolateFault) Apply(s *Sim) {
	s.lastRule = s.network.isolate(f.Node)
	s.trace.Note(EnPartition, s.now, f.Node,
		fmt.Sprintf("isolate %d rule=%d", f.Node, s.lastRule))
}

func (f isolateFault) HashInto(w io.Writer) {
	hashInts(w, 'I', f.Node)
}

func (f isolateFault) String() string {
	return fmt.Sprintf("isolate %d", f.Node)
}

func (f isolateFault) Equal(other any) bool {
	o, ok := other.(isolateFault)
	return ok && f.Node == o.Node
}

type healFault struct {
	ID RuleID
}

func NewHealFault(id RuleID) Fault {
	return healFault{
		ID: id,
	}
}

func (f healFault) Apply(s *Sim) {
	s.network.heal(f.ID)
}

func (f healFault) HashInto(w io.Writer) {
	hashInts(w, 'H')
}

func (f healFault) String() string {
	return "heal"
}

func (f healFault) Equal(other any) bool {
	_, ok := other.(healFault)
	return ok
}

type lossLinkFault struct {
	From, To int
	DropPPM  uint32
}

func NewDropLinkFault(from, to int, dropPPM uint32) Fault {
	return lossLinkFault{From: from, To: to, DropPPM: dropPPM}
}

func (f lossLinkFault) Apply(s *Sim) {
	s.network.setLoss(f.From, f.To, f.DropPPM)
}

func (f lossLinkFault) HashInto(w io.Writer) {
	hashInts(w, 'D', f.From, f.To, int(f.DropPPM))
}

func (f lossLinkFault) String() string {
	return fmt.Sprintf("drop %d->%d to %d%%", f.From, f.To, f.DropPPM)
}

func (f lossLinkFault) Equal(other any) bool {
	o, ok := other.(lossLinkFault)
	return ok && f == o
}

type duplicateLinkFault struct {
	From, To     int
	DuplicatePPM uint32
}

func NewDuplicateLinkFault(from, to int, duplicatePPM uint32) Fault {
	return duplicateLinkFault{From: from, To: to, DuplicatePPM: duplicatePPM}
}

func (f duplicateLinkFault) Apply(s *Sim) {
	s.network.setDup(f.From, f.To, f.DuplicatePPM)
}

func (f duplicateLinkFault) HashInto(w io.Writer) {
	hashInts(w, 'U', f.From, f.To, int(f.DuplicatePPM))
}

func (f duplicateLinkFault) String() string {
	return fmt.Sprintf("duplicate %d->%d to %d%%", f.From, f.To, f.DuplicatePPM)
}

func (f duplicateLinkFault) Equal(other any) bool {
	o, ok := other.(duplicateLinkFault)
	return ok && f == o
}

type resetConnectionFault struct {
	From int
	To   int
}

func NewResetConnectionFault(from, to int) Fault {
	return resetConnectionFault{From: from, To: to}
}

func (f resetConnectionFault) Apply(s *Sim) {
	s.network.resetConnection(f.From, f.To)
}

func (f resetConnectionFault) HashInto(w io.Writer) {
	hashInts(w, 'X', f.From, f.To)
}

func (f resetConnectionFault) String() string {
	return fmt.Sprintf("reset connection %d->%d ", f.From, f.To)
}

func (f resetConnectionFault) Equal(other any) bool {
	o, ok := other.(resetConnectionFault)
	return ok && f == o
}

// blockLinkFault stops traffic one way, from -> to.
type blockLinkFault struct{ From, To int }

func (f blockLinkFault) Apply(s *Sim) {
	s.lastRule = s.network.blockDirection([]int{f.From}, []int{f.To})
	s.trace.Note(EnFault, s.now, -1, fmt.Sprintf("block %d->%d rule=%d", f.From, f.To, s.lastRule))
}

func (f blockLinkFault) HashInto(w io.Writer) {
	hashInts(w, 'B', f.From, f.To)
}

func (f blockLinkFault) String() string {
	return fmt.Sprintf("block(%d->%d)", f.From, f.To)
}

func (f blockLinkFault) Equal(other any) bool {
	o, ok := other.(blockLinkFault)
	return ok && f == o
}
