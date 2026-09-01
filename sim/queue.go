package sim

// Implements container/heap. Ordered by (At, Seq).
type eventQueue []Event

func (q eventQueue) Len() int {
	return len(q)
}

// At first, then Seq. Never anything else.
func (q eventQueue) Less(i, j int) bool {
	if q[i].At != q[j].At {
		return q[i].At < q[j].At
	}
	return q[i].Seq < q[j].Seq
}

func (q eventQueue) Swap(i, j int) {
	q[i], q[j] = q[j], q[i]
}

func (q *eventQueue) Push(x any) {
	*q = append(*q, x.(Event))
}

func (q *eventQueue) Pop() any {
	old := *q
	n := len(old)
	e := old[n-1]
	old[n-1] = Event{} // release the Payload reference
	*q = old[:n-1]
	return e
}

func (q eventQueue) Peek() (Event, bool) {
	if len(q) == 0 {
		return Event{}, false
	}
	return q[0], true
}
