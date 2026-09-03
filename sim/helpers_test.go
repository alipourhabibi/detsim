package sim

import (
	"bytes"
	"io"
	"testing"
)

type testMsg struct {
	N int64
}

func (m testMsg) HashInto(w io.Writer) {
	h := hashTag(w, 'T')
	h.i64(m.N)
}

func (m testMsg) Equal(other any) bool {
	o, ok := other.(testMsg)
	return ok && o == m
}

func testConfig(seed uint64) Config {
	return Config{
		Seed:       seed,
		MaxEvents:  100_000,
		TraceLevel: TraceHashEvents,
		TraceKeep:  KeepAll,
	}
}

func noop(int, Deps) Handler { return NoOpHandler{} }

// stable returns a factory that hands back the SAME handler instance for a
// given id on every build, including across restarts.
//
// Deliberately unrealistic: a real node gets fresh volatile state on restart,
// which is the whole point of the durable/volatile split. Reusing the instance
// is how a test observes what happened across a restart boundary, so these
// handlers must only accumulate observations, never protocol state.
func stable[T Handler](made map[int]T, mk func(id int, d Deps) T) NodeFactory {
	return func(id int, d Deps) Handler {
		h, ok := made[id]
		if !ok {
			h = mk(id, d)
			made[id] = h
		}
		return h
	}
}

func hashOf(h Hashable) string {
	var b bytes.Buffer
	h.HashInto(&b)
	return b.String()
}

func droppedFor(s *Sim, reason string) int {
	return len(s.Trace().Filter(func(e Entry) bool {
		return e.Kind == EnDropped && e.Reason == reason
	}))
}

func delayedConfig(seed uint64, d Duration) Config {
	cfg := testConfig(seed)
	cfg.NetworkConfig = NetworkConfig{
		Delay: DelaySpec{Kind: DelayConstant, Base: d},
	}
	return cfg
}

func wantPanic(t *testing.T, what string) func() {
	t.Helper()
	return func() {
		if recover() == nil {
			t.Fatalf("%s did not panic", what)
		}
	}
}
