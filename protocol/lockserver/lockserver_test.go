package lockserver

import "testing"

type sent struct {
	to  int
	msg Msg
}

type fakeNet struct{ out []sent }

func (n *fakeNet) Send(to int, msg Msg) { n.out = append(n.out, sent{to, msg}) }

type fakeDisk struct{ kv map[string][]byte }

func (d *fakeDisk) Get(k string) ([]byte, bool) { v, ok := d.kv[k]; return v, ok }
func (d *fakeDisk) Put(k string, v []byte)      { d.kv[k] = append([]byte(nil), v...) }
func (d *fakeDisk) Sync()                       {}

type fakeTimers struct{}

func (fakeTimers) SetTimer(string, int64) {}
func (fakeTimers) CancelTimer(string)     {}

// client 1 releases attempt 5, asks again with attempt 6 and is granted. the
// old release arriving late must not take the lock away from attempt 6.
func TestLateReleaseIsIgnored(t *testing.T) {
	s := NewServer(0, &fakeNet{}, &fakeDisk{kv: map[string][]byte{}}, nil)
	s.OnAcquire(1, Acquire{Attempt: 5})
	s.OnAcquire(1, Acquire{Attempt: 6}) // Release#5 still on the wire
	s.OnRelease(1, Release{Attempt: 5})
	if got := s.Owner(); got != 1 {
		t.Fatalf("owner = %d after a late release, want 1", got)
	}
	s.OnRelease(1, Release{Attempt: 6})
	if got := s.Owner(); got != NoHolder {
		t.Fatalf("owner = %d after the right release, want none", got)
	}
}

// a restarted client must not reuse attempt numbers from before the crash, or
// a late Granted from the old run would look like an answer to the new one
func TestAttemptsDontRepeatAfterRestart(t *testing.T) {
	disk := &fakeDisk{kv: map[string][]byte{}}
	newClient := func() *Client {
		return NewClient(1, 0, 100, 300, &fakeNet{}, fakeTimers{}, disk, nil)
	}
	c := newClient()
	c.Start()
	first := c.Attempt()

	c = newClient() // crash and restart: memory gone, disk kept
	c.Start()
	if c.Attempt() <= first {
		t.Fatalf("attempt after restart is %d, before it was %d", c.Attempt(), first)
	}
}
