package sim

import (
	"slices"
)

type Store interface {
	Get(key string) ([]byte, bool)
	Put(key string, value []byte)
	Delete(key string)
	Keys() []string // sorted, always
	Sync()
	Rollback() // drop everything written since the last Sync
	Wipe()     // disk loss; faults only, never exposed to a harness
}

// undo remembers what a key looked like before the first write since the last
// Sync. Only the first write per key is recorded.
type undo struct {
	value   []byte
	existed bool
}

type defaultStore struct {
	data     map[string][]byte
	unsynced map[string]undo
}

func newDurable() Store {
	return &defaultStore{
		data:     make(map[string][]byte),
		unsynced: make(map[string]undo),
	}
}

func (d *defaultStore) Get(key string) ([]byte, bool) {
	v, ok := d.data[key]
	if !ok {
		return nil, false
	}
	return slices.Clone(v), true
}

func (d *defaultStore) Put(key string, value []byte) {
	d.record(key)
	d.data[key] = slices.Clone(value)
}

func (d *defaultStore) Delete(key string) {
	d.record(key)
	delete(d.data, key)
}

func (d *defaultStore) Keys() []string {
	out := make([]string, 0, len(d.data))
	for k := range d.data {
		out = append(out, k)
	}
	slices.Sort(out)
	return out
}

func (d *defaultStore) Sync() {
	clear(d.unsynced)
}

func (d *defaultStore) Rollback() {
	for key, u := range d.unsynced {
		if u.existed {
			d.data[key] = u.value
		} else {
			delete(d.data, key)
		}
	}
	clear(d.unsynced)
}

func (d *defaultStore) Wipe() {
	clear(d.data)
	clear(d.unsynced)
}

// record saves the pre-write state of key, once per Sync window.
func (d *defaultStore) record(key string) {
	if _, seen := d.unsynced[key]; seen {
		return
	}
	v, ok := d.data[key]
	d.unsynced[key] = undo{value: v, existed: ok}
}
