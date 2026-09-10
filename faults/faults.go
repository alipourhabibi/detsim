// A crash at a random time almost never lands in the small window between two
// lines. So you mark the window instead:
//
//	s.storage.Put(key, value)
//	if faults.Enabled && s.faults.Fire("lockserver/skip-sync") {
//	    // do not sync. See what happens.
//		return
//	}
//	s.storage.Sync()
package faults

// Injector decides whether to take the bad path at a marked place.
//
// Use a package prefix so two protocols cannot pick the same name:
// "lockserver/skip-sync", not "skip-sync".
type Injector interface {
	Buggify(site string) bool
}

// NoOp never fires. This is what production uses.
type NoOp struct{}

func (NoOp) Buggify(string) bool {
	return false
}
