//go:build simfaults

package faults

// Enabled is true when you build with:
//
//	go build -tags simfaults
//	go test  -tags simfaults ./...
//
// Now the branches are compiled in and the Injector decides.
const Enabled = true
