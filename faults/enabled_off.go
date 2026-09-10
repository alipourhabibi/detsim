//go:build !simfaults

package faults

// Enabled is false, and it is a constant, so the compiler removes every
//
//	if faults.Enabled && ...
const Enabled = false
