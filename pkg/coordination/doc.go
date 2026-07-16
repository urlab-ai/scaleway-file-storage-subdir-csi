// Package coordination provides process-local mutation ordering and the typed,
// closed evidence schemas used by the fixed Kubernetes Lease protocol.
//
// The local primitives are context-aware and bounded. They do not replace
// Kubernetes optimistic concurrency or provider fencing; they only serialize
// check-and-act sequences inside the single v1 controller or one node plugin.
package coordination
