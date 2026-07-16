// Package health implements the driver process's shallow liveness boundary.
//
// Liveness is deliberately independent from CSI readiness, controller
// leadership, provider availability, parent status, reconciliation, and mount
// state. The runtime event loop supplies a cheap heartbeat and may mark an
// unrecoverable internal failure. The HTTP handler reads only that cached
// process-local state and performs no external I/O.
package health
