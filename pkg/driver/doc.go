// Package driver coordinates CSI-independent controller and node state
// machines. Thin protobuf adapters translate requests and gRPC status codes;
// durable ordering, provider calls, filesystem mutation, and recovery remain in
// these testable cores.
package driver
