// Package csiadapter translates the generated CSI protobuf API to the
// provider-independent driver state machines.
//
// This package is the untrusted wire boundary. It validates request shape,
// closed StorageClass parameters, capability semantics, and RPC-specific error
// policy before invoking a core. It never owns durable ordering, provider
// mutation, Kubernetes state, or filesystem safety decisions.
package csiadapter
