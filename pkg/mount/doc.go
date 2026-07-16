// Package mount models Linux mount-table identity and exposes the narrow
// mount/unmount boundary used by the node service.
//
// Idempotency is based on live mount graph identity, never path existence. Node
// startup additionally resolves protected path trees and compares exact
// mountinfo device/root identities so lexically disjoint hostPath bind aliases
// and missing propagation fail before CSI serving. A foreign, aliased,
// replaced, or stacked target is an error and is never repaired by remounting
// or broad unmount loops.
package mount
