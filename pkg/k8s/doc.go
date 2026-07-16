// Package k8s contains narrow Kubernetes state boundaries used by the driver.
//
// Allocation records are represented independently of client-go so their
// optimistic-concurrency and ambiguity semantics can be tested deterministically.
// The production adapter implements ConfigMapClient with client-go; runtime code
// never treats an unavailable, forbidden, or timed-out read as NotFound.
package k8s
