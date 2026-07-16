// Package e2ecleanup validates retained real-cloud E2E inventory and derives
// an exact-ID, non-authorizing cleanup review plan.
//
// It deliberately has no Scaleway or Kubernetes mutation backend. Its trust
// boundary is untrusted retained JSON: static creation provenance, complete
// run scope, fresh observation state, and the ordered uninstall preconditions
// must all validate before any resource is listed as eligible for a later
// explicitly approved deletion.
package e2ecleanup
