// Package observability exposes bounded Prometheus metrics for the controller
// and node processes.
//
// The package is a trust boundary: metric label values are not accepted as
// arbitrary strings. Configured pools and parents are registered once, while
// lifecycle states, provider operations, CSI operations, and status codes use
// closed enumerations. Logical-volume IDs, node IDs, paths, request names, and
// other workload-controlled identities therefore cannot become labels.
package observability
