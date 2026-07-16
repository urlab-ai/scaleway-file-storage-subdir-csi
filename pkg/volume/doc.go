// Package volume defines the immutable logical-volume identity, wire context,
// durable lifecycle schemas, and compatibility rules.
//
// Values in this package cross the CSI, Kubernetes, and filesystem trust
// boundaries. Parsers therefore accept only the closed v1 formats and never
// infer missing identity from current configuration.
package volume
