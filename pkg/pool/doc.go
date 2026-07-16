// Package pool validates configured parent filesystems and performs exact
// logical-capacity, physical-free-space, and deterministic placement
// calculations.
//
// Capacity decisions consume only authoritative provider sizes, allocation
// records, and fresh statfs observations. Arithmetic is checked so a provider
// anomaly or configuration overflow cannot wrap into apparent free capacity.
package pool
