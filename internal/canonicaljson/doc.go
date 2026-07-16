// Package canonicaljson writes the deterministic JSON representation used by
// hashes and durable checksums.
//
// Callers remain responsible for supplying closed, typed payloads. The encoder
// disables HTML escaping because the v1 hash contract is over UTF-8 JSON
// strings rather than an HTML-safe projection. Go's JSON encoder sorts string
// map keys, emits base-10 integers, and does not add insignificant whitespace.
package canonicaljson
