// Package strictjson decodes untrusted durable records using the closed v1
// schema rules.
//
// The standard JSON decoder accepts duplicate object keys and silently keeps a
// later value. That behavior is unsafe for ownership and lifecycle records:
// two readers could authenticate different meanings from the same bytes. This
// package rejects duplicate keys, invalid UTF-8, unknown fields, trailing data,
// and type mismatches before returning a typed value.
package strictjson
