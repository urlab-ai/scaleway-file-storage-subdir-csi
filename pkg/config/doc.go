// Package config loads and validates the cross-component runtime configuration
// before a controller or node process can become ready.
//
// Helm projects a closed, size-bounded non-secret JSON document. Secret values
// are merged only through a narrow environment lookup: installation identity
// is retained as durable authority, controller credentials are presence-checked
// without storage, controller provider scope is cross-checked, and node mode
// rejects authenticated cloud credentials. Runtime validation remains the
// final fail-closed authority because restored state and live resources cannot
// be proven by chart rendering alone.
package config
