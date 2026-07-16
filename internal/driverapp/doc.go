// Package driverapp owns the closed process command line and the first runtime
// trust transition from untrusted flags and projected configuration into typed,
// component-specific startup state. CSI, Kubernetes, provider, and filesystem
// adapters are constructed only after this boundary succeeds.
package driverapp
