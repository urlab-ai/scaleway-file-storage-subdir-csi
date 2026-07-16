// Package kindfake provides the explicitly development-only CSI endpoint used
// by the mandatory kind chart-install suite. It never calls Scaleway or
// Kubernetes APIs and is not linked into either released executable.
//
// The fake controller derives stable handles and immutable volume context from
// each CreateVolume request. The Linux node endpoint uses real bind mounts
// below the disposable kind node's kubelet tree so the suite exercises kubelet,
// registrar, sidecar, RBAC, socket, and mount-propagation wiring. It is not a
// storage emulator and must never be enabled in a production chart render.
package kindfake
