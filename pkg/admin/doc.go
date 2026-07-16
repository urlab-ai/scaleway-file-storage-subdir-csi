// Package admin defines the versioned local operator protocol shared by the
// controller and the released csi-admin binary.
//
// The bounded, length-prefixed strict JSON transport accepts only a
// controller-local Unix listener, negotiates every mutation independently,
// limits connection concurrency and I/O time, and joins all connection
// goroutines during shutdown. Command handlers still own leadership,
// quiescence, CAS, provider, and filesystem authorization; a successful
// handshake never itself authorizes a mutation.
package admin
