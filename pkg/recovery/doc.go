// Package recovery defines canonical checkpoint inventories and fail-closed
// same-cluster startup recovery.
//
// Its only Kubernetes mutation surface is deterministic create-only allocation
// reconstruction: either after conclusive allocation/PV absence from
// authenticated ownership, or after a current surviving PV generation and
// authenticated ownership agree exactly. It does not authorize takeover or
// infer storage ownership from directory names. Provider fencing and Lease
// approval remain separate mandatory boundaries.
package recovery
