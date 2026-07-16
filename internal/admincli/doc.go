// Package admincli implements the bounded in-container csi-admin client used
// through an operator-authorized kubectl exec. It has no Kubernetes authority:
// the future operator-side orchestrator owns kubeconfig use and multi-Pod work.
package admincli
