# Troubleshooting

The driver fails closed. Preserve the error, request ID, Lease
UID/resourceVersion, parent/node identity, and timestamps before retrying. Do
not repair durable records manually.

## Logs and parent Events

Driver containers emit one JSON object per line. Start with
`csi_operation`, `grpc_code`, and `duration_ms`, then correlate the validated
`logical_volume_id`, `pool`, `parent_filesystem_id`, `node_id`, and sanitized
paths when present. Provider failures use `provider_operation`; parent-local
failures use `degradation_reason`. Values are flattened and bounded so one
untrusted response cannot forge or flood records.

The controller emits `ParentDegraded` Warning and `ParentRecovered` Normal
Events against its Pod when a configured parent's condition changes. Inspect
them with `kubectl events -n <driver-namespace> --for pod/<controller-pod>`.
An Event API failure is logged but does not turn a healthy unrelated parent
into a storage outage.

## Controller unready

Check configuration/Secrets; installation and `kube-system` UID; fixed Lease
holder/evidence; parent status and complete attachments; parent claims; Ready
Nodes/node Pods/CSINodes and node configuration generation; allocation/PV/
ownership reconciliation; and active checkpoint/uninstall/bootstrap state. Do
not delete an uncleared Lease.

## Unknown attachment

Identify whether it belongs to an official CSI driver, manual mount, old node,
or deleted-Instance orphan. Stop its owner and use only the explicit offline
detach path. A deleted Instance still present in regional inventory requires
Scaleway support; retain exact IDs.

## Parent status or size

`creating`, `updating`, or unknown is retryable but non-serving for dependent
operations. `error` requires operator/provider action. Healthy existing mounts
are not torn down for API outages. A smaller observed size enters
`critical-size-regression`; it clears only after an authoritative size reaches
the previous accepted value within the current controller generation. This is
a process-local diagnostic guard; after controller restart, fresh provider
metadata, complete allocation accounting, and `statfs` remain the authoritative
placement checks.

## Create/Delete/GC blocked

Create may be blocked by capacity/free-space reserve, parent status, node
compatibility/budget, foreign attachment, incompatible replay, or ownership
reconciliation. Retry the identical CSI request; never delete the deterministic
allocation to force replacement.

Delete/GC remains blocked by a PV/VolumeAttachment, published fence, one-sided
record, operation/path conflict, ambiguous mount, or missing ownership proof.
Use normal unpublish or conclusive provider fencing. Never remove archive,
quarantine, metadata, or tombstones by hand.

## Node target conflict

Preserve `/proc/self/mountinfo`. Foreign, aliased, replaced, stacked, wrong-root,
wrong-device, capability, or read-only mismatch is intentionally rejected. Do
not bypass it with lazy/recursive unmount; drain and resolve the foreign owner.
If the error reports unavailable `STATX_MNT_ID_UNIQUE`, the node kernel cannot
provide the non-reusable generation required for exact unmount. Do not weaken
this check or fall back to the reusable mountinfo ID; use a release-qualified
kernel line before scheduling workloads on the node.

## Checkpoint/recovery blocked

Prepare rejects active mutations/bootstrap, a non-Ready journal set, any
Pending/missing/malformed permanent journal, transitional state, execute-GC,
one-sided pairs, incomplete objects, or malformed/duplicate parent inventory.
Reconcile the exact predecessor first. A barrier owned by a failed request stays
closed until resume with that request. Resume resolves an exact Pending
reservation before lifecycle reconciliation; it never clears a divergent or
unavailable journal by assumption.

Recovery requires the exact completed package and immutable Secret, same
cluster/identity/chart/images, exact object and parent inventories, every old
Instance offline, clean attachments, and a fresh checkpoint-bound approval.
Newer/extra/missing state requires targeted manual recovery; it is not
overwritten.

For `checkpoint restore`, first verify that controller/node Pods and the node
DaemonSet are absent, the matching Deployment is absent or stably scaled to
zero, and the fixed Lease is absent or empty. A changed archive inode,
non-canonical tar, wrong identity Secret key, extra/conflicting journal or
allocation, or changed PV is
an input conflict, not a retryable absence. If execution stopped after some
permanent journal or allocation ConfigMaps were created, retry the identical
archive/request; never
create the checkpoint Secret manually. If the Secret already exists while an
allocation is missing, preserve all evidence and use targeted recovery.

## Parent decommission blocked

Keep the target configured as `draining` and rerun the same request in dry-run.
Remove listed Pods/PVCs/PVs/VolumeAttachments through normal Kubernetes flows
and clear published fences only through normal unpublish or conclusive provider
fencing. Detailed/nonterminal ownership and one-sided compact tombstones are
state errors, not directories to delete. During execute, a progress ConfigMap
owned by the exact controller Deployment is authoritative; retry the same
request ID after interruption. A different request, parent, runtime UID, or
workload UID must not adopt it. Do not manually detach, delete the DaemonSet, or
remove the parent from values before the completed target-only cleanup audit.

## CSI Pods fail before registration

If the driver reports an unsafe CSI socket directory, inspect the exact volume
inode and permissions. The root driver may only remove group/other write bits
from its own unchanged directory; it will not follow an alias or broaden
permissions. The socket itself must be mode `0666` for non-root sidecars.

The `node-driver-registrar` intentionally uses UID/GID 0 with
`privileged: false`, no capabilities, no escalation, and a read-only root so it
can create its socket in kubelet's root-owned shared `plugins_registry`.
Do not `chown` that shared directory: other CSI drivers use it. Provisioner,
attacher, and liveness sidecars run as UID/GID 65532.
