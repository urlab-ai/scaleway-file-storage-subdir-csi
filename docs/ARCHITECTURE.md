# Architecture and Trust Boundaries

This document is explanatory. [`SPECIFICATION.md`](SPECIFICATION.md) remains the
normative source of truth.

## Data path

The driver maps one Kubernetes PersistentVolume to one owned subdirectory of an
existing Scaleway File Storage parent:

```text
PVC / PV
  -> sfs1:<logical-volume-id>:<mapping-hash>
  -> permanent per-pool reservation journal
  -> persisted allocation ConfigMap
  -> parent-global claim + per-volume ownership record
  -> parent virtiofs mount on the workload node
  -> bind-mounted logical directory at the CSI staging path
  -> bind mount at the Pod target
```

The handle contains no filesystem path. The allocation record is the primary
mapping; PV attributes and filesystem ownership are independent recovery
evidence. A directory name alone never proves ownership.

## Controller and coordination

V1 runs one controller Pod with Deployment strategy `Recreate`. The driver is
privileged for parent mounts and lifecycle operations. Its CSI sidecars are
non-root and receive no Scaleway credentials.

Every mutation passes active Lease leadership, the process-wide mutation gate,
the ordered pool/volume/parent locks it needs, and the appropriate Kubernetes
CAS, provider inventory, mount identity, and ownership checks. The Lease
coordinates one process; it is not storage fencing. An uncleared holder needs a
conclusive provider fence and immutable operator approval.

Controller and node consume the same closed, non-secret runtime JSON projection
rendered by Helm. The decoder is size-bounded, rejects duplicate/unknown fields,
converts integer-second deadlines with overflow checks, validates the complete
pool/StorageClass model, validates the sorted release-qualified commercial-type
allowlist, and recomputes the node configuration generation. Production startup
requires that list to equal the independently linked binary release identity;
Helm values cannot acknowledge an untested Instance type.
Installation identity is merged from its external Secret. Controller provider
scope must match the independently projected environment exactly; credentials
are presence-checked but never retained. Node startup rejects any authenticated
Scaleway credential environment.

Before opening that projection, the executable accepts only the chart's closed
controller/node flag set: Unix CSI and fixed private-admin endpoints, one clean
configuration path, and numeric liveness/optional metrics listeners. Duplicate,
unknown, positional, network-CSI, aliased-socket-directory, and conflicting
listener inputs fail before adapter construction. The CSI socket directory is
pre-created by Kubernetes and never widened by the process. The root driver may
narrow group/other write bits only after proving the exact owned directory
inode, which accommodates Kubernetes `emptyDir` defaults without trusting a
writable socket namespace. Only one unchanged
connection-refused Unix socket may be removed as stale; foreign or ambiguous
entries remain untouched.

## Node plugin

One privileged node driver runs on every eligible Linux workload node. It reads
local Scaleway metadata but has no authenticated Scaleway API credential. It
binds the validated metadata region and commercial type to the exact immutable
runtime region and release-qualified allowlist before it can become serving.
`NodeGetInfo` then returns only the cached `<zone>/<serverID>` identity: it does
not repeat metadata I/O and deliberately exposes neither topology nor a
`MaxVolumesPerNode` value. The physical parent attachment limit cannot be used
as a Kubernetes logical-PVC limit.

The node plugin
mounts each parent with the single flagless `virtiofs` contract, bind-mounts the
owned logical directory at the CO-owned staging target, then bind-mounts staging
to the Pod target. Unpublish/unstage inspect the live kernel mount graph and
refuse foreign, aliased, replaced, mismatched, or stacked mounts. They never
detach a parent.

The mountinfo ID is used only to relate and order one coherent namespace
snapshot because Linux may reuse it after unmount. Each unstacked driver target
is also opened with `O_PATH|O_NOFOLLOW`; its fdinfo mount ID must still match
the snapshot and `statx(STATX_MNT_ID_UNIQUE)` supplies the non-reusable
generation carried into exact unmount. Missing statx support fails closed.

Before serving, the node resolves existing components of the parent, kubelet,
CSI socket, plugin, registry, pod, and staging trees and rejects lexical or
resolved overlap. A fresh bounded mountinfo snapshot must contain one exact
parent, plugins, pods, and CSI-directory mount. Required Bidirectional anchors
must be shared mounts. Comparing each anchor's device and filesystem root also
detects bind aliases that path strings and symlink resolution cannot expose.
This is only a startup topology proof; every later RPC still reopens and
validates its exact path, fdinfo identity, and unique statx mount generation
without following symlinks.

The controller's coherent eligible-node preflight also binds each Ready
Kubernetes Node/CSINode/plugin Pod to a live provider commercial type and
positive `MaxFileSystems`. The exact commercial allowlist participates in the
shared node configuration generation, so changing release compatibility cannot
silently preserve an older rollout generation.

## Durable state

The allocation and ownership record form a redundant pair. JSON is
closed-schema and canonical, with checksum authentication where specified.
Writes use resourceVersion CAS or descriptor-relative, no-follow,
crash-durable filesystem operations.

```text
Reserved -> CreatingDirectory -> Ready -> Deleting
                                            |-> Archived
                                            |-> Retained
                                            `-> Deleted
```

There is no generic `Failed` state. Reconciliation may repair only exact
documented predecessor/successor crash windows. Permanent compact `Deleted`
tombstones reserve names without authorizing mounts, capacity, or mutation.

## Filesystem and provider boundaries

Each parent has an immutable claim at `/.sfs-subdir-csi-owner.json`.
Workload-writable data and driver metadata are separate trust zones.
Destructive traversal uses directory descriptors, `O_NOFOLLOW`, mount IDs,
atomic no-replace rename, bounded depth/descriptors, and quarantine-before-
removal; it never follows symlinks or crosses a mount.

The controller reconciles complete unfiltered regional File Storage attachment
pages with each exact zonal Instance's `Server.Filesystems`. Unknown states,
pagination/count disagreement, foreign attachments, and ambiguous reads fail
closed. Attach and authorized offline detach reread both views after ambiguous
results.

## Recovery

A recovery point is a controller-generated checkpoint captured under full
quiesce. Its bounded manifest commits restore-stable aggregates; external
detailed inventories bind source UID/resourceVersion and exact record
paths/hashes. Recovery is same-cluster only and requires installation and
cluster identity agreement, offline all-pre-recovery-Instances approval, and
clean fresh provider inventories. The historical checkpoint holder alone is
never a sufficient fence.

Normal startup first validates the complete allocation/PV/configured-parent
ownership inventory without mutation. A missing allocation is reconstructed by
create-only CAS either from authenticated ownership after conclusive PV absence,
or from the intersection of an exact current PV generation and authenticated
ownership. Recovery needs complete before the lifecycle crash-window pass
rereads allocations. Historical compact tombstones on a completed offline
decommissioned parent remain Kubernetes-only evidence and cause no parent
remount or per-tombstone API lookup.

## Operator boundary

The released `csi-admin` client reaches the active in-Pod admin endpoint only
through a controller-local Unix socket. One bounded, length-prefixed strict JSON
request is served per connection. The client handshakes before every mutation,
and the server repeats protocol negotiation on the mutation envelope before it
dispatches a handler. Transport concurrency and I/O are bounded, cancellation
closes active connections, and unexpected handler errors are redacted. Typed
route owners strictly decode checkpoint, GC, and upgrade inputs; duplicate or
missing routes fail closed. Handler-owned canonical artifacts are validated as
JSON but never generically re-encoded by the transport. This
boundary authenticates protocol compatibility only; Lease leadership,
quiescence, state CAS, provider fencing, and filesystem proofs remain in the
workflow owners.

Safe uninstall is coordinated from the operator side because runtime Service
Accounts cannot exec Pods, delete the node DaemonSet, or scale the controller.
One request-bound sequence first
validates the complete blocker inventory, quiesces the controller, invokes
exact unmount on every sorted node target after each Node process has removed
readiness and drained its Stage/Publish/Unpublish/Unstage gate, stops the node
DaemonSet, clears
controller mounts and provider attachments, verifies both provider inventory
views, releases the Lease gracefully, and finally stops the controller. Each
node/controller result binds a configured parent ID to its exact configured
mount-root path. The final audit also binds node IDs to checked Instance IDs,
inventory hashes, versions, Lease UID, and completion time. A dry run performs
only the inventory. After quiesce, a bounded ConfigMap owned by the exact
controller Deployment retains CAS-updated phase evidence so an interrupted
execute remains quiesced and resumes only through the same request ID. Helm
deletion of that Deployment garbage-collects the progress object after audit.

Target-parent decommission uses the same operator/Kubernetes ownership boundary
but freezes only one configured `draining` parent as its subject. The controller
contributes a read-only record/PV/Instance inspection, then quiesces globally;
the operator persists a second post-quiesce inventory before any node unmount,
records exact per-node target-parent evidence, deletes the DaemonSet by UID,
performs target-only controller unmount/detach, releases the Lease, and stops
the controller. A request-owned CAS ConfigMap makes every completed phase
restartable without treating missing workloads as proof that earlier cleanup
succeeded.

Checkpoint restore deliberately does not use Pod exec because no driver Pod may
be running. The checksum-verified operator reads and byte-regenerates the
deterministic archive, verifies cluster/installation identity and the complete
surviving driver-PV set, creates the exact all-Idle permanent journals and
Ready journal-set commitment before missing exact allocation ConfigMaps, then
recomputes the restore-stable aggregate. It creates the externally owned
immutable checkpoint Secret last. Parent inspection and all-Instance provider
fencing remain responsibilities of the provisional non-serving controller on
the subsequent startup.

## Observability boundary

Controller and node processes own separate concurrency-safe Prometheus
registries. All metric families have fixed descriptors. Pools and parents are
registered once from validated configuration; allocation states, parent
conditions, attachment anomaly states, provider operations, CSI operations,
and gRPC codes use closed domains. A parent refresh replaces its logical
capacity, raw `statfs`, physical free-space, lifecycle counts, timestamp, and
one-hot condition series atomically. The exporter cannot accept volume IDs,
node or Instance IDs, paths, PVC names, or arbitrary error text as labels.

Configured parent IDs are the only resource identities in labels and are
bounded by the installation allowlist. Offline-decommission history is folded
into `_historical` rather than preserving an unconfigured name. The metrics
HTTP handler serves deterministic Prometheus text on the dedicated metrics
listener; it is not reused for CSI readiness or shallow process liveness.

Shallow liveness is a separate cached boundary. The runtime-owned event loop
advances a heartbeat; a bounded stall, local clock regression, explicit
shutdown, or permanent internal invariant failure makes `/livez` return 503.
Provider or Kubernetes unavailability, leadership loss, a degraded parent, and
incomplete startup do not fail liveness. The unauthenticated handler returns
only generic health text and never exposes its internal diagnostic reason.

Liveness and metrics use distinct bounded HTTP servers, not merely different
routes on one listener. Exact unescaped path routing prevents one handler from
being exposed on the other's port. Read, write, idle, header, and graceful-
shutdown times are finite; request contexts are canceled before a five-second
drain, and timeout is reported rather than hidden by an early force-close. The
standard request error logger is suppressed so malformed unauthenticated bytes
cannot bypass structured-log bounds.

## Security summary

- Scaleway credentials exist only in the controller driver container.
- The node ServiceAccount has no API permission and token automount is disabled.
- Helm never owns the runtime Lease, external Secrets, allocation data, parent
  claims, or user data.
- Controller and node mount containers are the only privileged containers.
- The node registrar is not a privileged container but uses UID/GID 0 solely to create
  its socket in kubelet's shared root-owned registry; all capabilities are
  dropped and that shared directory is never chowned by the chart.
- Metrics use closed or configuration-allowlisted labels; Instance, node, and
  volume IDs, paths, and PVC names stay in structured diagnostics.
