# Specification: Scaleway SFS Subdirectory CSI Driver

## 1. Purpose

This specification defines a new open source Kubernetes CSI driver for Scaleway
File Storage.

The driver must expose many Kubernetes RWX PersistentVolumeClaims as isolated
subdirectories inside a small pool of existing Scaleway File Storage file
systems.

The target model is:

```text
Kubernetes PVC
  -> CSI logical volume
  -> one subdirectory inside one Scaleway File Storage parent
  -> pod sees a normal RWX volume
```

The project must be a standalone public GitHub repository. It must not depend on
private URLab code, private naming, private deployment conventions, or private
container registries.

The public project can mention in its README that it was created by URLab and
released under the MIT license. The driver name, package names, Helm chart, CRDs
if any, Kubernetes labels, and examples must remain neutral and reusable by the
community.

## 2. Problem Statement

Scaleway File Storage provides managed RWX storage for Kapsule and Instances.
The official Scaleway File Storage CSI driver maps a Kubernetes PVC to a
Scaleway File Storage file system.

That default model is not sufficient for platforms that create many user-level
RWX volumes. Scaleway limits the number of File Storage file systems that can be
attached to an Instance. A platform can therefore hit the node-level attach
limit long before it exhausts storage capacity.

The desired behavior is different:

- keep only a small number of physical Scaleway File Storage file systems
  attached to nodes;
- create many Kubernetes PVCs;
- map each PVC to a dedicated subdirectory of one parent file system;
- keep Kubernetes and application code unaware of this implementation detail.

This is the same general storage pattern as "NFS subdir provisioning", but the
driver must target Scaleway File Storage through its supported API and mount
model. It must not depend on undocumented NFS endpoints.

Scaleway documents Kubernetes `subPath` usage for splitting one File Storage
file system into multiple application directories. This project turns that
manual pod-level pattern into a CSI-level abstraction so applications can keep
using normal PVCs without adding provider-specific `subPath` logic.

## 3. Non-Goals

The first production-ready version must stay focused. Do not include the
following features in v1:

- automatic creation of parent Scaleway File Storage file systems;
- automatic cost-based pool scaling;
- automatic deletion of parent file systems;
- snapshots;
- clones;
- cross-cloud support;
- support for non-Scaleway storage backends;
- hard per-PVC filesystem quotas unless Scaleway exposes a supported mechanism;
- UI, dashboards, or web control planes;
- application-specific logic from URLab or any other product.

The driver must solve one problem well: dynamic RWX PVC provisioning through
safe subdirectory allocation inside existing Scaleway File Storage parents.

## 4. Public Project Identity

### 4.1 Repository

The public repository name is:

```text
scaleway-file-storage-subdir-csi
```

Rationale:

- explicit enough for users to understand the backend;
- neutral enough for a public open source repository;
- does not mention private product names;
- avoids implying that the project is the official Scaleway CSI driver.

The README must clearly state:

```text
This is a community project and is not an official Scaleway product.
Created by URLab and released under the MIT license.
```

### 4.2 License

Use MIT.

Required files:

- `LICENSE`
- `README.md`
- `CONTRIBUTING.md`
- `CODE_OF_CONDUCT.md`
- `SECURITY.md`

### 4.3 Driver Name

The CSI driver name must be globally unique and must not use a domain owned by
Scaleway unless Scaleway explicitly adopts or approves the project.

The immutable public CSI driver name is:

```text
file-storage-subdir.csi.urlab.ai
```

URLab controls the `urlab.ai` domain. This name is frozen before the first real
Kapsule release-candidate run and must not change while any PV, allocation,
ownership record, checkpoint, or parent claim created under it exists.

Do not use a Scaleway-owned or Scaleway-branded domain unless Scaleway approves
the project identity.

The Helm chart must allow overriding the driver name, but production users
should keep it stable after the first deployment because changing a CSI driver
name breaks existing PersistentVolumes.

### 4.4 Public Artifacts and Versioning

The public artifact coordinates are:

- source and issues: `https://github.com/urlab-ai/scaleway-file-storage-subdir-csi`;
- Go module: `github.com/urlab-ai/scaleway-file-storage-subdir-csi`;
- CSI driver name: `file-storage-subdir.csi.urlab.ai`;
- controller/node image: `ghcr.io/urlab-ai/scaleway-file-storage-subdir-csi`;
- Helm OCI chart: `oci://ghcr.io/urlab-ai/charts/scaleway-sfs-subdir-csi`;
- `csi-admin` Linux `amd64` and Linux `arm64` binaries: matching GitHub Release
  assets in the source repository.

Versions follow SemVer 2.0. Git tags and image tags use `vMAJOR.MINOR.PATCH`
with normal SemVer prerelease suffixes such as `v0.1.0-rc.1`; CSI
`vendor_version`, chart `version`, and chart `appVersion` omit the leading `v`.
The frozen candidates `v0.1.0-rc.1` through `v0.1.0-rc.10` are superseded and
must not be promoted. The seventh candidate reached real Kapsule provisioning,
installed the chart, and created its first logical volume, then proved that
Scaleway File Storage `virtiofs` rejects directory
`renameat2(RENAME_NOREPLACE)` and exposed an incorrect PVC-count expression in
the smoke harness. The eighth candidate proved automatic recovery of the
prepared `Deleting` archive through the descriptor-relative compatibility
path, but pre-scenario review found a second copy of the same incorrect count
expression. Both count sites now use the regression-tested filter. The ninth
candidate completed all five functional `run-smoke` scenarios on real Kapsule,
but its
POSIX-shell result collector allowed scenario code to overwrite the generic
scenario-name variable. The real operation logs and their ordered hashes were
retained, but the resulting scenario names and evidence filenames were not
admissible evidence. The collector now uses reserved runner variables and has a
focused behavioral regression test. The public `v0.1.0-rc.10` artifacts were
never production-qualified and are superseded: pre-cloud review found that the
new checkpoint E2E host-command boundary could inherit an ambient kubeconfig,
could panic on a nil stdin, and had opened the qualification interlock before
the normative matrix was coherent. Those runner defects are covered by focused
tests. The public `v0.1.0-rc.11` and `v0.1.0-rc.12` artifacts are also
superseded and must not be promoted. Real RC11 recovery work exposed that a
second abnormal takeover needed a fresh approval tuple and that the immutable
driver image digest belonged in the node configuration generation; RC12 fixed
both invariants and proved the repeated takeover plus cross-node RWX on real
Kapsule. Its fresh qualification run then stopped before Helm because the
profile-free Scaleway CLI needed an explicit Organization scope, while the
cleanup path did not yet admit a conclusively absent pre-Helm release. Those
runner-only gaps now fail before billable mutation or use the bounded
bootstrap-abort proof described below. The next candidate is
`v0.1.0-rc.13`. It is not a production support claim until that exact candidate
passes every Linux, kind, CSI, Helm, and real Kapsule qualification gate.
Supported Kubernetes and Kapsule versions remain limited to the exact versions
retained in that qualification evidence. `POP2-HM-2C-16G` is the sole proposed
commercial type for the first controlled run because it is the lowest-priced
currently documented type with two File Storage slots; it does not enter the
supported allowlist unless that exact run and its cleanup evidence pass.

The v1 controller and node images support Linux `amd64` and Linux `arm64`.
Release CI must compile both architectures, and the real-provider support matrix
must retain evidence for every advertised architecture and Instance type. A
different Linux architecture is unsupported until its descriptor-relative
filesystem syscalls, mount-identity checks, image, and Kapsule behavior pass the
same release gates; it must fail clearly rather than use a weaker path-based
fallback. Operator-side `csi-admin` platforms remain a separate release-matrix
decision.

Every released chart must reference public images by immutable digest. Tags
remain required as human-readable release metadata, but production manifests
must render `repository@sha256:<digest>` for the driver and every CSI sidecar.
Tag-only images, `latest`, development-only image names, private registries, and
placeholder Helm repository URLs are not acceptable in a public release. The
release metadata must record the exact rendered digests.

Runtime and artifact versions are distinct typed values. `VERSION` is strict
SemVer 2.0 without a leading `v`; it is linked into both binaries and is the
exact CSI `vendor_version` and admin protocol build version. `RELEASE_TAG` is
the human Git/artifact tag and must equal `vVERSION`; the leading `v` is never
embedded in CSI identity. Any other tag/version pair fails before compilation.

Every non-development binary also embeds a complete lowercase 40-character
SHA-1 or 64-character SHA-256 Git object ID and a canonical second-precision
UTC build timestamp, plus the canonical sorted, unique, comma-linked commercial
Instance type allowlist qualified by that release's real E2E matrix. The only
identity allowed to retain `unknown` source fields and an empty embedded
allowlist is the exact `0.0.0-dev` development build. Both executables validate
their linked identity before runtime operation and before returning a successful
`--version`. Release cross-compilation emits a per-binary JSON identity sidecar
whose `commercialTypes` array exposes the same list, and a Go module manifest.
The same invocation requires the deliberately configured public repository URL,
emits an SPDX 2.3 SBOM plus unsigned SLSA provenance subjects for those twelve
files. The SBOM describes the project artifacts and deduplicates every embedded
Go module/version reported by the four binaries as a `DEPENDS_ON` package; an
empty dependency inventory is a release error. The invocation covers all
fourteen files in the SHA-256 manifest. It never guesses
release coordinates. Container builds execute both binaries' version validation
in the build stage. Signing/attestation, image and chart publication, and the
final cross-artifact provenance remain separate mandatory release gates; an
unsigned local statement must never be presented as a signed attestation.

## 5. Target User Experience

### 5.1 Administrator Flow

An administrator creates one or more Scaleway File Storage file systems:

```bash
scw file filesystem create \
  region=fr-par \
  name=sfs-subdir-pool-standard-01 \
  size=2TB

scw file filesystem create \
  region=fr-par \
  name=sfs-subdir-pool-standard-02 \
  size=2TB
```

Then the administrator creates a dedicated namespace, a Kubernetes Secret for
the Scaleway credentials, and a second Secret containing the installation
identity:

```bash
kubectl create namespace scaleway-sfs-subdir-csi
kubectl label namespace scaleway-sfs-subdir-csi \
  pod-security.kubernetes.io/enforce=privileged \
  pod-security.kubernetes.io/enforce-version=latest

kubectl -n scaleway-sfs-subdir-csi create secret generic scaleway-sfs-subdir-csi-credentials \
  --from-literal=SCW_ACCESS_KEY="$SCW_ACCESS_KEY" \
  --from-literal=SCW_SECRET_KEY="$SCW_SECRET_KEY"

INSTALLATION_ID="$(uuidgen | tr '[:upper:]' '[:lower:]')"

kubectl -n scaleway-sfs-subdir-csi create secret generic scaleway-sfs-subdir-csi-identity \
  --from-literal=installationID="$INSTALLATION_ID"
```

Then the administrator installs the CSI driver:

```bash
helm upgrade --install scaleway-sfs-subdir-csi \
  oci://ghcr.io/urlab-ai/charts/scaleway-sfs-subdir-csi \
  --version <qualified-version> \
  --namespace scaleway-sfs-subdir-csi \
  --values /absolute/path/release-values.yaml \
  --set scaleway.region=fr-par \
  --set scaleway.defaultZone=fr-par-1 \
  --set scaleway.projectId=<project-id> \
  --set scaleway.credentials.existingSecretName=scaleway-sfs-subdir-csi-credentials \
  --set installation.existingSecretName=scaleway-sfs-subdir-csi-identity \
  --set 'pools.standard.filesystems[0].id=<filesystem-id-1>' \
  --set 'pools.standard.filesystems[1].id=<filesystem-id-2>'
```

The production installation path must use existing Kubernetes Secrets for both
credentials and installation identity. The installation identity is not a
credential, but it is durable safety state and must be backed up with the driver
namespace. Chart-generated credentials are forbidden. A chart-generated
installation identity may exist for local development only and must be clearly
documented as non-production.

The dedicated namespace is security-sensitive because the CSI mount containers
are privileged. The installation preflight must verify the effective Pod
Security Admission policy before Helm installation and fail with an actionable
message when privileged pods would be rejected. Rights to create Pods or
workloads in this namespace must be restricted to the platform administrators
and the Helm release process.

The shipped operator-side installation preflight must remain non-persistent:
it reads the namespace and required external Secret key names, including the
exact configurable `installation.idKey`,
`scaleway.credentials.accessKeyKey`, and
`scaleway.credentials.secretKeyKey` values used by the same Helm render, uses a
server-side dry-run privileged Pod to exercise effective admission, and reads
the exact Kapsule cluster through the Scaleway CLI to verify Project, region,
Kapsule type, and the cluster-level `scw-filestorage-csi` tag. It must not print
Secret values, install Helm objects, or mutate Scaleway resources.

### 5.2 StorageClass Flow

The Helm chart creates a StorageClass similar to:

```yaml
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: sfs-subdir-rwx
provisioner: file-storage-subdir.csi.urlab.ai
parameters:
  poolName: standard
  directoryMode: "0770"
  directoryUid: "1000"
  directoryGid: "1000"
  onDelete: archive
reclaimPolicy: Delete
allowVolumeExpansion: false
volumeBindingMode: Immediate
```

Platform overlays can then map their provider-neutral storage class name to the
driver. The open source project must not contain private platform-specific
StorageClass names in its default manifests.

### 5.3 Application Flow

Applications create ordinary RWX PVCs:

```yaml
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: user-volume-a
spec:
  accessModes:
    - ReadWriteMany
  storageClassName: sfs-subdir-rwx
  resources:
    requests:
      storage: 10Gi
```

The application must not know:

- which parent Scaleway File Storage file system is used;
- which subdirectory is used;
- whether the parent pool has one, two, or more file systems.

## 6. Core Design Decisions

### 6.1 Parent File Systems Are Existing Resources in v1

The v1 driver must consume an explicit pool of existing Scaleway File Storage
file systems.

It must not create parent file systems automatically in v1.

Rationale:

- parent file systems are cost-bearing infrastructure resources;
- operators must control their number, size, tags, and lifecycle;
- the number of parents must respect node-level attach limits;
- keeping parent lifecycle outside the driver makes the first version safer.

Future versions can add optional tag-based discovery or parent provisioning, but
that must not be part of the v1 implementation.

### 6.2 One PVC Equals One Logical Volume, Not One Scaleway File System

Each Kubernetes PVC becomes one logical CSI volume. A logical CSI volume is:

```text
parent filesystem ID + base path + generated directory name
```

Example:

```text
filesystem: sfs-11111111
basePath: /kubernetes-volumes
directory: pvc-4d6c8d2e-tenant-a-user-volume
```

The physical path inside the parent file system is:

```text
/kubernetes-volumes/pvc-4d6c8d2e-tenant-a-user-volume
```

### 6.3 Keep the Pool Small and Explicit

The pool size must be chosen by the operator based on:

- node type attach limits;
- required total capacity;
- required aggregate IOPS and throughput;
- expected workload distribution.

If the selected node type supports only two attached File Storage file systems,
the pool must contain at most two parents for workloads that can run on that
node type.

For v1, every node eligible to run workloads that use a pool must be able to
attach every parent file system in that pool.

The driver must not expose Scaleway's physical File Storage attach limit as the
Kubernetes CSI `MaxVolumesPerNode` value for logical PVCs. Kubernetes would count
logical PVCs, not parent file systems, which would defeat the purpose of this
driver.

V1 must omit `NodeGetInfo.MaxVolumesPerNode`. A future release may add a
separate tested logical-PVC limit, but it must never expose the physical parent
File Storage attachment limit as a logical volume count.

The Helm values must expose an explicit operator-owned limit:

```yaml
pools:
  standard:
    maxParentsPerEligibleNode: 2
```

The driver must validate the configured parent count:

```text
len(pool.filesystems) <= pool.maxParentsPerEligibleNode
```

If the validation fails, the controller must fail at startup with a clear
configuration error.

If multiple pools share the same eligible node set, the validation must be done
on deduplicated filesystem IDs across those pools and the node's live attachment
inventory, not by adding two counts that may contain the same parent:

```text
len(currentlyAttachedFilesystemIDs UNION configuredParentFilesystemIDs) <= nodeAttachLimit
```

The union must include every attachment visible through the Instance API,
including attachments created manually or by another CSI driver. Filesystems in
`attaching`, `available`, and `detaching` state consume one slot until the API
proves that they are absent. An unknown state makes the preflight fail closed.

Do not introduce per-pool topology in v1 unless a real use case requires it.
The KISS v1 model is one global eligible node set and aggregate attach-budget
validation for every pool that can run on that set.

This validation is necessary but not sufficient. The v1 driver must also make
the homogeneous-node contract executable.

Required v1 preflight:

- identify the Kubernetes nodes eligible to run pods using this StorageClass;
- verify every eligible node is in the configured Scaleway region;
- verify every eligible node maps to a Scaleway Instance that can attach the
  configured parent count;
- verify the current number of attached File Storage file systems leaves enough
  room for the configured parents;
- verify the controller node can attach and mount every configured parent;
- fail startup before serving CSI controller operations if the check cannot be
  completed safely.

The controller must also refresh attachment inventory at runtime before any
operation that may attach a parent to a node. This check must account for File
Storage file systems attached by this driver, the official Scaleway File
Storage CSI driver, and manual operations when they are visible through the
Scaleway API. If remaining attach budget is insufficient, the operation must
fail clearly before attempting a partial attach.

Production v1 supports one scheduling model: every schedulable Linux workload
node in the cluster is storage-eligible and must satisfy the File Storage
compatibility and attach-budget preflight. The node DaemonSet must run on this
same set, and startup preflight must verify a Ready node-plugin pod and CSINode
registration for every eligible node.

A production cluster must expose at least two Ready compatible nodes on which
the controller Deployment can be scheduled; three are recommended when the
cluster has three or more nodes. The chart and operations guide must not pin the
singleton controller to one node. This provides rescheduling capacity for a
graceful drain or maintenance event without claiming automatic takeover after
an unfenced crash. Preflight reports and rejects a production installation with
fewer than two controller candidates. Those candidates may all be in the same
Scaleway zone: v1 requires node-level rescheduling capacity, not a multi-zone
cluster. Multi-zone placement is an operator availability choice and is not an
installation prerequisite.

A narrower storage-only node set is not production-supported in v1. An
acknowledgement flag is not an enforcement mechanism, and `Immediate` binding
does not constrain pod scheduling. Supporting a narrower set requires the
topology-aware design deferred to a future version.

The official Scaleway File Storage CSI driver may remain installed on the same
Kapsule cluster, including when the `scw-filestorage-csi` cluster tag is required
to enable host support. However, v1 does not support consuming official
`sfs-standard` PVCs or manually attached extra File Storage filesystems on the
same workload nodes managed by this driver. Kubernetes accounts attachment
limits independently per CSI driver and cannot reserve the other driver's
physical slots. Operators that need active volumes from both drivers must use
separate clusters until a tested cross-driver scheduling policy exists.

Accordingly, production preflight must reject an eligible or controller node
that has any live File Storage attachment outside the configured parent set,
even when nominal attachment headroom remains. This strict rule prevents the
other driver or a manual operation from consuming a slot after this driver's
budget decision.

The controller must repeat this exclusivity check before attachment operations
and during periodic node validation. A newly detected non-configured attachment
makes the controller degraded and blocks new create/publish operations without
disrupting already mounted logical volumes.

Per-Instance inventory alone is not sufficient because an existing parent may
still be attached to an orphaned Kapsule Instance, a deleted Kubernetes Node, or
an unrelated Scaleway Instance. The controller must also use the regional File
Storage `ListAttachments` API for every configured parent:

- list every page without accidentally inheriting a single-zone filter from
  `SCW_DEFAULT_ZONE`;
- reconcile every attachment resource ID, resource type, and zone against the
  current controller Instance and known workload Instances;
- preserve the exact known `server ID -> zone` mapping from Node/CSINode
  evidence; an ID match in a different zone is an inventory disagreement, not
  an authorization match;
- fail closed when an attachment is unreadable, has an unknown resource type,
  or belongs to an Instance outside the active installation;
- compare the deduplicated result count with the parent metadata's
  `NumberOfAttachments`; a temporary mismatch returns `Unavailable` and is
  retried rather than being interpreted as absence;
- run this check before the first parent claim, during startup, after any attach
  result whose completion is ambiguous, and at a bounded periodic interval;
- require a new parent to have no pre-existing attachment before claiming it,
  except an attachment created by the current bootstrap attempt after the
  initial empty-inventory check.

The exception for the current bootstrap attempt must survive a controller
crash without making an unrelated attachment claimable. Parent claiming is
serialized, and the controller records at most one bootstrap attempt at a time
in fixed annotations on the existing controller leadership Lease. After the
empty regional and Instance inventories have been read and before calling
`AttachServerFileSystem`, one resource-version compare-and-swap must persist:

```text
schemaVersion
attemptID
installationID
activeClusterUID
parentFilesystemID
controllerNodeID
controllerInstanceID
controllerZone
emptyInventoryObservedAt
phase = Prepared
claimTempPath = /.sfs-subdir-csi-owner.<attemptID>.tmp
```

The record is an operation journal, not a second source of parent ownership.
Scaleway attachments do not carry this local `attemptID`, so an attachment to
the recorded Instance is evidence for resuming that attempt but is never proof
that this installation exclusively created the provider attachment. Before
owner creation, resume is permitted only when all of the following remain true:

- no parent-global owner, logical-volume ownership, allocation, or driver PV
  exists for the parent;
- the regional and Instance inventories contain no attachment other than the
  exact expected attachment;
- the recorded installation, cluster, and parent still match runtime identity;
- the controller holds the Lease under the normal handoff or approved-takeover
  rules from section 10.3.

When no owner record exists, the controller may resume ownership initialization
only when the recorded node, Instance, and zone match its current runtime
identity. It must never automatically detach a bootstrap attachment from a live
Instance, including its current Instance, because another installation or mount
namespace may be using the same provider attachment. A failed or abandoned
attempt becomes an offline recovery operation: the recorded Instance must first
be conclusively stopped, stopped in place, or deleted, and every controller or
child mount must be absent or rendered impossible by that fencing. Only then may
the driver detach the parent, poll `ListAttachments` and `Server.Filesystems` to
conclusive absence, and clear the bootstrap annotation. An orphaned attachment
to a deleted Instance that cannot be detached remains fail-closed and requires
Scaleway support; the driver must not guess ownership.

The claim temp path is deterministic from the random attempt ID and is reserved
only for that journaled attempt. The controller writes the complete claim to
that root-level temporary file with no-follow/no-overwrite semantics, `fsync`s
the file and parent root, then atomically renames it without overwrite to the
fixed claim path from section 6.7 and `fsync`s the root again. The same attempt
may validate, complete, or remove only its exact temporary file. A foreign,
unbound, or ambiguous temporary claim keeps the parent fail-closed.

On Linux, the no-overwrite installation must use
`renameat2(..., RENAME_NOREPLACE)` or an implementation with the same atomic
create-if-absent semantics proven by the real `virtiofs` release tests.
`EEXIST` means another claimant won and must never trigger replacement or
detach. `ENOSYS`, `EOPNOTSUPP`, or inability to make the file and root directory
durable fails parent preflight; a check-then-rename fallback is forbidden.

If a crash occurred after atomic owner creation but before annotation cleanup,
the controller validates that the complete owner record exactly matches the
attempt and runtime identity, then clears the stale attempt and continues normal
startup; it never rewrites the owner. Successful owner creation normally clears
the annotation by compare-and-swap. A missing, malformed, mismatched, or
multi-attachment attempt fails closed with an actionable manual recovery
message. A parent-global owner created by another installation always wins the
atomic no-overwrite race; the losing installation must not detach the shared
provider attachment. The controller must never infer bootstrap ownership merely
because an attachment points to its current Instance.

Before clearing a completed attempt, the controller removes only its exact
attempt-bound temporary claim and syncs the parent root, including on an
already-absent retry. The immutable final claim then becomes the sole parent
authority, the exact Lease journal is cleared, and the root-owned `0700` base
path and reserved layout are established through the ordinary claimed-parent
path. A crash after journal cleanup but before layout completion therefore
retries only descriptor-confined claimed-parent directory creation; it never
reopens the empty-parent adoption boundary or replaces the claim.

`Server.Filesystems` remains the authoritative source for the transitional
state of an attachment on a known Instance. `ListAttachments` is the
installation-wide inventory that discovers attachments to Instances the
Kubernetes node list cannot reveal. Both views must agree before the controller
serves new mutations.

For attachment ownership, a known workload Instance is backed by an existing
Kubernetes Node and matching CSINode registration for this driver in the
configured Project and region. A cordoned or temporarily unschedulable Node
remains known until its Node object is removed, which allows normal drain
operations without reclassifying a warm attachment as foreign. An Instance
without that evidence is unknown even if its name or tags look familiar.

The implementation must therefore keep two distinct sets: Ready schedulable
Node/CSINode identities eligible for a new publish target, and all existing
matching Linux Node/CSINode identities recognized for regional attachment
inventory. The latter includes cordoned or temporarily unschedulable nodes.
Membership in the broader known set never authorizes a new attachment.

In the pinned SDK, `ListAttachments` fills an omitted request zone from the
client's default zone. The implementation must therefore use a dedicated
regional File Storage client without a default zone, or an equivalent
release-tested request path that proves the outgoing query has no `zone`
filter. Unit tests must inspect the request and verify that attachments from
`fr-par-1`, `fr-par-2`, and `fr-par-3` are returned and deduplicated.

### 6.4 Capacity Is a Reservation, Not a Hard Quota in v1

The PVC `resources.requests.storage` value must be treated as a logical
reservation used for placement and reporting.

The v1 driver must not claim to enforce hard per-PVC quotas unless a supported
Scaleway File Storage quota mechanism is identified and tested.

Logical capacity accounting must be explicit and deterministic:

```text
reserveBytes = max(
  minFreeBytes,
  ceil(observedParentSizeBytes * minFreePercent / 100)
)
usableParentBytes = max(0, observedParentSizeBytes - reserveBytes)
logicalCapacityBytes = floor(usableParentBytes * maxLogicalOvercommitRatio)
logicalAllocatedBytes = sum(selectedCapacityBytes for reserving allocation records)
logicalAvailableBytes = max(0, logicalCapacityBytes - logicalAllocatedBytes)
```

The implementation must use checked integer or exact decimal arithmetic. It
must not let floating-point rounding, integer overflow, or a size refresh make
logical capacity negative or wrap around.

`maxLogicalOvercommitRatio` is a positive non-exponent exact decimal containing
at most 18 digits in total, with no non-canonical leading zero, and defaults to
`1.0`. A value greater than `1.0` is
an operator decision to overcommit logical reservations on top of the observed
usable parent size. At the default ratio, workloads that remain within their
logical reservations cannot consume the configured safety reserve solely
because too many reservations were accepted. A ratio greater than `1.0`
deliberately waives that reservation-backed guarantee. The driver must document
the risk clearly and must still enforce the actual free-space guardrails
described below.

The following states reserve logical capacity:

- `Reserved`;
- `CreatingDirectory`;
- `Ready`;
- `Deleting`;
- `Archived`;
- `Retained`.

`Deleted` records never reserve logical capacity.

The README must explicitly document this limitation:

```text
PVC sizes are used for scheduling and pool accounting. They are not hard
filesystem quotas in v1.
```

Because reservations are not hard quotas, the driver must also observe real
parent filesystem free space.

Each pool must support safety thresholds:

```yaml
pools:
  standard:
    minFreeBytes: 10737418240
    minFreePercent: 5
```

`CreateVolume` must fail with a clear error if no active parent can satisfy the
configured free-space reserve after accepting the new PVC request, even when
logical reservation accounting still shows available capacity.

The check must be fail-closed:

- parent selection must use fresh actual free-space information from `statfs`;
- a parent with missing, stale, or failed `statfs` data must be excluded for the
  current request or make the request fail with a clear transient error;
- the selected parent must satisfy both `actualFreeBytes - selectedCapacityBytes >=
  minFreeBytes` and the configured post-request `minFreePercent` threshold.

Create-time guardrails do not enforce runtime quotas. A running workload can
still consume more than its requested logical size and fill the shared parent.
V1 must treat this as an accepted production risk:

- expose runtime parent free-space metrics;
- emit degraded conditions or Kubernetes events when a parent crosses warning or
  critical free-space thresholds;
- provide sample Prometheus alert rules;
- document that this StorageClass is not a hard isolation boundary for
  adversarial or untrusted tenants unless operators isolate pools by tenant or
  Scaleway exposes a tested hard quota mechanism.

### 6.5 Parent File System Size Must Be Refreshed from Scaleway

Parent file systems can be resized outside the driver through Scaleway tools.
The driver must not cache parent capacity forever.

Required behavior:

- fetch parent metadata from the Scaleway API during controller startup;
- refresh parent metadata periodically;
- refresh parent metadata during `CreateVolume`;
- expose the last observed size and last refresh timestamp in logs and metrics;
- fail clearly if current parent metadata cannot be fetched and the driver needs
  that information to make a safe placement decision.

Recommended default refresh interval:

```text
5 minutes
```

The interval must be configurable.

In v1, `controller.metadataRefreshInterval` is also the single bounded
controller-maintenance cadence. One non-overlapping pass revalidates the
homogeneous Node/CSINode/node-plugin generation and live Instance attachment
limits, reads every configured parent's complete paginated regional attachment
inventory, refreshes parent metadata and the size-regression tracker, reads the
permanent allocation inventory once under stable pool locking, samples every
already-mounted parent with descriptor-anchored `statfs`, runs the state-driven
lifecycle crash-window reconciler, and then selects eligible detailed Deleted
tombstones for bounded in-place compaction. Reusing one cadence keeps the
operator contract and API load bounded; v1 does not expose independently tuned
background loops. One pass compacts at most 100 tombstones so a large retained
history cannot monopolize mutation admission or Kubernetes/provider budgets.
It reads the complete allocation list once for selection; already compact and
`deletedUnknown` records cause no follow-up API or filesystem read.

Every lifecycle resume of `Reserved` or `CreatingDirectory`, including a
periodic maintenance resume, must first read the permanent pool journal. A
matching `Pending` intent may be completed only from the exact canonical
`Reserved` allocation; `CreatingDirectory` with its own matching journal still
`Pending` is a fatal inconsistency. An `Idle` journal, or a newer `Pending`
generation for another logical volume, proves the older allocation already
crossed its reservation barrier. No filesystem creation or allocation-state
advance is allowed before this check.

The first complete pass is part of cold startup. Later passes require active
leadership. Their authorization/inventory publication enters the global
mutation gate, and each lifecycle repair independently follows the normal gate
and logical-volume lock order. A checkpoint quiesce skips a scheduled pass and
does not become a maintenance failure. An unreadable global inventory removes
readiness and blocks new CreateVolume and ControllerPublishVolume work, but is
retried on the next interval without failing shallow liveness. A successful
complete pass restores that maintenance condition and advances both the
attachment-inventory and reconciliation success timestamps. An internal
metrics-registry or leadership failure terminates the controller through its
supervisor rather than being retried as provider unavailability.

The driver must not automatically resize parent file systems in v1. Resizing is
an infrastructure operation controlled by the operator through Scaleway Console,
CLI, Terraform, Pulumi, or API.

When a parent is resized outside the driver, the next metadata refresh must make
the new size visible for future `CreateVolume` placement decisions.

The official Scaleway File Storage resize contract supports growth only;
shrinking a filesystem is unsupported. Real-provider tests and operator
documentation must therefore cover upward resize only and must never instruct
an operator to test a shrink.

Every refresh must still compare the observed size with the previous value. An
unexpected decrease is a provider or observation anomaly, not a supported
workflow. It marks only that parent `critical-size-regression`, excludes it from
new placement, emits a Kubernetes Event and alert metric, and reports the exact
difference from the previous observation and from current reservations plus the
safety reserve. Existing node mounts remain untouched. Safety-improving
unpublish and cleanup may continue only when their normal ownership, mount, and
state checks succeed. The condition clears automatically only after a fresh
authoritative observation is at least the previous accepted size; v1 has no
acknowledgement flag that suppresses the anomaly.

The v1 high-water mark is a process-local defensive observation, not a durable
capacity-authority record. A controller restart rebuilds placement from fresh
provider metadata, complete allocation accounting, and `statfs`; it does not
invent a previous accepted size. Persisting this extra diagnostic value is
deferred until real `virtiofs` qualification demonstrates that it materially
improves safety. Durable allocation records and the physical-space guardrail,
not this alerting high-water, remain authoritative across restart.

Unit tests with the fake provider must cover the defensive size-regression path
and recovery. Real-provider tests must cover upward growth and verify that a
shrink is neither requested nor represented as supported.

### 6.6 Scaleway Provider Contract

This section is the normative provider appendix for v1. It is based on the
official `scaleway/scaleway-filestorage-csi` implementation at commit
`2ede1238d63cf03b575eacee2ef1449be9106387`, inspected on 2026-07-12. That
reference uses `github.com/scaleway/scaleway-sdk-go v1.0.0-beta.36`. The
implementation must pin an explicit tested SDK version and record any later
reference commit in the compatibility table. Official code is Apache-2.0; code
copied from it must preserve all applicable license and notice requirements.

Required APIs and identity contract:

- use `github.com/scaleway/scaleway-sdk-go`;
- use `file/v1alpha1` to read regional parent filesystem metadata and validate
  filesystem ID, project, region, size, and availability, and to list all
  attachments for each configured parent;
- use `instance/v1` to read the target server and its filesystem attachment
  inventory, and to call `AttachServerFileSystem`;
- use `DetachServerFileSystem` only for the offline parent decommission,
  provider-fenced stale-node cleanup, or provider-fenced provisional-bootstrap
  rollback defined in sections 6.3, 6.9, 6.14, and 6.17, or the completed
  safe-uninstall sequence in section 7.5. Each path must first conclusively
  fence the relevant Instance or stop the owning driver component and prove or
  make impossible every live controller and child mount;
  never use it for normal logical-volume unpublish;
- the node plugin obtains its Instance ID, zone, region, and commercial type
  from the local unauthenticated Scaleway metadata service, matching the
  official driver behavior;
- every metadata field is untrusted input: region and zone must be normalized,
  the zone must belong to the reported region, the Instance ID and resulting
  node ID must satisfy the exact closed syntax, and the commercial type must be
  bounded single-line UTF-8 without a path separator before the value is used
  or advertised;
- `NodeGetInfo.NodeId` is exactly `<zone>/<serverID>`;
- the controller parses and validates that node ID and always performs zonal
  Instance operations in the parsed target zone. `SCW_DEFAULT_ZONE` is SDK
  initialization configuration only and is never the workload node zone;
- the controller accepts a publish target only when the node ID matches the
  CSINode registration of an existing eligible Kubernetes Node and the resolved
  Instance belongs to the configured Project and region;
- the node plugin may access the local metadata service but must not receive
  Scaleway credentials or call authenticated public Scaleway APIs.

An authorized offline detach receives a non-empty exact, deduplicated Instance
target set. Before the first mutation it reads the complete unfiltered regional
inventory and every target's complete `Server.Filesystems`, rejects an
attachment outside that set or any disagreement, and accepts only understood
stable Instance states. It issues at most one detach call per observed target,
then always rereads both inventory surfaces, including after an ambiguous API
error. A committed ambiguous result is success only after conclusive absence;
transient inventory unavailability before or after the call is retried inside
the same deadline without issuing another detach. A definite detach error is
returned after the mandatory reread unless that reread already proves absence.
Otherwise bounded backoff with jitter continues until both views agree on
absence or the detach deadline expires. Unknown, orphaned, duplicated,
cross-zone, unavailable, or stale evidence never becomes absence.

The controller must normalize the pinned SDK's complete
`FileSystem.Status` enum. The matrix is closed: any future or unreadable value
uses the unknown row until a compatibility-tested release adds it.

| File Storage observation | New placement | New attachment or controller filesystem mutation | Existing data path and cleanup | Cold-start meaning |
| --- | --- | --- | --- | --- |
| `available` | allowed after all other checks | allowed | allowed | may become ready after all other gates |
| `creating` or `updating` | excluded for this decision | retryable `Unavailable`; do not attach, create, archive, delete, or GC | already verified kernel mounts continue; node unpublish/unstage and normal metadata-only controller unpublish remain allowed | non-serving while this configured parent is required for ownership reconciliation |
| `error` | excluded | `FailedPrecondition`; do not attach or mutate the filesystem | do not tear down healthy existing mounts automatically; node unpublish/unstage and normal metadata-only controller unpublish remain allowed | non-serving with an explicit provider-status error |
| `unknown_status`, unknown future value, or unreadable status | excluded | retryable `Unavailable`; do not infer safety | do not tear down healthy existing mounts automatically; node unpublish/unstage and normal metadata-only controller unpublish remain allowed | non-serving until an authoritative supported status is read |

A runtime transition away from `available` marks that parent degraded and
blocks only operations that depend on it; it does not make unrelated parents or
already-mounted workloads unavailable. The controller must emit a bounded
Kubernetes Event and metric on status transitions. It may resume blocked work
only after a fresh authoritative read returns `available` and all normal
ownership, attachment, mount, and capacity checks pass.

The same parent-local containment applies to a provider metadata read failure,
invalid parent identity, attachment-inventory anomaly, controller mount
failure, `statfs` failure, or parent-specific capacity/physical-space failure.
Periodic inventory must return an explicit degraded observation for that
parent, continue observing every other configured parent, and keep new
placement available on independently healthy parents. A Kubernetes node
authorization failure, an invalid or incomplete allocation inventory, a
cluster-wide attachment-budget/exclusivity failure, or cancellation remains a
global failure because it cannot be attributed safely to one parent.

The controller must normalize the pinned SDK's Instance state before any new
attachment or recovery decision. The v1 state matrix is closed and fail-safe:

| Instance observation | New publish or controller lifecycle attach | Recovery/fencing meaning |
| --- | --- | --- |
| `running` | allowed after all other inventory and budget checks | process is not fenced |
| `starting` | retryable `Unavailable`; do not attach | process is not fenced |
| `stopping` | retryable `Unavailable`; do not attach | process is not yet fenced |
| `locked` | retryable `Unavailable`; do not attach | process is not fenced |
| `stopped` or `stopped in place` | `FailedPrecondition`; do not attach | process is fenced, subject to attachment-absence checks |
| conclusive Instance `NotFound` | `NotFound`; do not attach | process is fenced, but regional orphan attachments remain blocking |
| unknown, unreadable, or ambiguous | retryable `Unavailable`; do not attach | no fencing proof |

The same matrix must be used by publish, controller lifecycle attachment,
bootstrap rollback, stale-node cleanup, and abnormal takeover. A deleted
Instance is proof that its process cannot continue, but it is not proof that
Scaleway removed the File Storage attachment. Before clearing quarantine,
published-node evidence, or recovery blocks, the controller must also obtain a
fresh paginated regional inventory and prove that the relevant parent
attachment is absent. An orphan attachment to a deleted Instance remains
fail-closed and requires the documented Scaleway support escalation path.
Provider fencing first validates that the durable CSI node zone belongs to the
configured region and that the parent ID is syntactically valid. Malformed or
cross-region evidence is rejected before a provider read; an Instance
`NotFound` in an unrelated zone cannot be converted into fencing proof.

Attachment inventory and idempotency contract:

1. List every regional File Storage attachment for the target parent, across
   all pages and zones, and reject any attachment outside the active
   installation.
2. Immediately before an attach, read every known server with the Instance API
   in its exact evidenced zone and reconcile each `Server.Filesystems` view
   with the complete regional inventory. A disagreement on a different known
   server blocks this parent mutation just as a disagreement on the target
   does; the periodic reconciliation is not a substitute for this
   pre-mutation proof.
3. Treat every filesystem in `attaching`, `available`, or `detaching` state as
   consuming one physical attachment slot. Unknown states fail closed.
4. Enforce section 6.3 by rejecting non-configured live attachments on
   production workload/controller nodes.
5. Calculate the post-operation budget from the union of currently attached IDs
   and configured parent IDs. Never add overlapping counts.
6. If the target parent is `available`, return success.
7. If it is `attaching`, poll the server until it becomes `available`.
8. If it is `detaching`, return a retryable `Unavailable` error. Do not issue a
   competing attach.
9. If it is absent and the union fits within
   `ServerType.Capabilities.MaxFileSystems`, call `AttachServerFileSystem` once,
   then poll until the target becomes `available`.

The one-call rule spans the complete driver operation, including a lost or
ambiguous Attach response. A transient `Unavailable`/deadline observation
before or after that call is retried with bounded backoff inside the same
ten-minute deadline. After Attach has been issued, an immediate reread that is
still absent is treated as possible propagation and never authorizes a second
Attach call. Permission, identity, argument, quota, and definite precondition
failures remain immediate failures. HTTP `409 Conflict` is kept distinct from
a definite `412 Precondition Failed`: the conflict follows the
ambiguous-result reread path and never authorizes a second Attach call in the
same operation.
10. After conflict, timeout, connection loss, or any ambiguous API result,
    re-read both the regional attachment inventory and the target server before
    deciding whether to retry or return success.

The official driver currently uses a fixed three-second sleep after attach. That
is a behavioral reference, not an acceptable readiness contract for this
driver. Polling must use a configurable deadline with a production default of 10
minutes, honor an earlier caller cancellation, use bounded backoff with jitter,
and never busy-loop. Sidecar timeouts must be configured consistently with this
deadline. A timeout returns `DeadlineExceeded`; temporary Scaleway or network
failures return `Unavailable`.

Provider error mapping:

- malformed IDs, zones, or configuration -> `InvalidArgument`;
- missing parent or Instance after a conclusive read -> `NotFound`;
- rejected IAM authorization -> `PermissionDenied`;
- physical attachment limit or provider quota exhausted ->
  `ResourceExhausted`;
- incompatible Instance type, region, or parent state ->
  `FailedPrecondition`;
- temporary API, network, `attaching`, `detaching`, or unknown completion state
  -> `Unavailable`;
- exhausted operation deadline -> `DeadlineExceeded`.

The production IAM application must be scoped to the target Project. With the
permission sets documented by Scaleway at the time of this specification, the
minimum usable policy is:

- `FileStorageReadOnly` to read existing parent metadata;
- `InstancesFullAccess` to read Instances and attach or explicitly detach File
  Storage filesystems.

`InstancesFullAccess` is broader than the driver ideally needs. The README must
state this limitation. If Scaleway publishes a narrower attachment-specific
permission set, it must replace `InstancesFullAccess` after a least-privilege E2E
test. `FileStorageFullAccess` is not required because v1 never creates, resizes,
or deletes parent filesystems.

Mount contract:

- parent filesystems are mounted with filesystem type `virtiofs`;
- the mount source is the parent filesystem ID;
- `ControllerPublishVolumeResponse.publish_context` is empty in v1;
- the node derives everything needed for the mount from immutable
  `volume_context`, the local metadata identity, and host attachment state;
- the driver never depends on an undocumented NFS endpoint or user-managed
  Private Network configuration. Scaleway documents the private backend
  connection as managed and transparent.

Release-time compatibility contract:

- File Storage is limited to regions and Instance types that the current
  Scaleway API and documentation qualify. As of 2026-07-20, the public
  documentation identifies the PAR region and reports File Storage as General
  Availability;
- startup must verify `ServerType.Capabilities.MaxFileSystems > 0` for every
  eligible node and must not rely only on a hardcoded family list;
- every release must embed the exact allowlist of Scaleway commercial types
  qualified by its real Kapsule E2E matrix. Startup requires both allowlist
  membership and live `ServerType.Capabilities.MaxFileSystems > 0`; a non-zero
  limit alone does not qualify an untested type. Extending the production
  allowlist requires a new tested release, not an operator acknowledgement;
- the chart projects that canonical sorted allowlist into the closed runtime
  document, but production startup requires byte-for-byte set equality with the
  independently embedded binary list. An operator may not add, remove, reorder,
  duplicate, or acknowledge a type around that comparison;
- every advertised commercial Instance type must pass real Kapsule E2E and
  appear in the release compatibility table;
- the Kapsule cluster must carry the `scw-filestorage-csi` tag at cluster level
  whenever Scaleway requires it to enable File Storage host support. A pool-level
  tag is insufficient;
- the official CSI may be installed but must remain unused on this driver's
  workload nodes, as defined in section 6.3;
- every release must re-check product maturity, region availability, quotas,
  attach limits, cluster tag behavior, and the SDK attachment states.

A production release must target the GA Scaleway File Storage offer. Supporting
a future beta/preview region or variant requires a deliberate specification and
support-policy change; an operator acknowledgement alone is insufficient.
Provider facts that cannot be confirmed by public documentation must be proven
by the real Kapsule E2E suite before release; the driver must fail closed rather
than infer them.
The installation preflight and README must verify the cluster-level
`scw-filestorage-csi` tag through the Kapsule API/CLI before Helm installation;
the runtime driver does not gain broader Kapsule IAM solely to repeat that
administrative check.

### 6.7 Parent Ownership and Stable Installation Identity

In v1, one parent filesystem is an exclusive ownership boundary for exactly one
driver installation. Sharing one parent between installations, even through
apparently disjoint base paths, is unsupported because two installations cannot
atomically prove that `/a` and `/a/b` do not overlap without a parent-global
claim registry.

Each installation must have a stable `installationID` supplied through the
production identity Secret described in section 7. The value must remain stable
for the lifetime of every PV, allocation record, ownership record, and parent
claim created by that installation.

The controller must also derive an immutable `activeClusterUID` from the UID of
the Kubernetes `kube-system` Namespace. The installation ID identifies the
logical driver installation; the cluster UID proves which Kubernetes cluster is
currently authorized to operate its parents. A copied identity Secret must not
authorize a second cluster.

The node plugin has no Kubernetes API permission and therefore does not derive
an independent cluster UID. Before authorizing a new stage or publish, it must
instead require byte-for-byte equality of `activeClusterUID` across the
immutable CSI `volume_context`, the mounted parent-global claim, and the
per-volume ownership record. Cleanup derives the expected value from the
authenticated ownership record and requires the parent claim to agree. A
mismatch in any direction fails closed; neither context nor a directory name
alone authorizes access.

Every side-effecting controller path must revalidate the loaded allocation's
`driverName`, `installationID`, and `activeClusterUID` against the current
runtime immediately after the durable read and before provider, fencing,
filesystem, or durable-record mutation. Startup ownership checks remain
mandatory but are not a substitute for this per-operation anti-copy boundary.

Each parent must have one root-level parent-global claim record:

```text
/.sfs-subdir-csi-owner.json
```

The owner record must contain:

```json
{
  "schemaVersion": "1",
  "revision": 1,
  "driverName": "file-storage-subdir.csi.urlab.ai",
  "installationID": "...",
  "activeClusterUID": "...",
  "parentFilesystemID": "...",
  "basePath": "/kubernetes-volumes",
  "basePathHash": "...",
  "controllerNamespace": "...",
  "helmReleaseName": "...",
  "leadershipLeaseName": "scaleway-sfs-subdir-csi-controller",
  "bootstrapAttemptID": "...",
  "contentChecksum": "sha256:...",
  "createdAt": "..."
}
```

Rules:

- each parent filesystem ID may appear exactly once across all configured pools
  in one installation;
- a parent claimed by one installation must not be configured by another
  installation, imported through the official CSI driver, or modified by
  another provisioner;
- `basePath` must be an absolute, normalized, non-root path and its first
  component must not use the reserved `.sfs-subdir-csi-owner` namespace used
  by the immutable root claim and bootstrap temporary files;
- after the parent-global claim is established, the controller may create a
  missing base path and reserved subdirectories with driver-only ownership;
  every component in that managed layout is root-owned with mode `0700`, and
  each created or repaired inode and containing directory is synced before the
  controller serves logical-volume mutations;
- a previously unclaimed v1 parent must be dedicated and empty: the fixed claim
  file, configured base path, and logical-volume metadata paths must be absent,
  and no unrelated user entry may exist on the parent. The one exception is the
  exact temporary claim file bound to the current bootstrap journal;
- this driver and the official Scaleway File Storage CSI driver may coexist on
  the same cluster only through separate driver names and separate
  StorageClasses;
- startup preflight must validate that the root-level claim file is either
  absent or already owned by the same configured
  `driverName`, `installationID`, `activeClusterUID`, controller namespace, and
  fixed `leadershipLeaseName`;
- the parent-global claim is permanent in v1. Removing a parent from a pool does
  not release it for another installation. Parent claim transfer is a future
  feature requiring an explicit offline migration protocol.

If ownership cannot be proven, the controller must fail closed before any
metadata/data mutation or logical-volume stage, publish, archive, retain, or
delete. The only permitted parent mount is the provisional inspection path
defined in section 10.2; it must not expose a logical volume. "Read-only
inspection" describes the controller operation surface, not an unverified
`virtiofs` mount option: the provisional mount uses the same release-tested
flagless parent mount contract, while the process exposes no mutation or repair
path until ownership is proven.

Creating the parent-global claim must be atomic. The implementation must not use
a check-then-create sequence or create a destination directory before ownership
is durable. It first inspects the dedicated parent and fails closed on any
unexpected data, base path, claim file, temporary claim not bound to the exact
journal, symlink, or nested mount. It then uses the root-level temp-file,
`fsync`, and no-overwrite rename protocol defined in section 6.3. The final
claim is immutable and is never replaced in v1. A diagnostic dry-run may print
the blocking inventory but never authorizes adoption. There is no production
bootstrap override or persistent adoption flag in v1. Importing existing data
is a future offline migration feature and must not be implemented implicitly.

Production identity lifecycle:

- the chart requires `installation.existingSecretName` and reads the UUID-like
  value from `installation.idKey`;
- the chart never owns or deletes that existing Secret;
- an upgrade or rollback must reuse the same value;
- reinstalling the chart requires the same Secret or a restored Secret with the
  same value;
- same-cluster namespace recovery must observe the same `activeClusterUID` from
  `kube-system` and the same value in every parent-global owner record;
- automatic cross-cluster restore is unsupported in v1. A different
  `activeClusterUID` must keep the controller non-serving even when the restored
  `installationID` matches;
- v1 does not implement parent-claim transfer. Moving an installation to a new
  cluster requires an explicitly documented future offline transfer protocol
  that first fences every old controller and workload Instance and then updates
  every parent claim consistently;
- if the Secret is missing while any driver PV, allocation record, or
  parent-global owner record exists, startup fails closed;
- the operations guide must back up the identity Secret together with allocation
  records and Helm values;
- automatic identity generation is allowed only behind an explicit
  development-only value and must never be a production default.

### 6.8 Parent Selection Strategy

The driver must support a simple production-safe parent selection strategy.

Required v1 strategy:

```text
least-allocated
```

Definition:

- reconstruct or validate missing allocation records from existing
  PersistentVolumes when required;
- compute logical allocation from allocation records owned by this driver,
  keyed by `logicalVolumeID`;
- sum requested PVC capacity by parent file system;
- refresh Scaleway metadata and actual free-space information for eligible
  active parents;
- exclude parents that do not satisfy logical capacity, actual free-space, node
  compatibility, or lifecycle requirements;
- select the remaining parent with the most remaining logical capacity;
- break ties with deterministic hashing of the requested volume name.

The v1 driver must run exactly one controller replica. CSI sidecar leader
election is still required where supported, but it does not replace
driver-owned leadership coordination for startup reconciliation, pool
accounting, controller mounts, archive, retain, delete, and node compatibility
refresh.

Controller HA and automatic failover are out of scope for v1. They can be added
only after the project has a tested storage-level fencing or equivalent takeover
design. A Kubernetes Lease alone is not fencing.

The driver must persist the chosen parent in the allocation record, the
driver-owned ownership record, and immutable `volume_context` returned by
`CreateVolume`. The CSI `volumeHandle` stores the deterministic logical volume
ID and mapping hash that validate this immutable mapping. After that, the
volume always uses the same parent, even if parent sizes change later.

### 6.9 Parent Lifecycle

Each parent file system in a pool must have an explicit lifecycle state:

```text
active
draining
```

Behavior:

- `active`: eligible for new logical volumes;
- `draining`: existing logical volumes continue to work, but the parent is not
  selected for new logical volumes.

`draining` changes placement only. It must not remove the parent from lifecycle
operations. The controller must still be able to attach and mount a draining
parent while any allocation record, ownership record, archived data, retained
data, delete operation, or GC operation references it.

A parent file system cannot be removed online from the Helm values in v1.
Changing it to `draining` stops new placement, but the parent remains configured
while the driver is running because controller and node parent mounts are kept
warm and physical attachments are intentionally not reference-counted.

Exceptional parent decommissioning is an offline operator procedure, not a CSI
reconciliation feature. It is permitted only when no active, reserving,
archived, retained, deleting, PV, VolumeAttachment, published-node, staging
mount, or live detailed ownership record references the parent. Valid terminal
allocation tombstones with `state: Deleted` and `reservesCapacity: false`, and
matching compact `Deleted` ownership tombstones, are historical evidence and do
not block the procedure. Allocation tombstones may use the detailed, compact,
or `deletedUnknown` variant. Detailed allocation tombstones may be compacted in
place first but compaction is not a prerequisite.

Before detaching the parent, the decommission validator must compare every
remaining compact ownership tombstone with its Kubernetes allocation tombstone
and reject any missing, conflicting, malformed, or non-terminal pair. This is
the last online validation that reads the decommissioned parent's compact
ownership inventory. Readable workload, PV, VolumeAttachment, published-fence,
staging, target, and child-mount references are reported together in stable
order so the operator can remove every normal blocker; malformed, duplicated,
one-sided, or conflicting durable evidence remains a fail-closed error and is
never presented as an operator-removable blocker.

The procedure must then:

1. stop the controller and node-plugin processes that can use the parent;
2. verify and unmount the exact controller and node parent mount on every
   attached Instance, after proving no child bind mount remains;
3. verify the parent is absent from each Instance's mount table;
4. call the explicit detach operation and poll both `ListAttachments` and
   `Server.Filesystems` until the attachment is absent everywhere;
5. remove the parent from Helm values only after those checks succeed.

The operator coordinator exposes a read-only dry-run and an execute mode over
one immutable normalized plan. Execute must repeat the complete Kubernetes,
node-mount, allocation, and ownership blocker inventory after the controller
owns the drained quiesce barrier and before any node parent unmount. The parent,
release workload UIDs, node targets, mount roots, and version identities must
remain equal to the initial plan. It then orders target-only node unmount,
node-plugin stop, target-only controller unmount/detach and fresh dual-inventory
proof, graceful controller release, and controller stop. Cleanup evidence is
validated before release and the final audit is validated before it can
authorize the separate Helm-values update. A crash-sensitive implementation
must persist request-bound progress before deleting the node DaemonSet or
scaling the controller, using the same no-ambiguity and exact-UID principles as
safe uninstall.

Removing a parent from values does not release its permanent parent-global claim
or make cross-installation reuse supported in v1. The operations guide must not
describe a live-node detach shortcut. After removal, Kubernetes allocation
tombstones are the only online name-reservation and historical evidence for that
parent. Startup, checkpoint, and reconciliation must not reattach or remount an
unconfigured historical parent only to read its compact ownership tombstones,
must not reconstruct those tombstones, and must not perform capacity accounting
or filesystem mutation against that parent.

Controller startup must fail clearly if existing reserving or non-terminal
allocation records, ownership records, or PersistentVolumes reference a parent
file system that is missing from the current configuration. A schema-valid terminal tombstone with
`state: Deleted` and `reservesCapacity: false` may retain its historical parent
ID after offline decommission and must not make startup fail. This exception
does not authorize filesystem access, reconstruction, or parent reuse.

Reintroducing a previously decommissioned parent ID into configuration is not a
lightweight rollback. Before serving, the controller must perform the complete
parent claim, attachment, mount, inventory, allocation-pairing, and ownership
validation required for any configured parent. Any difference from the
permanent claim or retained Kubernetes tombstones fails closed.

V1 has no emergency configuration bypass. Recovery requires restoring the exact
parent to configuration or completing the documented offline decommission
procedure. The driver never weakens ownership, path, or attachment checks based
on an operator boolean.

The operations guide must document the safe sequence:

```text
active -> draining -> no references -> stop driver -> unmount everywhere -> detach everywhere -> remove from values
```

### 6.10 Volume Handle and Volume Context

The volume handle must be stable, parseable, compact, and sufficient to locate
the driver's durable allocation record.

CSI `DeleteVolume` must not depend on `volumeContext` during the normal path.
The handle therefore carries immutable lookup identity, while long mapping data
lives in the allocation record, the PersistentVolume CSI attributes, and the
driver-owned ownership record.

Required v1 format:

```text
sfs1:<logical-volume-id>:<mapping-hash>
```

Example:

```text
sfs1:lv-4d6c8d2e9f1a2b3c:mh-9f34ac10d8e7a1b2
```

Rules:

- the encoded handle must be less than or equal to 128 bytes;
- `logicalVolumeID` is `lv-` followed by the first 32 lowercase hexadecimal
  characters of `SHA-256(driverName + NUL + CreateVolumeRequest.name)`;
- `mapping-hash` must be derived from immutable mapping fields:
  `poolName`, `parentFilesystemID`, normalized `basePath`, `directoryName`, and
  `logicalVolumeID`;
- the handle must not encode long values such as full base paths, directory
  names, regions, or parent names;
- the driver must validate the format and length on every controller and node
  operation that can mutate or expose state;
- invalid handles must return clear CSI errors, except for the CSI-mandated
  `DeleteVolume` unknown-ID behavior defined below.

The mapping hash is `mh-` followed by the first 32 lowercase hexadecimal
characters of SHA-256 over canonical JSON containing exactly the mapping fields
listed above. Object keys are lexicographically sorted, strings are UTF-8,
integers use base-10 JSON representation, and no insignificant whitespace is
written. The request hash uses the same canonicalization and `rh-` prefix over
the normalized creation fields defined in section 6.12. A truncated-hash
collision is detected by comparing the stored full original identity and
immutable fields; the driver must fail closed rather than reuse the record.

`basePathHash` is `bp-` plus the first 32 lowercase hexadecimal characters of
SHA-256 over the normalized UTF-8 base path. `volumeHandleHash` is `vh-` plus
the first 32 lowercase hexadecimal characters of SHA-256 over the complete
volume handle. These algorithms and prefixes are part of the v1 compatibility
contract.

The compact handle intentionally does not contain every recovery field. The
allocation record is the primary durable mapping. The PersistentVolume CSI
attributes and the driver-owned ownership record are secondary recovery inputs.
If all durable mapping sources are missing, the driver must not guess a
filesystem path. For `DeleteVolume`, it must preserve CSI idempotency by
recording a minimal non-reserving deleted-unknown tombstone and returning success
for a valid driver handle only when every authoritative lookup completed
successfully and conclusively reported absence. API timeouts, denied reads,
unmounted parents, stale inventory, or unavailable metadata are not absence and
must return a retryable error without writing a tombstone.

`CreateVolume` must also return immutable `volume_context` fields:

```text
schemaVersion
installationID
activeClusterUID
poolName
parentFilesystemID
basePath
basePathHash
directoryName
directoryMode
directoryUid
directoryGid
onDelete
logicalVolumeID
```

The list above is the complete driver-owned context. The pinned Kubernetes
external-provisioner adds its own non-empty bounded
`storage.kubernetes.io/csiProvisionerIdentity` field to the resulting PV and
replays that field to later Controller and Node calls. The driver must never
emit, hash, persist as allocation identity, or authorize from that sidecar
value. Inbound parsing accepts and strips exactly that one CO-owned key after
applying the same wire-size validation; every other unknown key, and an empty
provisioner identity, fails closed. Checkpoint and restore-stable PV projections
contain only the 13 driver-owned fields, while exact source-generation checks
may retain the complete live Kubernetes map to detect a changed PV during one
recovery attempt.

`logicalVolumeID` must use the exact deterministic algorithm above. It must not
be random. This is the key idempotency invariant for repeated CSI `CreateVolume`
calls with the same name.

The returned CSI wire data must remain within the CSI specification limits:

- every `volume_context` key and value must be at most 128 UTF-8 bytes;
- the complete `volume_context` map must be at most 4 KiB under the CSI-defined
  size calculation;
- the implementation must validate the final encoded map before persisting an
  allocation record, creating a directory, or returning `CreateVolume`;
- chart validation must bound `installationID`, pool names, parent IDs,
  `basePath`, directory defaults, and every other configured value that can
  appear in the map. A generated directory name must be shortened by its
  deterministic naming algorithm, never truncated after the mapping hash has
  been computed;
- a value that cannot fit without changing identity must fail with
  `InvalidArgument`. The driver must not silently omit a required key or emit a
  partial context.

Boundary tests must cover 128-byte strings, 129-byte strings, a map exactly at
4 KiB, and a map one byte over the limit, including multi-byte UTF-8 input.

V1 never reuses a `CreateVolumeRequest.name` within one installation, including
after deletion and tombstone compaction. Kubernetes external-provisioner volume
names are unique, so this restriction does not affect ordinary PVC recreation.
Permanent name reservation avoids an old `DeleteVolume` retry ever resolving to
a newer logical volume with the same deterministic ID.

Node staging and publishing operations must receive these immutable
`volume_context` fields from the PV. They must recompute the mapping hash,
validate it against the parsed handle, strip only the documented
external-provisioner delivery identity, and fail closed on missing, other
unknown, or mismatched fields. One shared immutable-context validator must
compare every normalized driver-owned context field, not only the mapping hash:

- `ControllerPublishVolume` compares it with the allocation and detailed
  ownership record before provider attach or durable publish-fence mutation;
- `NodeStageVolume` and `NodePublishVolume` compare it with the parent claim and
  per-volume ownership record before `chmod`, `chown`, stage, or publish;
- `schemaVersion`, `installationID`, `activeClusterUID`, delete policy,
  UID/GID/mode, parent/path identity, and every hash are all mandatory equality
  checks.

`ValidateVolumeCapabilities` is the only non-delete exception: because the RPC
is read-only and the upstream CSI sanity contract may omit `volume_context`, an
empty map is resolved from the handle plus authoritative allocation and
ownership records. When the map is present, every field must match exactly.
Missing context remains an error for `ControllerPublishVolume` before any
repair, provider call, filesystem action, or durable mutation. The CSI sanity
unknown-volume and unknown-node probes may receive a conclusive `NotFound`
first because those requests deliberately omit `volume_context`. The
unknown-node decision is restricted to the already-observed Kubernetes node
inventory and performs no provider lookup, attach, mount, repair, or durable
write. A known node, unavailable read, or ambiguous absence does not bypass
context validation.
Missing context is also always an error for `NodeStageVolume` and
`NodePublishVolume` because those calls have filesystem side effects.

The mapping hash remains the compact path-identity proof; field-by-field
validation protects the complete v1 context. The "no `volumeContext`" recovery
requirement applies to controller `DeleteVolume` and the read-only validation
exception above, not to normal controller publish or node mounting.

Changing a pool `basePath` while volumes exist is not supported in v1.

`DeleteVolume` resolution order:

1. parse and validate the handle;
2. load the allocation record by `logicalVolumeID`, distinguishing a conclusive
   Kubernetes `NotFound` from an unavailable or forbidden lookup;
3. validate that the persisted mapping hash matches the handle;
4. if the allocation record is missing and the PV still exists, reconstruct from
   the PV CSI attributes and the driver-owned ownership record;
5. if the allocation record is missing but configured parent/base paths still
   contain a matching driver-owned ownership record for `logicalVolumeID`, use
   it only after validating the mapping hash and driver name;
6. if any authoritative lookup is unavailable, return `Unavailable` or
   `FailedPrecondition` without mutating state;
7. only if every authoritative source was read successfully and all report
   absence, persist a minimal non-reserving deleted-unknown tombstone, emit an
   event/metric, and return success without touching the filesystem.

### 6.11 Directory Naming

Directory names must be deterministic, safe, and human-inspectable.

Recommended format:

```text
<namespace>--<pvc-name>--<short-pv-or-volume-id>
```

Rules:

- lower-case;
- max length enforced;
- only safe characters: `a-z`, `0-9`, `-`, `_`, `.`;
- no `/`;
- no `..`;
- no path traversal;
- no shell interpretation assumptions;
- include a short stable suffix to avoid collisions.

The Helm chart must run `external-provisioner` with:

```text
--extra-create-metadata=true
```

The driver may use these CSI parameters when present:

```text
csi.storage.k8s.io/pvc/name
csi.storage.k8s.io/pvc/namespace
csi.storage.k8s.io/pv/name
```

Do not depend on PV name during `CreateVolume`; the PV may not exist yet.

Fallback when PVC metadata is absent:

```text
<logical-volume-id>
```

The final sanitized directory name must be persisted in the allocation record,
the returned volume context, and the driver-owned ownership record. It must not
be encoded directly in the compact volume handle; the handle only contains the
bounded logical volume ID and mapping hash.

The full path must always be joined and validated through a safe path helper.
Never concatenate paths with string interpolation for destructive operations.

### 6.12 Directory Lifecycle

The driver must manage directory lifecycle at the CSI controller level.

`CreateVolume` must:

1. validate and canonically normalize the request, choose
   `selectedCapacityBytes`, and compute `requestHash` for integrity;
2. derive deterministic `logicalVolumeID` from `driverName` and
   `CreateVolumeRequest.name`;
3. acquire the per-logical-volume lock and read the deterministic allocation
   ConfigMap before any provider refresh, `statfs`, or new placement work;
4. if the record exists, apply semantic compatibility and state handling
   immediately: return `Ready`, resume `Reserved` or `CreatingDirectory` using
   the persisted parent, or return the defined terminal/incompatible error;
5. only after a conclusive ConfigMap `NotFound`, refresh Scaleway metadata for
   configured active parents;
6. attach and mount candidate parent file systems on the controller pod's node
   when needed;
7. collect fresh `statfs` data for candidate parents;
8. choose a parent that satisfies logical capacity, actual free-space reserve,
   lifecycle, and node-compatibility requirements;
9. immediately before the selected-pool reservation, advance the generation of
   every currently `Idle` journal in the committed journal-set. This
   installation-wide CAS fence makes any older ambiguous `Begin` against a
   different pool conflict before a fresh placement can become durable;
10. while retaining the pool lock, CAS the permanent fixed-name journal for the
   selected pool from `Idle` to `Pending`; the journal contains a monotonic
   generation and the complete exact `Reserved` allocation about to be created;
   no allocation POST may be emitted until this transition is conclusively
   observed;
11. atomically create the allocation record named from `logicalVolumeID` in
    `Reserved` state, including the selected parent, selected capacity,
    normalized immutable parameters, and request hash;
12. if create returns `AlreadyExists` or an ambiguous API result, re-read that
    same deterministic record and follow step 4; a single immediate `NotFound`
    after an ambiguous create is not conclusive because the original request
    may still commit, so retry the exact create-if-absent operation a small
    bounded number of times while retaining the pool lock; never perform a
    second placement or reserve a different parent; after the first possibly
    emitted mutation, use an internal resolution context bound to the current
    leadership generation and process shutdown, not solely to the CSI caller;
    if bounded resolution still cannot prove the record present or absent,
    leave the journal `Pending`, mark that pool unresolved, and reject all new
    placements on it; once an earlier create is
    unresolved, a later `Forbidden`, invalid request, or other apparently
    definitive retry result does not clear that marker because it says nothing
    about whether the earlier POST can still commit; every successor must
    resolve the exact `Pending` intent before serving or admitting another
    placement on the pool;
13. only after the exact allocation is conclusively present, CAS the journal
    back to `Idle`; an ambiguous completion remains conservatively `Pending`;
14. move the allocation record to `CreatingDirectory`;
15. create the logical volume data directory;
16. write the driver-owned ownership record outside the mounted data directory;
17. apply ownership and mode to the logical data directory;
18. verify the ownership record, data directory, ownership, and mode;
19. move the allocation record to `Ready`;
20. return a CSI volume handle and volume context containing the immutable
    mapping.

A retry that finds no allocation but finds its own exact `Pending` intent must
reissue only the journal's canonical allocation and must not perform placement
again. Before any new placement, this lookup covers every journal in the
committed installation-wide journal-set, not only the pool supplied by the
retry. A matching intent with incompatible immutable parameters is rejected,
and multiple `Pending` intents for one `logicalVolumeID` are corruption that
fails closed. Once an exact allocation and an `Idle` journal are conclusively
observed, whether the final journal CAS was performed by this call or merely
observed after a lost response, the controller removes the process-local pool
marker under the pool lock. Startup, checkpoint resume, and quiesced terminal
reconciliation apply the same rule after proving every configured journal
`Idle`; the marker is never retained after the ambiguity it guards has ended.

If `CreateVolume` is retried while the allocation record is `Reserved` or
`CreatingDirectory`, the driver must resume and repair the operation. It must
not return success until the directory and driver-owned ownership record have
been verified. A retry must not require the parent to remain eligible for new
placement or have spare capacity; it validates and resumes the already-reserved
mapping.

Capacity and compatibility rules:

- if `required_bytes > 0`, `selectedCapacityBytes` is `required_bytes`;
- if the capacity range is absent or `required_bytes == 0`, v1 uses a 1 GiB
  logical reservation;
- if `limit_bytes > 0` and the selected capacity exceeds it, return
  `OutOfRange`;
- a replay is capacity-compatible when the persisted selected capacity is
  greater than or equal to the new `required_bytes` and, when non-zero, less
  than or equal to the new `limit_bytes`;
- pool, delete policy, UID, GID, mode, access type, supported access modes, and
  other immutable StorageClass parameters must be semantically equal after
  canonical normalization;
- map order and capability order must not affect compatibility or hashes;
- a compatible replay returns the original handle, immutable volume context,
  and persisted selected capacity;
- an incompatible replay returns `AlreadyExists` as required by CSI.

`requestHash` is a canonical integrity and diagnostics field. Exact hash
equality must never replace the semantic compatibility check required by CSI.
Its canonical payload contains exactly: original required bytes, original limit
bytes, selected capacity bytes, pool name, delete policy, directory UID, GID,
mode, filesystem access type, normalized filesystem type, and sorted supported
access modes. Volume content sources, non-empty mount flags, non-empty topology
accessibility requirements, and mutable parameters are unsupported in v1 and
must be rejected before hashing.

If `CreateVolume` is retried after the same logical record has reached a
terminal state, the driver must not silently reuse the old mapping as a new
active volume:

- `Ready` with a semantically compatible request returns the existing mapping;
- `Reserved` and `CreatingDirectory` with a semantically compatible request
  resume and repair creation;
- `Deleting` fails with a clear "deletion in progress" error;
- `Archived`, `Retained`, and `Deleted` fail with `AlreadyExists` or
  `FailedPrecondition` and a clear message that v1 permanently reserves the CSI
  volume name within the installation;
- an unrecoverable observation leaves the record in its last defined
  crash-resumable state and returns an actionable error; v1 never invents an
  unpaired generic failure state.

`NodeStageVolume` must verify the directory exists before mounting it. It must
not create or recreate the logical data directory for `Ready`, `Deleting`,
`Archived`, or `Retained` records. Directory creation and repair are
controller-owned operations limited to `Reserved` and `CreatingDirectory`
recovery paths.

Rationale:

- CSI `DeleteVolume` is a controller operation, not a node operation;
- archive/delete policies require filesystem access during controller delete;
- creating the directory during `CreateVolume` makes provisioning failures
  visible before pods are scheduled;
- relying only on node-side directory creation would leave cleanup ambiguous.

The controller therefore needs a tightly scoped filesystem operations path. The
v1 implementation must use one simple model: the active controller pod owns the
attach, mount, `statfs`, directory, archive, retain, and delete lifecycle for
parent file systems through a controller-owned mount root.

A separate filesystem-manager sidecar is out of scope for v1. It may be added
later only if the direct privileged controller model proves insufficient.

The chart must let operators constrain controller placement through
`nodeSelector`, `affinity`, and `tolerations`. The controller must fail fast if
it is scheduled on a node that cannot attach and mount the configured parents.

### 6.13 Ownership Record

Every logical volume must have an authoritative driver-owned ownership record
outside the user-mounted data directory:

```text
<basePath>/.sfs-subdir-csi/volumes/<logicalVolumeID>.json
```

The ownership record must contain:

```json
{
  "schemaVersion": "1",
  "recordKind": "detailed",
  "driverName": "file-storage-subdir.csi.urlab.ai",
  "installationID": "...",
  "activeClusterUID": "...",
  "volumeHandle": "...",
  "volumeHandleHash": "...",
  "logicalVolumeID": "...",
  "mappingHash": "...",
  "poolName": "standard",
  "parentFilesystemID": "...",
  "basePath": "/kubernetes-volumes",
  "basePathHash": "...",
  "directoryName": "...",
  "createVolumeRequestName": "...",
  "requestHash": "...",
  "originalRequiredBytes": 10737418240,
  "originalLimitBytes": 0,
  "selectedCapacityBytes": 10737418240,
  "normalizedCreateParameters": {},
  "deletePolicy": "archive",
  "directoryUid": 1000,
  "directoryGid": 1000,
  "directoryMode": "0770",
  "publishedNodeIDs": [],
  "state": "Ready",
  "revision": 1,
  "contentChecksum": "sha256:...",
  "createdAt": "..."
}
```

The ownership record is required for:

- idempotent create when the directory already exists;
- node-side directory validation before bind mount;
- `archive`;
- `delete`;
- `retain`;
- recovery after controller restart.

The ownership record is also the last-resort immutable recovery envelope when
both the allocation ConfigMap and PV are unavailable. It must therefore contain
the original create request name, request hash, original capacity range,
selected capacity, normalized immutable creation parameters, delete policy,
UID/GID/mode, and complete mapping fields. These values are copied at creation
and never inferred later from a current StorageClass or pool default. Recovery
recreates the allocation record with compare-and-swap before entering the normal
delete state machine.

Detailed ownership records have one closed lifecycle extension shared with the
allocation record. Fields are absent until their transition begins and then
become immutable for that operation:

```text
deleteOperationID
deleteOperation
deleteSourcePath
deleteTargetPath
deletePreparedAt
deleteRemoveStartedAt
deleteCompletedAt
archivedPath
retainedPath
quarantinePath
gcRequestID
gcRequestedMode
gcExpectedState
gcRequestedAt
gcOperationID
gcTargetPath
gcQuarantinePath
gcStartedAt
gcRemoveStartedAt
gcCompletedAt
```

Readers reject unknown lifecycle fields and state/field combinations outside
the transition table in section 10.6. An operation ID and its paths never change
after preparation.

`publishedNodeIDs` is lifecycle state rather than immutable creation input, but
it must be mirrored in the ownership record so namespace loss cannot erase the
last proof of a potentially live node mount. Destructive operations require the
allocation and ownership sets to agree and both to be empty. Publish-first and
unpublish-first crashes can produce the same divergent representation, so
generic or startup reconciliation must not infer an operation direction. Under
the per-volume lock it computes the deduplicated union, writes that union to the
allocation record first and the ownership record second, and never removes an
entry. Only an active `ControllerUnpublishVolume`, after its normal-node or
provider-fencing checks succeed, may remove a node entry.

The user-mounted data directory is untrusted. Workloads may delete, rename, or
modify any file inside it. Therefore a marker placed inside the data directory
may be written for diagnostics, but it must never be the sole proof used for
archive, delete, retain, or recovery.

Destructive operations must fail closed if the driver-owned ownership record is
missing or inconsistent with the allocation record, volume handle, or volume
context.

The ownership state is part of node-side authorization. Only `Ready` authorizes
new `ControllerPublishVolume`, `NodeStageVolume`, and `NodePublishVolume`
operations. Before a filesystem mutation for delete or GC, the controller must
atomically update the record to the prepared lifecycle state. `Deleting`,
`Archived`, `Retained`, and `Deleted` never authorize a new mount.
Unpublish and unstage remain idempotent in every state.

Ownership records follow the allocation lifecycle:

- `Ready`: the ownership record authorizes node staging and controller
  lifecycle operations.
- `Archived` and `Retained`: the ownership record must be preserved and updated
  with the terminal path and state. It remains required for manual GC.
- `Deleted`: the ownership record becomes a permanent compact, non-authorizing
  tombstone after the allocation `Deleted` tombstone is durable. It is never
  removed in v1.

The compact ownership tombstone remains at the same deterministic path and
contains exactly the durable identity needed to reject name reuse and reconstruct
a missing Kubernetes tombstone:

```text
schemaVersion = 1
recordKind = compactDeleted
revision
driverName
installationID
activeClusterUID
volumeHandleHash
logicalVolumeID
createVolumeRequestName
mappingHash
parentFilesystemID
basePathHash
directoryName
state = Deleted
deleteResult
updatedAt
deletedAt
contentChecksum
```

When populated by the matching detailed predecessor, the compact ownership
tombstone may additionally preserve only these terminal audit fields:
`deleteOperation`, `archivedPath`, `retainedPath`, `quarantinePath`,
`deleteOperationID`, `deleteCompletedAt`, `gcOperationID`, `gcTargetPath`,
`gcQuarantinePath`, and `gcCompletedAt`. Delete-created tombstones must include
`deleteOperationID`; GC-created tombstones must include `gcOperationID` and
`gcCompletedAt`. Unknown fields are rejected. These fields never authorize a
filesystem operation.

It never authorizes staging, publishing, filesystem mutation, or capacity
reservation. Startup must enumerate detailed and compact ownership records on
every currently configured parent. If a compact ownership tombstone on a
configured parent has no allocation ConfigMap after same-cluster recovery, the
controller reconstructs the matching `compactDeleted` allocation tombstone by
compare-and-swap before serving. Any identity or checksum mismatch fails
closed. Compact ownership records on an offline-decommissioned, unconfigured
parent are intentionally not read during normal startup or checkpoint; their
already-validated Kubernetes allocation tombstones remain the online evidence.

Offline parent decommission requires no reserving or non-Deleted allocation
records and no live detailed ownership records for that parent, plus the mount,
published-node, and attachment checks in section 6.9. Any schema-valid
non-reserving `Deleted` allocation tombstone may retain the historical parent ID
without blocking decommissioning. Compact `Deleted` ownership tombstones are
also historical evidence and do not block decommissioning, but the final
decommission validation must prove that every remaining compact ownership
tombstone matches its allocation tombstone before the parent becomes
unavailable. The parent-global claim remains unreleased.

### 6.14 Delete Policy

The driver must support three delete policies:

```text
archive
delete
retain
```

Default:

```text
archive
```

Behavior:

- `archive`: move the directory to `<basePath>/.archived/<directory>-<timestamp>`;
- `delete`: recursively delete only the validated directory;
- `retain`: leave the directory untouched and log the retained path.

The `delete` policy must be opt-in.

The driver must refuse deletion if:

- the path is empty;
- the path is `/`;
- the path equals the base path;
- the path is outside the configured base path after normalization;
- the path contains traversal;
- the target cannot be proven to belong to the volume being deleted.

Archive and retain must create durable tombstone records that remain part of
pool accounting until a documented garbage collection operation removes them.

`DeleteVolume` must follow a concrete idempotent state machine:

1. parse and validate the volume handle;
2. acquire the same per-logical-volume lock used by publish and lifecycle
   operations;
3. load the allocation record by `logicalVolumeID`;
4. validate that the record's `mappingHash` matches the handle;
5. if the record is `Deleted`, `Archived`, or `Retained`, validate enough
   persisted state to return idempotent success without touching a new path;
6. list VolumeAttachment objects for this driver and handle. If the read is
   unavailable, return a retryable error. If any attachment object remains,
   including one with a deletion timestamp, return `FailedPrecondition` because
   the volume may still be in use;
7. require the allocation and ownership `publishedNodeIDs` sets to agree and be
   empty. A missing or unavailable node is not proof that its bind mount
   disappeared. Apply the fenced-node recovery rule below before clearing a
   stale entry;
8. transition `Ready` to a prepared `Deleting` record before any filesystem
   mutation. This record must include a collision-resistant
   `deleteOperationID`, the delete operation, and the planned
   archive/quarantine/retain target path when the policy needs one;
9. atomically update the driver-owned ownership record to the matching
   non-authorizing `Deleting` state;
10. attach and mount the parent on the controller node if needed;
11. validate the safe path, persisted base path, parent ID, driver name, and
   driver-owned ownership record;
12. execute the configured delete policy;
13. persist matching terminal allocation and ownership state before returning
    success.

For the `delete` policy, terminalization writes the non-reserving allocation
`Deleted` tombstone first and then atomically replaces the matching detailed
ownership record with its permanent compact ownership tombstone. A crash after
the first write may complete only that exact ownership compaction after
validating the prepared delete operation, mapping, path, and terminal outcome;
it never repeats
filesystem deletion. `DeleteVolume` returns success only after both records
agree. `archive` and `retain` preserve matching detailed terminal ownership
records until explicit GC performs the same allocation-first, compact-owner-last
terminalization.

`ControllerPublishVolume` must acquire the per-logical-volume lock and accept
only `Ready`. A publish racing with deletion either completes before the
VolumeAttachment check and makes deletion fail, or observes `Deleting` and is
rejected. It must never publish after the prepared deletion state is durable.
For `SINGLE_NODE_WRITER`, an existing different node ID in the conservative
union of allocation and ownership `publishedNodeIDs` returns
`FailedPrecondition` before any provider attachment. Repeated controller publish
to the same node remains idempotent; the node service separately enforces the
single-target rule required by this access mode.

Every allocation and ownership record must contain the same deduplicated,
bounded `publishedNodeIDs` set. After the parent attachment is confirmed
`available` and before returning a successful `ControllerPublishVolume`, the
controller must add the request's exact node ID to the allocation record first,
then durably mirror it to the ownership record. It returns success only after
both records agree.

`ControllerUnpublishVolume` may trust the CSI node-unpublish/unstage ordering
only for a conclusive normal-node path: the Kubernetes Node still exists, is
`Ready=True`, has no `node.kubernetes.io/out-of-service` taint, and its current
CSINode registration advertises this driver's exact request node ID. This does
not independently prove an unmount; it establishes that the request follows
the normal CSI orchestration contract rather than Kubernetes forced-detach or
orphan cleanup. If any of that evidence is missing, stale, mismatched, or
unreadable, the controller must retain the durable node fence until the provider
fencing rule below conclusively proves that the old Instance can no longer
serve the mount. A deleted VolumeAttachment is never such proof.

For every node ID that is safe to clear, the controller removes the ownership
entry first and the allocation entry second. These write orders ensure that at
least one durable source remains conservative at every crash point. If a crash
leaves the sets divergent, generic reconciliation restores their union; the
retried unpublish then revalidates safety and removes the target again. This may
temporarily preserve or restore a stale fence but never discards one without
proof. Unavailable Kubernetes or provider reads return `Unavailable` without
clearing the fence; a conclusive but still-unfenced stale node returns
`FailedPrecondition` with the exact node and required operator action.

CSI permits `ControllerUnpublishVolume.node_id` to be empty and defines that
request as unpublish from all nodes. Under the per-volume lock, the target set
is the exact request node ID when non-empty, otherwise the deduplicated union of
the allocation and ownership `publishedNodeIDs` sets. The controller applies
the same normal-node or provider-fenced decision independently to every target.
It may durably clear proven-safe targets, but returns success only when both
records agree and every requested target is absent. It must never translate an
empty node ID into an empty-string entry or blindly erase the complete set.
A non-empty request node ID already absent from both agreeing records is an
idempotent success and does not require Kubernetes or provider reads.

The set is a conservative fence for non-graceful node loss; it is not a usage
counter. A stale entry may be cleared automatically only when the Scaleway API
conclusively proves either that the Instance no longer exists, or that the
Instance is non-running and this logical volume's parent attachment has been
removed from that Instance. For a deleted Instance, the fresh regional
attachment inventory must also prove that no orphan attachment remains. A
missing Kubernetes Node, deleted Pod, absent
VolumeAttachment, timeout, or unreachable Instance is not sufficient. Reads
that are unavailable or ambiguous keep the entry and block delete/GC. The
operations guide must require the operator to stop and detach or delete the
Instance before retrying the destructive operation. Once fencing is proven, the
controller clears the ownership entry first and allocation entry second using
the same crash-safe unpublish ordering; operators never patch either record
directly.

For the pinned Instance SDK, only terminal `stopped` and `stopped in place`
states qualify as non-running. `starting`, `stopping`, `locked`, unknown, and
unreadable states do not fence a process and must keep the published-node entry.

Policy-specific rules:

- `archive`: before any rename, persist `Deleting` with `deleteOperation =
  archive`, the validated `deleteSourcePath`, and a collision-resistant
  `archivedPath` under `<basePath>/.archived`. Then move the data directory once
  to that already-persisted path and persist `Archived`. Retries must handle:
  source present + archive path absent, source absent + archive path present,
  and source absent + archive path absent. The last case must fail closed with a
  manual recovery message.
- `retain`: persist `Retained` with `retainedPath`. It must not move data.
  Retries return success without moving data.
- `delete`: before any rename, persist `Deleting` with `deleteOperation =
  delete`, the validated `deleteSourcePath`, and a collision-resistant
  `quarantinePath` under `<basePath>/.deleted`. Then move the data directory to
  that already-persisted quarantine path and complete the lifecycle durability
  barrier from section 6.15. Before recursive removal, write
  `deleteRemoveStartedAt` to the allocation record first and then to the exact
  matching ownership record. Recursive removal is authorized only after both
  records agree on the operation ID, paths, and remove-start evidence. After
  removal, complete the `.deleted` directory durability barrier, persist
  `Deleted`, and release logical reservation. If a retry sees source absent,
  quarantine absent, and matching `deleteRemoveStartedAt` in both records, it
  must first complete the `.deleted` directory durability barrier and may then
  mark the record `Deleted`. Without matching evidence, it must fail closed.

`Archived` and `Retained` records continue reserving logical bytes until a
documented manual garbage-collection operation removes the data and updates the
record. `Deleted` records no longer reserve capacity, but remain as durable
tombstones. Normal `DeleteVolume` must never delete terminal allocation records.
An operation that cannot advance safely remains in its last persisted
crash-resumable state. It preserves the capacity semantics of that state and
returns a clear recovery action through the CSI error, Kubernetes Event, and
bounded diagnostic status. Operators never patch lifecycle state directly.

`DeleteVolume` missing-state semantics:

- an empty `volume_id` fails with `InvalidArgument`;
- a non-empty foreign or syntactically impossible ID that this driver could
  never have emitted returns idempotent success without creating a tombstone or
  performing Kubernetes, provider, or filesystem lookup;
- a parseable driver handle whose logical record exists but whose mapping hash
  conflicts fails closed; it is evidence of corruption, not an unknown volume;
- valid driver handles with a known terminal tombstone return success;
- valid driver handles with no allocation record, no PV attributes, and no
  ownership record return success after persisting a minimal non-reserving
  deleted-unknown tombstone only when every required Kubernetes and filesystem
  lookup completed successfully and conclusively reported absence;
- any unavailable, forbidden, timed-out, stale, or unmounted lookup returns
  `Unavailable` or `FailedPrecondition` without persisting deletion state;
- known inconsistent states fail closed when the driver has enough evidence
  that data may still exist but cannot prove a safe mutation target.

Manual GC must apply the same `VolumeAttachment` and `publishedNodeIDs` gates as
normal deletion. Renaming a directory does not revoke an existing bind mount,
so archive is not exempt from this rule.

The driver must never recover deletion state by heuristic scanning of
`.archived` or `.deleted`. It must rely on the allocation record, the
driver-owned ownership record, and deterministic/persisted delete target paths.

### 6.15 Filesystem Safety

All filesystem operations must go through a dedicated safety package.

Required behavior:

- validate every path component below `basePath` with no symlink following;
- verify the final bind source is a real directory, not a symlink;
- never follow symlinks during archive/delete;
- never cross mount boundaries during archive/delete;
- avoid check-then-act filesystem races for destructive operations, except for
  the narrowly bounded lifecycle-directory rename compatibility path below on
  a release-qualified filesystem that rejects `RENAME_NOREPLACE`;
- anchor destructive traversal under an already-open trusted `basePath`
  directory and use no-follow, descriptor-relative operations where the Go/Linux
  implementation allows it;
- resolve every kubelet, parent-target, and logical-directory component with
  `openat2(RESOLVE_BENEATH|RESOLVE_NO_SYMLINKS|RESOLVE_NO_MAGICLINKS)` or an
  equivalent `openat(O_NOFOLLOW)` walk; `os.Root` confinement alone is not a
  no-symlink guarantee because it follows links that remain beneath the root;
- retain the final descriptor until the mount, metadata update, or
  descriptor-relative removal completes; reopening a pathname is not
  authority;
- never overwrite an existing archive target;
- make archive target names collision-resistant by including volume ID,
  timestamp, and a random suffix;
- apply `chown` and `chmod` only to the logical volume root by default, not
  recursively;
- implement `delete` as a two-step flow:
  1. move the verified directory to a driver-owned quarantine directory;
  2. recursively remove only that verified quarantined directory.

On Linux, lifecycle traversal must use descriptor-relative `*at` operations and
`O_NOFOLLOW`. The opened parent mount's device and live kernel mount ID are the
boundary for every directory descriptor; checking only `st_dev` is insufficient
because a bind mount can use the same device. Mount IDs read from
`/proc/self/mountinfo` or fdinfo are namespace-local and may be reused after an
unmount, so they are not a durable mount generation. Every exact unmount proof
must additionally obtain `STATX_MNT_ID_UNIQUE` through an `O_PATH|O_NOFOLLOW`
descriptor, verify that the descriptor's fdinfo mount ID still equals the
coherent mountinfo entry, and compare the non-reusable statx generation
immediately before the destructive action. The final Linux unmount must remain
anchored to that authenticated mount object: v1 opens it with `open_tree(2)`
without `OPEN_TREE_CLONE`, revalidates both identities on the returned mount FD,
and moves that exact mount object with `move_mount(2)` to a generation-named
directory under the fixed private
`/run/scaleway-sfs-subdir-csi-mount-quarantine` mount. The chart supplies that
root as a dedicated `emptyDir` mounted only in the privileged driver container,
with no mount propagation. It is deliberately outside the parent and kubelet
mount roots. A container runtime may nevertheless initially expose that
dedicated mount as a slave of its own mount namespace. Before scanning or using
the quarantine, startup authenticates the exact mount through an `O_PATH`
descriptor, records its mountinfo ID and `STATX_MNT_ID_UNIQUE` generation,
applies `mount_setattr(AT_EMPTY_PATH, MS_PRIVATE)` through that descriptor, and
then proves that the same ID and generation became private. Any missing,
stacked, replaced, unreadable, or still-propagating root fails closed.

Linux shared-propagation ancestry can reject `move_mount` of an attached
kubelet mount with `EINVAL`. After that failed, non-mutating move, v1 repeats
the complete exact-generation and no-descendant proof and may atomically apply
`MNT_DETACH` directly to `/proc/self/fd/<authenticated-mount-fd>`. This fallback
never uses the public pathname as action authority and creates no
post-syscall quarantine state; any other move error fails closed. The normal
non-propagated path still uses the recoverable private quarantine protocol.
The same `EINVAL` fallback is permitted when rolling back a detached parent or
bind mount that this exact call created and still owns by its non-reusable
mount-generation FD. That rollback repeats the generation proof and never
selects a mount through a pathname.

The generation name is deterministic (`mnt-` plus the fixed-width hexadecimal
`STATX_MNT_ID_UNIQUE` value). If the process receives an error or is interrupted
after `move_mount`, the next preflight in the same container namespace scans
only that fixed private root, validates each closed-format name against the
mount generation observed through a no-follow descriptor, and detaches only an
exact match. Plain, malformed, stacked, mismatched, or unreadable entries fail
closed, except that a correctly named plain directory left after a container
restart is removed only after proving it is on the unchanged quarantine root.
A container restart destroys its private mount namespace, so a moved mount
cannot remain propagated into the host namespace; the Pod's `emptyDir` content
may persist as that now-unmounted deterministic directory. The original CSI
target pathname is never used after the FD proof. Lazy detach through the
authenticated quarantine mount FD is required
because the live mount FD itself pins the object; it detaches only that exact
mount while existing file references drain normally. The deterministic
directory is removed only after detach is proven complete.

Quarantine recovery is a precondition of normal retries, not only startup.
Before `NodeUnpublishVolume`, `NodeUnstageVolume`, parent cleanup, or any
provider detach treats an absent public target as success, the mutating mount
boundary authenticates and completes every recoverable quarantine entry.
Read-only inventories report a non-empty quarantine and fail closed; they never
clean it implicitly. Any malformed, stacked, foreign, or unreadable quarantine
keeps the component non-serving and blocks provider detach.

Mount inventories used for Node cleanup, decommission, and safe uninstall
retain every mount at or below the driver-protected parent, staging, publish,
and quarantine roots. Unknown descendants are classified as foreign and block
the destructive workflow; they are never filtered before the safety decision.

Immediately before `move_mount`, one final coherent mount-table and unique-ID
comparison may only veto the operation; it never changes which mount FD is the
action authority. After the move, the private quarantine must contain exactly
one layer with the expected generation. If a foreign top layer was stacked in
the syscall-sized interval and moved as a child of the authenticated mount, the
driver never detaches that tree: it moves the intact tree back to the empty
original target and fails closed. A failed restoration remains visible in the
private quarantine and prevents serving rather than deleting a foreign layer.

The v1 threat model covers kubelet, CSI calls, retries, crashes, and cooperating
driver generations serialized by Kubernetes rollout and the driver's locks. It
does not claim to defeat an unrelated, non-cooperating process with node-root
mount privileges that inserts a new mount in the syscall-sized interval between
the final proof and `move_mount`: such a process already has authority to
unmount or replace every workload mount on the node. The driver still detects
and restores a concurrent stack whenever it remains alive, and the privileged
Linux gate exercises that path. Deployments that permit arbitrary root mount
mutation outside kubelet and this driver are outside the v1 security model.

The production durable-record backend applies the same descriptor rule to
metadata mutation. The descriptor that proves the parent directory's device
and mount ID remains open through `openat`, `linkat`, lifecycle `renameat2` or
the qualified `renameat` fallback below, `unlinkat`, and the required directory
`fsync`; it is never closed and replaced by a second pathname walk before the
action. Privileged Linux tests inject a late nested mount at that boundary, and
real-backend barrier tests close and reopen the store after each
file/link/rename/directory-sync interruption to prove that only an old or new
complete generation is accepted.

Absence of kernel support for `open_tree(2)`, `move_mount(2)`,
`mount_setattr(2)`, `fsopen(2)`, `fsconfig(2)`, `fsmount(2)`, procfs mount-FD resolution, or
`STATX_MNT_ID_UNIQUE`, a zero/missing result, or disagreement between the two
identities keeps the node and controller non-serving and never falls back to
reusable mount IDs or pathname unmount. Startup performs a real scratch probe
inside the private quarantine mount and recovers any exact interrupted entry
before serving. Directory lifecycle rename first uses atomic
`renameat2(RENAME_NOREPLACE)`. Real Kapsule release evidence established that
Scaleway File Storage `virtiofs` returns `EINVAL` for this flag even though the
ordinary atomic rename operation is supported. Only `EINVAL`, `ENOSYS`, or
`EOPNOTSUPP` from the flagged call permits the compatibility path, and only for
the exact operation-bound archive or quarantine target already persisted under
the driver-only `.archived` or `.deleted` directory. The path must remain
collision-resistant, workloads must never receive either reserved directory,
and the single active controller plus the per-logical-volume lock must
serialize every cooperating lifecycle writer. After the failed flagged call,
the backend repeats the nested-mount and authenticated source-inode proofs,
proves the destination absent without following symlinks through its already
opened parent descriptor, performs descriptor-relative atomic `renameat`, then
proves the destination is the same authenticated source inode and the source
name is conclusively absent before the durability barriers run. A destination
that already exists, is unreadable, or appears in any cooperating retry is
never replaced and fails closed. The only remaining race requires an unrelated
process with root access to the protected parent mount; that actor is outside
the explicit v1 threat model above and already has direct authority to replace
or remove workload data. Parent claims and durable ownership metadata retain
their separate atomic `linkat` create-if-absent protocol and never use this
fallback. Any other flagged-rename error fails closed. Recursive removal
unlinks symlinks and other non-directory entries as entries, never follows
them, revalidates directory identity before removal, rejects any mount at or
below the tree, honors context cancellation between bounded directory batches,
and enforces a bounded open descriptor/depth limit. `os.RemoveAll` cannot
satisfy this contract and must not be used for lifecycle data.

The persisted v1 archive and quarantine target is derived exactly as:

```text
<basePath>/<.archived-or-.deleted>/<directoryName>-<logicalVolumeID>-<operationTimestamp>-<operationID>
```

`operationTimestamp` is the matching canonical UTC `deletePreparedAt` or
`gcStartedAt` value lowercased with `-`, `:`, and `.` removed. The record reader
must recompute this path from the immutable directory/logical identity and the
persisted operation ID and time. Merely finding an arbitrary direct child under
`.archived` or `.deleted` is not ownership proof and must fail closed. Retain
uses the exact original `<basePath>/<directoryName>` as both source and target.

Required reserved directories under the pool base path:

```text
.archived
.deleted
.sfs-subdir-csi
.sfs-subdir-csi/volumes
```

The driver must refuse to create user logical volumes using reserved names.
Reserved directories are driver-owned implementation state. They must not be
bind-mounted into workloads, and user data paths must never be allowed to
traverse into them.

The initial root-level parent claim uses the dedicated no-overwrite protocol in
sections 6.3 and 6.7. All per-volume metadata writes use one crash-durable
helper:

1. open a same-directory temporary file with exclusive creation and no symlink
   following;
2. write canonical JSON containing a schema version, monotonically increasing
   revision, and SHA-256 checksum over the canonical payload excluding the
   checksum field;
3. `fsync` the temporary file;
4. atomically create or replace the destination without following symlinks and
   without overwriting an unexpected owner;
5. `fsync` the containing metadata directory;
6. read back and validate the complete record before advancing the Kubernetes
   allocation state.

Crash retry uses the same deterministic operation-bound temporary path. It may
resume an existing temporary file or an already-installed destination only when
its complete canonical bytes exactly equal the generation prepared by that
operation. An exact completed destination causes the redundant temporary entry
to be unlinked and the metadata directory to be synced before success. Any
different bytes, unreadable entry, symlink, or unexpected generation fails
closed without replacing the destination.

For per-volume ownership records, the temporary operation ID is a deterministic
UUID derived from the logical volume ID and the destination ownership revision.
An exact retry of one transition therefore resumes the same temporary name
instead of creating an unbounded sequence of random orphan temporaries. The
root-level parent-claim temporary remains bound to the separately journaled
bootstrap attempt ID.

The implementation must never rewrite the only valid metadata generation in
place. Initial record creation uses no-overwrite semantics. Controlled updates
must verify the expected prior revision before replacement. Crash-point tests and
real Scaleway E2E must validate the required `virtiofs` rename and durability
behavior before v1 release.

Filesystem lifecycle mutations have a separate durability contract. A
successful syscall is not sufficient evidence for advancing Kubernetes or
ownership state to the next durable phase:

- after creating a logical data directory and applying its final UID, GID, and
  mode, sync the directory inode and its containing base directory before
  writing ownership `Ready`;
- after moving a source into `.archived` or `.deleted`, sync both the source and
  destination parent directories before recording rename completion;
- after recursively removing a quarantine directory, sync `.deleted` before
  writing terminal `Deleted` state or releasing capacity;
- after removing a bootstrap temporary claim during rollback, sync the parent
  filesystem root before clearing the bootstrap journal. A retry after an
  acknowledged or ambiguous root sync may accept only descriptor-confined,
  conclusive absence of that exact attempt-bound temporary, sync the root again,
  and then continue; an unreadable or ambiguous lookup is not absence;
- archive and GC use the same barriers as normal delete; no separate weaker
  path is allowed.

The implementation must use the smallest Linux durability primitive that has
the required semantics on the release-qualified `virtiofs` stack, normally
file/directory `fsync` and, only when required and proven, `syncfs` for the
mounted parent. An unsupported, ambiguous, or failed durability operation keeps
the prepared non-terminal state and returns a retryable or manual-recovery
error; it never advances optimistically. These barriers cover driver-owned
metadata and directory entries, not recursive flushing of arbitrary workload
data. Real node-failure tests must interrupt each lifecycle phase and verify the
post-restart source/target and durable-state pairing.

### 6.16 Mount Model

The node plugin must mount parent file systems and then bind-mount subdirectories
to individual CSI volume targets. A new parent is constructed as a detached
mount with `fsopen("virtiofs")`, an exact `source=<filesystemID>`
`fsconfig`, and `fsmount`; it is moved only to the already-opened target FD.
The detached generation remains owned through post-move graph validation, so a
renamed/replaced target rolls back only that new generation. There is no legacy
`mount(2)` pathname fallback.

A new bind is not attributed from a pathname
snapshot after `mount(MS_BIND)`: the implementation opens the authenticated
source, creates a detached clone with `open_tree(OPEN_TREE_CLONE)`, applies
private propagation and read-only state with `mount_setattr` before exposure
when requested, compares the source FD's unique generation with the already
validated parent/staging generation, obtains the clone's unique generation
from its owned FD, and
uses `move_mount` to install that exact object at the already-opened target.
Post-move validation must still match the requested parent, logical root, kind,
generation, filesystem, and read-only state. A replacement or other ambiguity
preserves the foreign mount and forbids destructive rollback.

Expected node flow:

```text
NodeStageVolume:
  1. parse volume handle and acquire the context-aware per-logical-volume node
     lock
  2. validate required immutable volume context and mapping hash
  3. validate the parent is configured for this driver and region
  4. while retaining the volume lock, use the per-parent node lock to mount or
     validate the parent SFS at a
     driver-owned path with virtiofs
  5. verify base path exists
  6. verify the logical volume directory exists and has a valid driver-owned
     ownership record in Ready state
  7. verify the local exact CSI node ID is present in the ownership record's
     publishedNodeIDs set
  8. apply ownership and mode if configured
  9. validate that the CO-provided staging path is an existing writable absolute
     directory under the configured kubelet staging tree; Kubernetes owns its
     creation and removal
  10. return idempotent success only when an existing staging mount has the
      exact logical-directory source, supported capability, and filesystem
      type; otherwise return `AlreadyExists`
  11. bind-mount the logical directory to the per-volume staging path; on
      failure, use a cancellation-independent five-second cleanup context to
      re-read the mount table and undo only an exact authenticated mount created
      by this call; leave a foreign, replaced, or stacked mount and the CO-owned
      directory intact

NodePublishVolume:
  1. parse the handle, acquire the same per-logical-volume node lock followed by
     the lock for the normalized target path, and revalidate the Ready ownership
     record and local published node ID
  2. inspect the live mount table and prove the staging path is the exact
     logical-directory bind mount backed by the expected configured parent
     `virtiofs` mount; an unmounted, replaced, stacked, or foreign staging path
     returns `FailedPrecondition`
  3. for `SINGLE_NODE_WRITER`, reject a different existing target for the same
     staged volume with `FailedPrecondition`; an identical retry at the same
     target remains idempotent
  4. validate that the target is an absolute path under the kubelet-managed
     plugin target tree, and create the target directory when absent
  5. if the target is already mounted with the same kernel-observable backing
     identity as the exact validated staging mount, the same supported durable
     capability, and the same read-only mode, return success; if any of those
     properties differ, return `AlreadyExists` without remounting
  6. bind-mount the staging path to the pod target path
  7. respect `NodePublishVolumeRequest.readonly`; read-only requests must result
     in a read-only bind mount
  8. if mounting or post-mount verification fails, use the same bounded
     cancellation-independent cleanup to unmount only an exact publish graph;
     remove the target directory only when this call created it and only after
     proving that it is not mounted and is empty

NodeUnpublishVolume:
  1. parse the handle, acquire the same per-logical-volume node lock followed by
     the lock for the normalized target path, and validate with no-follow
     resolution that the absolute target is under this driver's exact kubelet
     pod CSI target subtree
  2. when mounted, inspect the live mount table and prove that exactly one
     validated staging mount and the target share this volume's expected parent
     `virtiofs` source, device identity, and logical-directory root; a foreign,
     stacked, aliased, or mismatched mount returns `FailedPrecondition` without
     unmounting
  3. reconcile the private exact-unmount quarantine, then unmount the pod
     target when ownership is proven; an absent target is idempotent success
     only after the quarantine is proven empty and authenticated
  4. while the target lock still excludes every cooperating Publish/Unpublish,
     exact-unmount opens the now-visible underlying target with a no-follow
     descriptor and returns its device/inode identity; remove only that same
     empty inode through its authenticated parent descriptor. A historically
     absent mount does not authorize removal of an unrelated empty directory,
     and any replacement preserves the replacement and fails closed. Tolerate
     `NotFound`, but surface any other cleanup failure. The Node lock order is
     always volume, target, then parent; no path may acquire these locks in
     reverse

NodeUnstageVolume:
  1. parse the handle, acquire the same per-logical-volume node lock, and
     validate with no-follow resolution that the absolute staging path is under
     this driver's exact kubelet staging subtree
  2. when mounted, inspect the live mount table and prove the source is the
     requested logical directory under the expected configured parent; a
     foreign, stacked, aliased, or mismatched mount returns
     `FailedPrecondition` without unmounting
  3. reconcile the private exact-unmount quarantine, then unmount the
     per-volume staging path when ownership is proven; absence is idempotent
     success only after the quarantine is proven empty and authenticated
  4. leave creation and removal of the staging directory to Kubernetes
  5. keep the parent mounted unless safe cleanup is implemented
```

Linux mountinfo retains the bind mount's filesystem/device identity, root within
that filesystem, target, and mount options, but it does not retain the source
pathname originally passed to the bind-mount syscall. Node validation must use
the kernel-observable identity above and must not invent or trust a reconstructed
`sourcePath`. The exact staging target is validated independently, and both
mounts must resolve to the same configured parent and logical root. The complete
CSI capability is checked against the authenticated ownership record; the
read-only property is additionally checked in the live publish mount options.
The parser bounds the complete snapshot to 64 MiB in addition to its per-line
and entry-count limits. An empty mountinfo read is never interpreted as an empty
mount namespace, and duplicate mount IDs cannot form a coherent snapshot;
either condition fails closed before any absence or mount-boundary decision.
Linux `nsfs` entries are the only accepted exception to an absolute normalized
filesystem root: the parser accepts only the closed kernel namespace-handle
form `<lowercase-alphanumeric-or-underscore-name>:[<positive-decimal-inode>]`,
with a lowercase first character, when both filesystem type and source are
exactly `nsfs`. That opaque value is inventory only and can never authorize a
path comparison or mutation; every protected driver anchor still requires an
absolute normalized mount root.
For every unstacked driver target, the runtime opens the exact target without
following, matches the descriptor fdinfo mount ID to that snapshot, and records
the non-reusable `STATX_MNT_ID_UNIQUE` generation. Unpublish, unstage, rollback,
and uninstall compare that generation, not the reusable mountinfo ID. A stacked
target is rejected before generation comparison. This kernel primitive is a
mandatory v1 node compatibility requirement; release qualification must prove
it on every supported Kapsule/kernel line.

Parent mounts can remain mounted on the node. The pool is intentionally small,
and keeping parent mounts warm avoids unnecessary attach/mount churn.

Concurrent `NodeStageVolume` calls for logical volumes sharing one parent must
serialize parent mount creation and validate an existing mount's source,
filesystem type, and target before treating it as idempotent success. Per-volume
staging operations must reject an existing mount whose source, capability, or
filesystem type conflicts with the request. Publishing operations additionally
validate the requested read-only mode. A conflict returns `AlreadyExists`; the
driver never repairs it by remounting in place because that could alter a live
pod mount.

All four Node stage/publish/unstage/unpublish RPCs must acquire one
context-aware in-process lock keyed by parsed `logicalVolumeID` before any mount
table check or mutation. The lock serializes same-volume check-and-act
sequences, honors cancellation while waiting, and is released on every error.
The separate per-parent lock is acquired inside the volume lock only for shared
parent mount creation and validation. No Node code path may acquire these locks
in the opposite order. The implementation must never create stacked mounts as
a retry mechanism.

`NodeStageVolume` must fail with `FailedPrecondition` when a `Ready` volume's
data directory or driver-owned ownership record is missing or mismatched. It
must not run `mkdir -p` for a data directory that should already exist.

`NodeStageVolume` must not call authenticated public Scaleway APIs and must not
require Scaleway credentials. Node identity is initialized separately from the
local metadata service. The stage operation must treat local `virtiofs` mount
failure as proof that the parent is not available on the node after
`ControllerPublishVolume`.

### 6.17 Attach Model

The controller must attach parent file systems to the target Scaleway Instance
when Kubernetes schedules a workload on a node.

Expected controller flow:

```text
ControllerPublishVolume:
  1. parse volume handle
  2. load and validate a Ready allocation and ownership record under the
     per-logical-volume lock
  3. resolve node ID to Scaleway Instance ID and zone
  4. execute the attachment inventory, budget, attach, and readiness state
     machine from section 6.6
  5. persist node ID in the allocation record, then mirror it to the ownership
     record as defined in section 6.14
  6. return an empty publish context only after both records agree

ControllerUnpublishVolume:
  1. acquire the per-logical-volume lock
  2. build the target set from the exact request node ID, or from the union of
     both persisted sets when node_id is empty
  3. for each target, establish the conclusive normal-node path or apply the
     provider-fenced stale-node rule; ambiguous targets keep their fence
  4. for each safe target, remove it from the ownership set first
  5. remove the same target from the allocation set second
  6. verify both records agree and all requested targets are absent; a retry
     first reconciles any divergent sets to their union, then repeats the
     safety check and removal
  7. do not detach the parent except through the explicit provider-fenced
     stale-node cleanup path
```

Default behavior must be conservative:

- attach parents idempotently;
- keep parent file systems attached while the node is part of the cluster;
- do not detach a parent just because one logical PVC was unpublished.

Rationale: many PVCs share the same parent. Detaching a parent while another
logical volume still uses it would break unrelated workloads.

A future version can implement safe reference-counted detach. It is not required
for v1.

V1 must still handle stale node attachments operationally:

- rebuild driver-relevant `(nodeInstanceID, parentFilesystemID)` attachment
  inventory from the regional `ListAttachments` API and known Instances;
- refresh live attachment inventory before `ControllerPublishVolume` and before
  controller lifecycle mounts;
- expose metrics and warnings for stale attachments on deleted or unknown nodes;
- provide an explicit offline operator cleanup procedure. A live Instance must
  first unmount the parent and prove that no child bind mount remains. A lost
  Instance must be stopped and detached or deleted. Detach is allowed only after
  every allocation's `publishedNodeIDs`, PV, VolumeAttachment, staging path, and
  mount evidence has been reconciled as described in sections 6.14 and 6.9.

The provider appendix must state whether Scaleway automatically removes File
Storage attachments when an Instance is deleted. The driver must not assume that
behavior unless it is verified by documentation and e2e tests.

Exactly two v1 paths may call the Scaleway attach API:

- `ControllerPublishVolume`, for the workload node in the request;
- the active controller lifecycle reconciler, for the controller pod's current
  node when it needs parent access for reconciliation, `statfs`, create, delete,
  archive, retain, or GC.

Both paths must use the same section 6.6 inventory, exclusivity, budget,
idempotency, polling, timeout, and error-mapping implementation. The controller
must resolve its current Kubernetes node through Downward API `spec.nodeName`
and the matching CSINode node ID, then validate the Scaleway Instance and zone.
It must never use `SCW_DEFAULT_ZONE` as the controller target zone.
`NodeStageVolume` only mounts and bind-mounts after attachment has been requested
by one of these controller paths.

The v1 `ControllerPublishVolumeResponse.publish_context` is empty, matching the
provider contract in section 6.6. The node uses immutable `volume_context`, its
local identity, and host attachment state. Adding node-specific publish context
is a future wire-contract change and is not permitted in v1.

### 6.18 Topology and Scheduling

The v1 model is intentionally simple: all schedulable Linux workload nodes form
one homogeneous eligible set in the configured Scaleway region, and every one of
those nodes can attach every configured parent.

`volumeBindingMode: Immediate` is acceptable only under that invariant.

The driver must not silently rely on this invariant. Startup preflight must prove
that all schedulable Linux workload nodes and the controller node satisfy the
homogeneous contract. Otherwise the controller must remain unready and refuse
new CSI mutations. A development-only skip flag may bypass preflight, but the
chart and README must state that this mode is unsupported for production.
For each eligible node, the coherent preflight projection includes its exact
CSINode ID, live provider commercial type, and positive live
`MaxFileSystems`. The node ID zone must belong to the configured region, the
commercial type must be in the release-qualified list, and a missing or zero
live limit keeps the controller non-serving even when Kubernetes readiness and
registration are otherwise healthy.

The node DaemonSet must target `kubernetes.io/os=linux` and cover the same
workload-node set. Preflight must verify that every eligible node is Ready, has a
Ready node-plugin pod, and exposes this driver in its CSINode object before the
controller serves provisioning or publish operations.

Readiness alone does not prove that a rolling DaemonSet has the same parent
allowlist and schema contract as the controller. The chart must compute a
`nodeConfigGeneration` as SHA-256 over canonical node-relevant configuration:
driver name, immutable driver image digest, region, complete parent ID and
base-path mapping, exact sorted commercial-type allowlist, node mount roots,
supported access modes, and ownership schema reader/writer generation.
Development rendering uses its explicit empty-digest sentinel, while every
production release uses the required immutable digest. It
writes the bounded hash to a fixed Pod
annotation on both controller and node-plugin Pods. Before serving create or
publish, the controller must require one Ready node-plugin Pod with the exact
expected generation on every eligible node. A staggered Helm/DaemonSet rollout
therefore causes temporary unready state rather than provisioning a volume that
an old node configuration cannot mount.

Mixed compatible and incompatible workload node pools, a narrowed node
selector, and externally acknowledged scheduling are unsupported in production
v1. They require the topology-aware future design below rather than an
operator-acknowledgement boolean.

The controller must maintain this contract after startup. It must revalidate
eligible nodes when relevant Kubernetes Node objects change, or at a bounded
periodic interval. If the eligible node set becomes invalid, the controller must
expose a clear degraded condition, keep existing mounts safe, and refuse new
operations that depend on the homogeneous-node invariant until the operator
fixes placement or capacity.

V1 uses the configured controller metadata-refresh interval for that periodic
fallback. Every successful pass also validates that each eligible Instance has
no File Storage attachment outside the configured parent set, recomputes its
deduplicated configured-parent slot budget from the live `MaxFileSystems`
value, and publishes aggregate used/limit and ready/expected metrics. Event-
driven informer acceleration may be added later but cannot replace this bounded
full reread or weaken its fail-closed result.

If a future version supports heterogeneous topology-aware pools, it must add:

- `WaitForFirstConsumer`;
- `NodeGetInfo.accessible_topology`;
- `CreateVolumeRequest.accessibility_requirements`;
- `CreateVolumeResponse.accessible_topology`;
- parent topology persisted in the allocation record and immutable volume
  context without breaking the compact handle contract.

That topology-aware mode is out of scope for v1.

### 6.19 Access Modes

The v1 CSI access-mode surface is deliberately small and exact:

```text
SINGLE_NODE_WRITER
MULTI_NODE_MULTI_WRITER
```

`MULTI_NODE_MULTI_WRITER` is the URLAB RWX path. `SINGLE_NODE_WRITER` is a
narrower use of the same backend and is required for compatibility with the
pinned upstream CSI sanity suite. For a single-node volume, the controller must
reject publication to a second distinct node. On the node, only an identical
retry at the same target is allowed; a second different target returns
`FailedPrecondition`, as required for a non-multi-node access mode. V1 does not support
`MULTI_NODE_READER_ONLY`, `MULTI_NODE_SINGLE_WRITER`, or the newer single-node
mode variants. Read-only pod mounts remain supported through
`NodePublishVolumeRequest.readonly` without advertising a read-only access mode.

V1 has one fixed parent mount contract because many logical volumes share the
same `virtiofs` parent mount:

- `VolumeCapability` must use the mounted filesystem access type;
- `fs_type` must be empty or exactly `virtiofs` after normalization;
- non-empty CSI mount flags are unsupported and mapped according to the
  RPC-specific table below;
- non-empty `accessibility_requirements` are rejected because v1 does not
  advertise or implement topology;
- parent mounts always use one driver-owned, release-tested option set and never
  inherit options from an individual StorageClass or PVC;
- read-only behavior is implemented and verified on the per-volume bind mount,
  not by remounting the shared parent read-only.

The same validation must run in `CreateVolume`,
`ValidateVolumeCapabilities`, `ControllerPublishVolume`, `NodeStageVolume`, and
`NodePublishVolume`. An idempotent stage succeeds only when the existing mount
matches the complete supported capability. An idempotent publish must also
match the requested read-only mode.

Validation results are RPC-specific, as required by CSI:

| RPC | Unsupported well-formed capability | Malformed request | Existing incompatible publication/mount |
| --- | --- | --- | --- |
| `CreateVolume` | `InvalidArgument` | `InvalidArgument` | `AlreadyExists` for an incompatible replay |
| `ValidateVolumeCapabilities` | `0 OK` with `confirmed` unset and diagnostic `message` | `InvalidArgument` | not applicable; unknown volume is `NotFound` |
| `ControllerPublishVolume` | `FailedPrecondition` | `InvalidArgument` | `AlreadyExists` when already published incompatibly |
| `NodeStageVolume` | `FailedPrecondition` | `InvalidArgument` | `AlreadyExists` at the same staging target |
| `NodePublishVolume` | `FailedPrecondition` | `InvalidArgument` | `AlreadyExists` at the same target |

The implementation may share parsing and normalization, but it must not share
one hard-coded gRPC result across these RPCs. Tests assert the exact status and
the `ValidateVolumeCapabilitiesResponse` shape.

Malformed or foreign driver handles and non-empty immutable contexts with
missing, unknown, non-canonical, mismatched, or over-limit fields are request
validation errors and map to `InvalidArgument`, never `Internal`. The
`DeleteVolume` foreign-ID success rule remains its deliberate RPC-specific
exception and is resolved before generic error mapping.

For a second `NodePublishVolume` target, `SINGLE_NODE_WRITER` returns
`FailedPrecondition` whether the remaining arguments match or not.
`MULTI_NODE_MULTI_WRITER` permits another target when its request is otherwise
supported. These target-count semantics are node-local and are derived from the
validated live mount table, not a new durable control-plane record.

The v1 parent option set is empty: mount exactly
`mount -t virtiofs <filesystem-id> <parent-target>` with no user-configurable
mount flags. New parent-wide options require a later compatibility-tested
release and cannot be introduced through a StorageClass.

When a pod or CSI request asks for a read-only mount, `NodePublishVolume` must
publish the target path as read-only. Idempotent re-publish must preserve the
requested mode. If an existing mount has a conflicting read-only/read-write
mode, the driver must return `AlreadyExists` without changing the mount; it must
not silently return success with the wrong access mode or remount a live target.

### 6.20 Volume Expansion

Logical PVC expansion is out of scope for v1.

The v1 StorageClass must set:

```yaml
allowVolumeExpansion: false
```

Parent File Storage expansion is separate:

- operator expands parent SFS through Scaleway;
- driver observes the new parent size on next metadata refresh;
- new PVCs use the refreshed size.

Future logical PVC expansion can be added after v1. It must define allocation
record updates, retry behavior, shrink rejection, and reconciliation after
partial expansion failures before enabling `external-resizer`.

### 6.21 Reclaim Policy

The v1 StorageClass must use:

```text
reclaimPolicy: Delete
```

This tells Kubernetes to call `DeleteVolume` when the PVC/PV is deleted.

Actual data behavior is controlled by the driver's `onDelete` parameter:

- Kubernetes reclaim policy `Delete` + driver `archive` means the PV is deleted
  but data is archived.
- Kubernetes reclaim policy `Delete` + driver `delete` means the PV and data are
  both deleted.
- Kubernetes reclaim policy `Delete` + driver `retain` means the PV is deleted
  but data remains in place.

## 7. Configuration

### 7.1 Helm Values

The Helm chart must expose a clear `values.yaml`:

```yaml
driver:
  name: file-storage-subdir.csi.urlab.ai
  logLevel: info

installation:
  existingSecretName: scaleway-sfs-subdir-csi-identity
  idKey: installationID
  generateForDevelopmentOnly: false

scaleway:
  region: fr-par
  defaultZone: fr-par-1
  projectId: ""
  credentials:
    existingSecretName: scaleway-sfs-subdir-csi-credentials
    accessKeyKey: SCW_ACCESS_KEY
    secretKeyKey: SCW_SECRET_KEY

controller:
  replicas: 1
  leaderElection: true
  updateStrategy: Recreate
  maxConcurrentMutations: 10
  shutdownDeadline: 90s
  terminationGracePeriodSeconds: 120
  progressDeadlineSeconds: 3900
  leadership:
    enabled: true
    leaseDuration: 30s
    renewDeadline: 20s
    retryPeriod: 5s
  attachReadyDeadline: 10m
  metadataRefreshInterval: 5m
  parentMountRoot: /var/lib/scaleway-sfs-subdir-csi/controller-parents
  privilegedMounts: true
  nodeSelector:
    kubernetes.io/os: linux
  affinity: {}
  tolerations: []

node:
  parentMountRoot: /var/lib/scaleway-sfs-subdir-csi/parents
  kubeletPath: /var/lib/kubelet
  nodeSelector:
    kubernetes.io/os: linux
  affinity: {}
  tolerations: []

scheduling:
  allSchedulableLinuxNodesAreEligible: true
  requireHomogeneousEligibleNodes: true
  skipNodePreflightForDevelopmentOnly: false

pools:
  standard:
    basePath: /kubernetes-volumes
    selectionPolicy: least-allocated
    maxParentsPerEligibleNode: 2
    maxLogicalOvercommitRatio: "1.0"
    minFreeBytes: 10737418240
    minFreePercent: 5
    onDelete: archive
    directoryMode: "0770"
    directoryUid: "1000"
    directoryGid: "1000"
    filesystems:
      - id: ""
        name: sfs-subdir-pool-standard-01
        state: active
      - id: ""
        name: sfs-subdir-pool-standard-02
        state: active

storageClasses:
  - name: sfs-subdir-rwx
    poolName: standard
    defaultClass: false
    reclaimPolicy: Delete
    allowVolumeExpansion: false
    volumeBindingMode: Immediate
```

`controller.replicas` must be `1` in v1. The Helm chart must reject values
greater than `1` with a clear validation error unless a future release adds and
tests driver-internal leader election for every controller-owned mutation.
`controller.leaderElection` configures CSI sidecar leader election where
supported; it must not be documented as driver-controller HA in v1.
`controller.updateStrategy` must be `Recreate`. `controller.leadership` must be
enabled. The chart creates the narrow Lease RBAC, while the controller creates
and owns the runtime Lease through Kubernetes optimistic concurrency. Helm must
not render or patch the mutable Lease object because doing so could erase live
holder or recovery evidence. The Lease coordinates the single active process
but is not a storage fence. The chart must reject production values that narrow
the node-plugin/workload eligible set, disable the homogeneous-node preflight,
or remove `kubernetes.io/os=linux`. Compatible controller-only placement
constraints remain supported only when they leave at least two Ready candidate
nodes and do not bind the controller to one hostname. They may select a single
zone when the cluster itself is single-zone, provided at least two compatible
candidate nodes remain.

`node.parentMountRoot` is fixed to
`/var/lib/scaleway-sfs-subdir-csi/parents` in production v1. A development
override is unsupported for release values. `node.kubeletPath` may vary by
distribution but must be absolute, normalized, and resolve to a directory
disjoint from the fixed parent root. Node startup resolves symlinks and rejects
equality or ancestor/descendant overlap between the parent root and kubelet,
plugin socket, registry, pod, staging, or target trees before enabling
Bidirectional propagation.

The startup proof resolves every currently existing component of those paths.
For a dynamic path that does not exist yet, it resolves the deepest existing
ancestor and appends only the normalized missing suffix; a dangling symlink,
permission error, non-directory ancestor, or unreadable component is not
absence and fails startup. The parent root, kubelet root, CSI socket directory,
driver plugin directory, and pod target root must already exist as directories.
The registry and per-driver staging trees may be absent, but every existing
component is still resolved and checked. Per-RPC descriptor-relative no-follow
validation remains mandatory because startup evidence cannot authorize a path
created or replaced later.

Lexical and symlink resolution alone cannot detect two different container
paths backed by overlapping hostPath bind sources. The node therefore parses
one fresh bounded `/proc/self/mountinfo` snapshot before serving. It requires
exactly one mount entry—never zero or a stack—for the parent root, kubelet
`plugins` root, kubelet `pods` root, and CSI socket directory. The parent,
plugins, and pods entries must carry a `shared:<peer-group>` optional field,
proving the chart's Bidirectional propagation contract reached the process
mount namespace. For each protected anchor, the process compares the kernel
major/minor device identity and normalized mountinfo filesystem root against
the parent root. Equal or ancestor/descendant source roots on the same device
are an alias and fail startup even when their container target strings are
disjoint. A different device remains a distinct source. Missing, malformed, or
ambiguous mount identity keeps the node CSI endpoint non-serving. The startup
snapshot also probes `statx(STATX_MNT_ID_UNIQUE)` through the opened parent-root
anchor. Before either privileged CSI endpoint becomes ready, the kernel mount
preflight uses the chart-provided private quarantine to exercise `open_tree`,
detached bind cloning, `mount_setattr`, `move_mount`, mount-FD identity, and
exact detach on driver-owned scratch directories, then removes those
directories. A kernel, runtime security profile, or container configuration
that blocks any required primitive is not compatible with v1 even if ordinary
mountinfo parsing succeeds.

#### 7.1.1 Runtime Configuration Projection

The chart renders one non-secret `config.json` key in its ConfigMap and mounts
that same file read-only into the controller and node driver containers. JSON is
deliberate: the process can use the standard library plus the driver's strict
duplicate-key decoder without adding a YAML parser to the runtime dependency or
supply-chain surface.

The runtime document has closed `schemaVersion: "1"`, is limited to 1 MiB before
decode, and contains the complete process configuration projection: mode,
driver/log identity, controller namespace and Helm release, exact chart version,
the sorted rendered-image commitment described below, installation Secret
reference, non-secret provider scope, credential Secret/key references,
controller deadlines and leadership timing as positive integer seconds, node
paths, scheduling contract, pools, StorageClasses, and the exact
`nodeConfigGeneration`. Unknown or duplicate fields, trailing JSON, malformed
types, non-canonical UID/GID or ratio values, overflowed durations, an unfixed
Lease name, and a generation mismatch all fail before Kubernetes, provider, or
filesystem mutation.

`renderedImages` is a closed sorted array with the exact names `driver`,
`external-attacher`, `external-provisioner`, `liveness-probe`, and
`node-driver-registrar`. Production requires a lowercase immutable
`sha256:<64 hex>` digest for every entry. Development rendering may project an
empty digest so local chart structure can be tested, but such a runtime is not
checkpoint- or recovery-eligible. The controller uses this independently
rendered projection, rather than environment guesses or mutable tags, when it
creates and verifies checkpoint manifests.

Secret values never enter the ConfigMap or the decoded configuration object.
Both components inject `INSTALLATION_ID` from the external identity Secret. The
controller additionally requires non-empty `SCW_ACCESS_KEY` and
`SCW_SECRET_KEY` and requires the environment-projected project, region, and
default zone to exactly equal the decoded Helm projection, preventing a
one-sided Deployment/ConfigMap change from selecting another provider scope.
The node process fails startup if either authenticated Scaleway credential
environment key is present. Runtime errors describe only the missing or
mismatched key and never include credential values.

Helm tests must extract the rendered JSON, validate its closed projection, and
load that exact file through both production decoder modes. Unit tests cover
ConfigMap-style symlink projection, size bounds, cancellation, strict JSON,
environment authority, duration conversion, and generation tampering.

The driver process command line is closed and has no implicit runtime defaults:

```text
scaleway-sfs-subdir-csi --mode=<controller|node> --endpoint=unix:///absolute/csi.sock --admin-endpoint=unix:///run/scaleway-sfs-subdir-csi/admin.sock --config=/absolute/config.json --live-address=<numeric-host:port> [--metrics-address=<numeric-host:port>]
```

Every required flag appears exactly once. Positional arguments, short aliases,
duplicates, unknown flags, empty or multi-line values, TCP CSI endpoints,
relative/non-normalized/overlong Unix paths, a non-fixed admin endpoint, and
DNS listener names are rejected before opening the configuration. The CSI and
admin sockets must use different directories, and live/metrics listeners must
use distinct non-zero canonical numeric ports. After flag validation, the
process loads the bounded configuration with the selected component's
credential boundary before constructing a listener or adapter. `--version`
and `--help` are the only standalone invocations.

### 7.2 StorageClass Parameters

Supported StorageClass parameters:

```yaml
parameters:
  poolName: standard
  onDelete: archive
  directoryMode: "0770"
  directoryUid: "1000"
  directoryGid: "1000"
```

Rules:

- `poolName` is required, must reference an existing pool, and must be a bounded
  DNS-safe name.
- `onDelete` defaults to the pool policy and must be exactly `archive`, `delete`,
  or `retain`.
- directory ownership and mode default to the pool policy;
- UID and GID must be base-10 integers in the inclusive range 0 to 2147483647;
- mode must match `0[0-7]{3}` and is interpreted as octal;
- the external-provisioner metadata keys
  `csi.storage.k8s.io/pvc/name`,
  `csi.storage.k8s.io/pvc/namespace`, and
  `csi.storage.k8s.io/pv/name` are accepted only as non-policy naming metadata
  injected by `--extra-create-metadata=true`;
- every other unknown parameter is rejected with `InvalidArgument` rather than
  silently ignored;
- StorageClass parameters must never contain credentials.

The three metadata keys are not user policy and are not compared as immutable
StorageClass parameters during an idempotent replay. Their sanitized result is
captured by the persisted `directoryName` and mapping hash. Missing metadata
uses the deterministic fallback from section 6.11.

### 7.3 Credentials

Credentials must come from Kubernetes Secrets. Non-secret provider scope must
have one different, authoritative source: Helm values.

Environment variable sources:

```text
SCW_ACCESS_KEY          <- credentials Secret
SCW_SECRET_KEY          <- credentials Secret
SCW_DEFAULT_PROJECT_ID  <- scaleway.projectId
SCW_DEFAULT_REGION      <- scaleway.region
SCW_DEFAULT_ZONE        <- scaleway.defaultZone
```

The credentials Secret must contain only the access and secret key. Project,
region, and default zone must not also be accepted from that Secret, because two
configuration authorities could select the wrong Project or region. The chart
must render non-secret scope directly from validated values and must never put
credentials in ConfigMaps.

`installation.idKey`, `scaleway.credentials.accessKeyKey`, and
`scaleway.credentials.secretKeyKey` must each contain 1 to 128 characters from
the Kubernetes Secret data-key set `[-._A-Za-z0-9]`. Credential key names must
also be distinct. Invalid projections are rejected by both Helm schema and
runtime validation before any Secret read or provider operation.

Scaleway File Storage parent metadata is regional. Instance attachment
operations are node-specific and must use the zone of the target Scaleway
Instance. `SCW_DEFAULT_ZONE` may be required to initialize the Scaleway Go SDK,
but it must never be used as the workload node zone for
`ControllerPublishVolume`. The controller must derive the node zone from the CSI
`NodeId` or from validated Scaleway node metadata for the target node.

In v1, Scaleway credentials must be mounted only into the controller container
or controller-side components that strictly need Scaleway API access. The node
plugin must not receive `SCW_ACCESS_KEY`, `SCW_SECRET_KEY`, or equivalent
Scaleway credentials.

Rationale:

- only the controller calls Scaleway APIs for metadata and attach;
- the node plugin may read the local unauthenticated Instance metadata service,
  but `NodeStageVolume` must not call authenticated public Scaleway APIs;
- the node plugin is a privileged DaemonSet and should not carry cloud
  credentials it does not need.

The chart must always reference an existing credential Secret. It must not
accept raw access or secret keys as Helm values or render a credential Secret,
including for local development. Tests must ensure credentials never appear in
ConfigMaps, StorageClasses, logs, or non-Secret rendered resources.

The README must document the Project-scoped IAM permission sets and their known
granularity limitation exactly as defined in section 6.6.

### 7.4 Helm Chart Contract

The Helm chart is part of the production surface. It must be concrete enough to
install the driver outside URLab without private assumptions.

Required stable values surface:

```yaml
image:
  repository: ""
  tag: ""
  digest: ""
  pullPolicy: IfNotPresent

sidecars:
  operationTimeout: 12m
  externalProvisioner:
    image: registry.k8s.io/sig-storage/csi-provisioner
    tag: ""
    digest: ""
    workerThreads: 5
  externalAttacher:
    image: registry.k8s.io/sig-storage/csi-attacher
    tag: ""
    digest: ""
    workerThreads: 5
  nodeDriverRegistrar:
    image: registry.k8s.io/sig-storage/csi-node-driver-registrar
    tag: ""
    digest: ""
  livenessProbe:
    image: registry.k8s.io/sig-storage/livenessprobe
    tag: ""
    digest: ""

imagePullSecrets: []
resources:
  controllerDriver:
    requests: {cpu: 100m, memory: 128Mi}
    limits: {cpu: "1", memory: 512Mi}
  externalProvisioner:
    requests: {cpu: 50m, memory: 64Mi}
    limits: {cpu: 500m, memory: 256Mi}
  externalAttacher:
    requests: {cpu: 50m, memory: 64Mi}
    limits: {cpu: 500m, memory: 256Mi}
  nodeDriver:
    requests: {cpu: 50m, memory: 64Mi}
    limits: {cpu: 500m, memory: 256Mi}
  nodeDriverRegistrar:
    requests: {cpu: 20m, memory: 32Mi}
    limits: {cpu: 200m, memory: 128Mi}
  livenessProbe:
    requests: {cpu: 10m, memory: 32Mi}
    limits: {cpu: 100m, memory: 64Mi}
probes:
  startup:
    periodSeconds: 10
    failureThreshold: 360
  readiness:
    periodSeconds: 10
  liveness:
    periodSeconds: 20
podLabels: {}
podAnnotations: {}
priorityClassName: system-cluster-critical
serviceAccounts:
  controller: {create: true}
  node: {create: true}
rbac:
  create: true
metrics:
  enabled: true
  service:
    enabled: true
```

The chart must render, at minimum:

- controller Deployment with `strategy: Recreate`;
- node DaemonSet;
- CSIDriver;
- StorageClass objects;
- controller ServiceAccount and RBAC;
- node ServiceAccount and RBAC;
- RBAC for the controller-owned runtime leadership Lease;
- sidecar leader-election RBAC where needed;
- metrics Service when metrics are enabled;
- startup, readiness, and liveness probes with startup timing that permits the
  bounded provider and reconciliation preflight;
- CSI socket volumes and hostPaths required by Kubernetes CSI.

The chart must explicitly define controller and node security contexts, hostPath
mounts, CSI socket paths, kubelet paths, and mount propagation. It must not use
wildcard hostPath mounts.

The chart must include `values.schema.json` for structural types and numeric
ranges, plus template validation for cross-field rules. It must reject at least:

- missing production identity or credential Secret references;
- empty project, region, driver name, pool name, parent ID, or StorageClass name;
- duplicate parent filesystem IDs across pools;
- a missing pool referenced by a StorageClass;
- root, relative, non-normalized, or reserved parent-owner-namespace base paths;
- a production `node.parentMountRoot` different from the fixed dedicated path,
  or any kubelet/socket/registry/pod/staging path that is relative,
  non-normalized, equal to, an ancestor of, or a descendant of that root;
- any configured CSI context field that can exceed 128 UTF-8 bytes, or any
  configuration whose fixed context values already make a valid 4-KiB
  `volume_context` impossible; runtime validation covers request-derived values;
- unsupported delete policy, directory mode, UID, or GID;
- non-positive capacity ratios or invalid free-space percentages;
- more than one default StorageClass;
- `controller.replicas != 1`, a non-`Recreate` controller strategy, disabled
  leadership, or narrowed production node-plugin/workload placement; compatible
  controller-only placement constraints remain allowed;
- invalid leadership timing or a sidecar operation timeout that is not greater
  than `controller.attachReadyDeadline` by at least one minute;
- a mutation limit below one or above the release-tested maximum of 10;
- sidecar worker counts below one or above the release-tested controller
  mutation limit;
- `controller.shutdownDeadline` without at least 30 seconds of additional pod
  termination grace, or a Deployment progress deadline that cannot cover the
  startup-probe budget plus a five-minute margin;
- empty version tags or missing/malformed `sha256` digests for the controller
  or any sidecar in release values.

Every chart duration uses the canonical string form `[1-9][0-9]{0,5}(s|m|h)`.
The six-digit bound keeps Helm integer conversion, unit multiplication, the
rendered positive-seconds projection, and Go `time.Duration` conversion inside
their common non-overflowing range. Longer values are rejected by schema before
template arithmetic; they must never wrap into zero or a negative runtime
deadline.

Lexical path validation compares each configured path with its POSIX-cleaned
form. This includes terminal `/.` and `/..` components, not only traversal
components surrounded by slashes. In production, required node affinity is
rejected because it can narrow the asserted all-schedulable-Linux-node set.
Controller placement rejects a standard hostname pin because it leaves no
rescheduling candidate. Zone and region topology constraints are allowed: they
remain subject to the runtime startup proof of at least two distinct Ready
compatible candidate nodes, and all those candidates may be in one zone.

Resource requests and limits are required separately for every controller and
node container. The defaults above are conservative starting points and remain
operator-overridable; release E2E and scale tests must prove that the controller
stays within its default 512 MiB memory limit at the supported scale envelope.
The default startup-probe budget is intentionally 60 minutes so slow provider
attachment or large reconciliation does not cause a restart loop. Readiness,
operation deadlines, and actionable status expose progress; liveness must not be
used as a short provisioning timeout.

The chart must pass the same explicit `sidecars.operationTimeout` to the
`--timeout` flag of external-provisioner and external-attacher. Their upstream
defaults are much shorter than the driver's ten-minute attachment deadline and
must not be relied upon. The production default is 12 minutes. A caller
cancellation is still honored, and state-driven retries remain required for
operations such as recursive deletion that can exceed one sidecar attempt.

The chart must also pass the explicit `workerThreads` values to the provisioner
and attacher `--worker-threads` flags. The driver enforces the authoritative
process-wide mutation limit; sidecar worker counts only prevent unnecessary
queue pressure and do not replace that gate.

Before v1, the release notes must include a compatibility table with:

- supported Go toolchain version;
- supported Kubernetes versions;
- supported Kapsule versions tested by e2e;
- CSI spec module version;
- pinned `kubernetes-csi/csi-test` module version;
- Scaleway SDK version;
- sidecar image versions and required flags;
- minimum and maximum tested sidecar versions;
- upgrade test coverage from the previous released chart to the candidate chart.

The v1 implementation baseline is pinned to the dependency set used by the
official Scaleway File Storage CSI reference at commit
`2ede1238d63cf03b575eacee2ef1449be9106387`, except for this driver's newer Go
toolchain requirement:

| Contract | Pinned development baseline |
| --- | --- |
| Go toolchain | `1.26.0` |
| CSI specification Go module | `github.com/container-storage-interface/spec v1.12.0` |
| CSI sanity module | `github.com/kubernetes-csi/csi-test/v5 v5.4.0` |
| gRPC Go | `google.golang.org/grpc v1.80.0` |
| Kubernetes Go modules | `k8s.io/api`, `k8s.io/apimachinery`, and `k8s.io/client-go v0.35.3` |
| Scaleway SDK | `github.com/scaleway/scaleway-sdk-go v1.0.0-beta.36` |

This table pins source and test contracts; it is not by itself a claim that a
Kubernetes or Kapsule release is supported. The public support matrix still
requires the kind, Kapsule, sidecar, upgrade, and real-provider evidence listed
in this specification. Updating any row is a deliberate compatibility change
that must update the retained test evidence in the same release.

Production releases must pin explicit image tags and non-empty immutable image
digests. Operators may override images, but release mode must reject a tag-only
override and the README must state that untested sidecar/image combinations are
outside the support contract.

### 7.5 Safe Uninstall Contract

Direct `helm uninstall` is unsupported because Helm may remove Lease RBAC before
the controller finishes its graceful release and may remove node Pods while
warm parent mounts still exist. The project must provide an idempotent
`csi-admin uninstall prepare` workflow that runs before Helm deletion.
`csi-admin` is an operator-side binary using the caller's kubeconfig; it invokes
the narrow controller/node admin endpoints with `kubectl exec` and scales
workloads with the operator's existing authorization. The runtime controller
and node ServiceAccounts receive no workload-write or pod-exec permission for
this workflow.

1. inventory every driver PV, PVC, Pod target, VolumeAttachment, allocation,
   published-node fence, staging mount, and node target. The command never
   deletes user workloads or PVCs implicitly; it prints the exact blockers and
   requires the operator to remove them through normal Kubernetes workflows;
2. wait until Kubernetes has completed every normal node unpublish and unstage,
   and reject preparation while any live driver PV, non-terminal allocation,
   VolumeAttachment, published-node fence, staging bind mount, or workload
   target remains;
3. quiesce the controller through the same global barrier used by checkpoint;
   the active uninstall request owns the barrier and is the only admin mutation
   allowed to execute the remaining ordered cleanup. Under that drained
   barrier, resolve every exact `Pending` intent, require the exact committed
   journal set and every permanent pool journal to be `Idle`, and then rebuild
   the allocation/provider target inventory. A reservation resolved to
   `Reserved` is a normal uninstall blocker;
4. invoke the node-local admin command with `kubectl exec` on every eligible
   node-plugin Pod. Each mutating node prepare first removes Node readiness,
   closes one process-wide Node mutation gate, and waits for every admitted
   `NodeStageVolume`, `NodePublishVolume`, `NodeUnpublishVolume`, and
   `NodeUnstageVolume` call to finish. Only then may it inspect the final mount
   graph, prove no child bind mount remains, and unmount the exact configured
   driver parent roots. The gate remains closed for that Pod; normal service
   requires a fresh node-plugin process;
5. delete the exact node DaemonSet with a Kubernetes UID precondition while its
   RBAC still exists, wait for every node-plugin Pod selected by that exact Helm
   release to terminate, and retain the successful exact per-node unmount
   inventory from step 4. Kubernetes does not expose a `scale` subresource for
   DaemonSets; exact preconditioned deletion is therefore the v1 stop primitive.
   A retry validates the name and UID frozen in the same request's progress,
   accepts absence only after the controller is quiesced for that request, and
   still proves that no matching Pod survives. With workloads removed and node
   plugins stopped, no driver component can remount a parent;
6. unmount the controller's exact parent roots, detach each configured parent
   from every installation Instance, and poll both paginated regional and
   Instance inventories to conclusive absence. This is the only normal
   production path, besides the explicitly fenced recovery paths, that
   authorizes detach;
7. request graceful controller release, verify the exact release marker, scale
   the controller to zero while its Lease Role and RoleBinding still exist, and
   wait for Pod termination;
8. only then permit `helm uninstall`. Terminal allocation ConfigMaps, external
   identity/checkpoint Secrets, parent claims, and user data are never deleted
   implicitly.

The command is dry-run capable, resumable by request ID, and fails closed on any
unreadable mount or provider inventory. The chart does not use a privileged
pre-delete hook; the explicit command keeps operator authorization and audit
outside the runtime driver. Reinstall requires the same identity Secret and
validates all retained claims before serving.

After the controller accepts quiesce, the operator binary persists one bounded,
closed-schema progress ConfigMap named from a SHA-256 of namespace, Helm release,
and request ID. It records only immutable workload identities and already
validated phase evidence; it contains no kubeconfig, credential, Secret value,
or workload data. Every update uses `resourceVersion` compare-and-swap. A retry
must strictly validate the ConfigMap, its request/release labels, and its sole
ownerReference to the exact controller Deployment UID before using cached
evidence. The caller's Kubernetes authorization, not a runtime ServiceAccount,
creates and updates this object. The ownerReference retains progress while the
controller is scaled to zero and lets Kubernetes garbage-collect it only when
the audited later Helm removal deletes that exact Deployment.

The uninstall audit result must record the request ID, chart/driver/admin
versions, fixed Lease name and UID, exact nodes and Instances checked, unmounted
paths, detached parent IDs, final inventory hashes, and completion timestamp.
Retries with the same request ID resume from observed state and never skip a
previously incomplete proof.

Every node and controller unmount result binds one configured parent ID to the
exact normalized `<configured-parent-mount-root>/<parentID>` path. A bare list
of parent IDs or paths is insufficient because it cannot prove which identity
was cleared. The final audit carries the configured node and controller roots,
the complete parent set, per-node bound unmount evidence, controller bound
unmount evidence, and the exact checked node-to-Instance projection. Repeated
path strings on different nodes remain distinct per-node evidence rather than
being collapsed into an ambiguous installation-wide path list.

### 7.6 Admin CLI Release Contract

`csi-admin` is a supported release artifact, not a source-tree helper. Every
release must publish versioned static binaries for the documented operator
platforms, SHA-256 checksum files, an SBOM, and build provenance beside the Helm
chart and driver image. The README must show checksum verification before use.

The binary must expose `csi-admin version` and perform a version handshake with
the controller/node admin endpoint before any mutation. V1 accepts only the
same major admin protocol and an explicitly declared compatible minor range; an
unknown or incompatible protocol fails before quiesce, GC, checkpoint, upgrade,
or uninstall changes state. Audit output records both versions.

The controller/node-local v1 wire protocol uses a Unix socket only; it is never
exposed by a Kubernetes Service or TCP listener. Each connection carries
exactly one request and one response, framed by a four-byte unsigned big-endian
payload length followed by closed-schema UTF-8 JSON. Empty frames and frames
larger than 2 MiB are rejected before JSON allocation. The 2 MiB limit permits
the maximum 1 MiB checkpoint manifest plus its bounded prepare-result envelope;
detailed inventories never cross this control channel. The request command set
is closed to `handshake`, `checkpoint.prepare`, `checkpoint.resume`,
`decommission.inspect`, `decommission.prepare`, `decommission.quiesce`,
`decommission.cleanup`, `decommission.release`, `gc.submit`,
`upgrade.preflight`, `uninstall.inspect`, `uninstall.prepare`,
`uninstall.quiesce`, `uninstall.cleanup`, and `uninstall.release`.
`uninstall.inspect` is the node-local read-only mount inventory used by
dry-run. `uninstall.prepare` is the node-local exact-unmount phase. The
remaining uninstall commands are separate controller-local phases; separation
is required because the controller ServiceAccount deliberately has neither Pod
exec nor workload-write authority. GC and upgrade carry one command-specific
JSON object. Every decommission phase carries only the same exact
`parentFilesystemID` object; all other v1 commands carry no payload. Unknown commands, fields,
duplicate keys, non-object command payloads, mixed handshake/mutation
envelopes, and trailing JSON are rejected before a workflow handler runs.

The fixed in-container endpoint is
`unix:///run/scaleway-sfs-subdir-csi/admin.sock` for both controller and node
driver processes. `/run` is an `emptyDir` mounted only into the driver
container; the admin socket must never be placed in the CSI socket volume,
kubelet plugin hostPath, or a sidecar mount. The driver creates its private
subdirectory with mode `0700` and socket with mode `0600`, rejects symlink or
non-socket replacement, revalidates the socket inode immediately before
unlink, and unlinks only its exact stale socket inside that private directory.
The released in-image `/usr/local/bin/csi-admin` internal
command connects locally after an operator-authorized `kubectl exec`; the
operator-side CLI owns kubeconfig use and multi-Pod orchestration. Neither
endpoint is an authentication substitute for Kubernetes exec authorization or
the protocol/leadership checks below.

The controller additionally creates the private controller-only streaming
socket
`unix:///run/scaleway-sfs-subdir-csi/checkpoint-export.sock` in the same
`0700` directory with mode `0600`. It is not a second control endpoint: it
accepts only a negotiated checkpoint-export prelude containing the exact
prepared ticket, then emits one bounded archive stream. Node Pods never create
this socket. The stream server serializes package construction with prepare
and resume through the same coordinator operation lock, so resume cannot open
the mutation gate during an export reread.

The closed in-container client grammar is:

```text
csi-admin local [--endpoint=unix:///run/scaleway-sfs-subdir-csi/admin.sock] [--timeout=30s] handshake
csi-admin local [global flags] checkpoint prepare --request-id=<uuid>
csi-admin local [global flags] checkpoint export --request-id=<uuid> --ticket-stdin=true
csi-admin local [global flags] checkpoint resume --request-id=<uuid>
csi-admin local [global flags] decommission inspect --request-id=<uuid> --parent-filesystem-id=<uuid>
csi-admin local [global flags] decommission prepare --request-id=<uuid> --parent-filesystem-id=<uuid>
csi-admin local [global flags] decommission quiesce --request-id=<uuid> --parent-filesystem-id=<uuid>
csi-admin local [global flags] decommission cleanup --request-id=<uuid> --parent-filesystem-id=<uuid>
csi-admin local [global flags] decommission release --request-id=<uuid> --parent-filesystem-id=<uuid>
csi-admin local [global flags] gc submit --request-id=<uuid> --logical-volume-id=<id> --mode=<dry-run|execute> --expected-state=<Archived|Retained>
csi-admin local [global flags] upgrade preflight --request-id=<uuid> --candidate-file=/absolute/candidate.json
csi-admin local [global flags] upgrade preflight --request-id=<uuid> --candidate-stdin=true
csi-admin local [global flags] uninstall inspect --request-id=<uuid>
csi-admin local [global flags] uninstall prepare --request-id=<uuid>
csi-admin local [global flags] uninstall quiesce --request-id=<uuid>
csi-admin local [global flags] uninstall cleanup --request-id=<uuid>
csi-admin local [global flags] uninstall release --request-id=<uuid>
```

Global flags precede the command. Control-channel I/O timeout is bounded from
one second to five minutes. The checkpoint export stream alone accepts a
timeout through one hour; it still performs its control handshake with a
five-minute maximum before connecting to the separate stream socket. The
grammar accepts only bounded single-line UTF-8 arguments and
unique long-form flags from the closed command-specific set; short, duplicate,
unknown, empty, or positional arguments are rejected rather than interpreted by
last-value precedence. Direct local upgrade candidate input uses a clean
absolute non-root path and must remain the same exact non-symlink regular file
across open and bounded read. The private `candidate-stdin=true` variant is used
only by the complete operator orchestrator under `kubectl exec -i`; it accepts
one bounded canonical `UpgradePreflightPayload`, rejects every other stdin
shape, and never accepts an operator path inside the Pod. Candidate input is no
larger than the wire limit, is strictly decoded, and is validated before the
mutation connection. The private checkpoint `ticket-stdin=true` variant
likewise accepts only the exact bounded canonical `CheckpointTicket` returned
by prepare; the stream server regenerates and compares that ticket before
writing the first archive byte. The node-local decommission primitives accept
only one configured parent ID: inspect reports only staging and workload mounts
backed by that parent, and prepare first closes and drains the process-wide Node
mutation gate, then unmounts only its exact parent root after those child sets
are empty. Mounts for other configured parents are validated
but neither reported as blockers nor unmounted. These are private building
blocks for the complete offline operator workflow and do not by themselves
authorize provider detach or Helm-value removal. The controller-local inspect
phase accepts only a configured `draining` parent
and returns the complete validated allocation/ownership/PV blocker set plus the
fresh installation Instance targets without changing readiness, admission, an
allocation ConfigMap, or a reservation journal. It reads the committed journal
set only; any matching `Pending` intent is a blocker and inspect never repairs
it. The controller-local quiesce phase is the only decommission phase allowed
to reconcile a `Pending` journal after the global mutation gate has drained.
The controller-local quiesce phase accepts only that configured `draining`
parent, drains the global mutation gate, resolves every exact `Pending` journal,
then rejects the resulting allocation when it targets that parent, rereads and
validates the full startup inventory under that barrier, and
captures exact Instance targets. Cleanup unmounts and detaches only that parent
and proves fresh regional and per-Instance absence without rejecting other
configured parent attachments. Release requires successful cleanup and writes
the normal exact graceful-release marker. The complete operator must interleave
these phases with its own repeated Kubernetes inventory and Pod-stop proof;
calling the local phases alone remains unsupported. Successful
output is the exact validated handshake or handler-owned JSON object followed
by one newline. Command-line errors exit 2; transport, compatibility, remote,
cancellation, and output failures exit 1.
The local uninstall phases are private orchestration primitives: none emits a
safe-uninstall completion audit or authorizes Helm deletion. The supported
operator entry point is the complete multi-Pod command:

```text
csi-admin uninstall prepare --namespace=<namespace> --release=<helm-release> --request-id=<uuid> --mode=<dry-run|execute> [--kubeconfig=/absolute/path] [--context=<name>] [--timeout=30m]
```

It loads the caller's kubeconfig, performs a compatible handshake through
`kubectl exec` before every local phase, reads Kubernetes inventory with the
caller's authorization, and is the only command that can emit the final audit.
The caller therefore needs read access to cluster-scoped Nodes, CSINodes,
StorageClasses, PVs, and VolumeAttachments; read access to Pods, PVCs, the
release ConfigMap, and the installation identity Secret; Pod exec on the exact
release Pods; create/get/update access to the request progress ConfigMap;
preconditioned delete access to the exact node DaemonSet; and get/update scale
access to the exact controller Deployment. These permissions are never added
to the driver ServiceAccounts.
Calling a local phase directly is unsupported and can only leave the driver
fail-closed or partially prepared; it never constitutes permission to run
`helm uninstall`.

The supported operator-side parent-decommission entry point is:

```text
csi-admin decommission prepare --namespace=<namespace> --release=<helm-release> --request-id=<uuid> --parent-filesystem-id=<id> --mode=<dry-run|execute> [--kubeconfig=/absolute/path] [--context=<name>] [--timeout=30m]
```

Dry-run performs complete caller-authorized Kubernetes inventory, target-bound
node mount inspection, and a controller read-only inspection of allocation,
ownership, PV, and provider-target evidence. Execute repeats the complete view
after the controller owns the quiesce barrier. Only then may it mark the
post-quiesce proof complete in a bounded closed-schema progress ConfigMap,
owned solely by the exact controller Deployment UID and updated by
`resourceVersion` compare-and-swap. The progress binds the request, target
parent, runtime ConfigMap, Deployment, DaemonSet, controller Pod, every node
Pod/UID, node/Instance projection, mount roots, and version identities.
Target-parent unmount evidence is persisted before the node DaemonSet is
deleted; controller target-only cleanup and graceful-release evidence are
persisted before the controller is scaled to zero. A retry with the same
request validates surviving identities or exact absence evidence and resumes
without reissuing a completed destructive phase. A different request or target
cannot adopt the progress. The final audit, not a local phase or an incomplete
progress object, is the only evidence that permits the separate Helm-values
removal.

A dry-run that encounters durable progress from an interrupted execute request
may validate and report that progress, but it never updates
`postQuiesceValidated` or any other recovery field. Only the execute path that
currently owns the quiesce authority may advance durable progress.

The supported operator-side online-upgrade entry point is:

```text
csi-admin upgrade preflight --namespace=<namespace> --release=<helm-release> --request-id=<uuid> --candidate-file=/absolute/candidate.json [--kubeconfig=/absolute/path] [--context=<name>] [--timeout=10m]
```

It validates and freezes the caller's exact non-symlink candidate inode before
cluster access, sends only the canonical bounded payload through `kubectl exec
-i`, and relies on the in-Pod client to perform the mandatory handshake before
the controller-local live comparison. No candidate file is copied into the
read-only driver image.

The supported operator-side manual-GC entry point is:

```text
csi-admin gc submit --namespace=<namespace> --release=<helm-release> --request-id=<uuid> --logical-volume-id=<id> --mode=<dry-run|execute> --expected-state=<Archived|Retained> [--kubeconfig=/absolute/path] [--context=<name>] [--timeout=30m]
```

It discovers exactly one running controller Pod for the release and invokes the
existing local command through `kubectl exec`. The local client handshakes
before submitting the durable request. The operator client strictly validates
that the returned typed audit has the same request ID, logical volume ID, mode,
and expected predecessor state; retries retain the same request ID and rely on
the controller's persisted state machine rather than client-side filesystem
assumptions.

The CLI uses a separate connection for the handshake before every mutation.
The mutation envelope repeats the CLI build version and protocol version, and
the server independently negotiates that envelope again immediately before
dispatch, so writing a mutation frame directly cannot bypass compatibility.
Success results are bounded JSON objects. The transport validates but does not
decode and re-encode handler-owned result objects: doing so could reorder a
nested canonical durable artifact such as `checkpoint.json`. The typed command
owner supplies those canonical bytes and the transport preserves them exactly.
Failures use only the stable codes
`invalid_argument`, `failed_precondition`, `unavailable`, and `internal`, with
a single-line message bounded to 1,024 UTF-8 bytes; unclassified internal
errors are redacted. The default server admits at most four concurrent local
connections, permits a configured bound only from one through sixteen, applies
bounded I/O deadlines, closes a connection before any I/O when its deadline
cannot be installed, closes active connections on shutdown, and joins every
connection goroutine before exit. Workflow leadership, quiesce, locking, and
authorization checks remain mandatory after transport validation; a successful
wire handshake is never mutation authority.

The server freezes one typed owner per configured command route at startup;
duplicate route owners fail startup and a missing route returns
`failed_precondition` without fallback. `checkpoint.prepare` calls the quiesce
coordinator and returns the bounded ticket defined in section 10.2;
`checkpoint.resume` returns success only after its full reconciliation and gate
resume complete. `gc.submit` accepts exactly `logicalVolumeID`, `mode`, and
`expectedState`, persists the bounded request envelope through the active
leader, and then invokes the state-driven GC reconciler. Its result must bind
the request, logical volume, parent, previous/final state, target/quarantine
paths, and completion status; a dry-run cannot carry mutation completion and an
execute result must be a completed `Deleted` transition. `upgrade.preflight`
validates the candidate declaration completely before any live-state read,
then compares it with the authoritative live projection and returns both live
and candidate node generations for audit. The safe-uninstall command is
available as an operator-side command only when its complete Kubernetes and
`kubectl exec` backend is present. Runtime endpoints register only the phases
appropriate to their component: target-mount inspect/prepare on nodes and
durable-record/provider-target inspect plus quiesce/cleanup/release on the
controller. The same `decommission.inspect` wire command is deliberately
component-specific; the complete operator validates both typed results and
never substitutes one for the other.

Release CI and the release-candidate E2E suite must download and verify the
packaged binary, then use that exact artifact for checkpoint export/restore,
parent decommission, GC, upgrade preflight, and safe uninstall. `go run`, an
unversioned locally built binary, or direct record editing is not an accepted
production procedure.

## 8. Repository Structure

Recommended structure:

```text
scaleway-sfs-subdir-csi/
  cmd/
    scaleway-sfs-subdir-csi/
      main.go
    csi-admin/
      main.go
  pkg/
    driver/
      identity.go
      controller.go
      node.go
      server.go
    scaleway/
      client.go
      filesystem.go
      instance.go
      fake.go
    pool/
      config.go
      selection.go
      accounting.go
      refresh.go
      parent_lifecycle.go
    volume/
      handle.go
      path.go
      allocation_record.go
      ownership_record.go
      delete_policy.go
    safety/
      filesystem.go
      statfs.go
    mount/
      mounter.go
      linux.go
      fake.go
    k8s/
      pv_index.go
      node_metadata.go
    coordination/
      lease.go
      shutdown.go
      quiesce.go
    recovery/
      checkpoint.go
      takeover.go
      upgrade.go
  charts/
    scaleway-sfs-subdir-csi/
      Chart.yaml
      values.yaml
      templates/
  deploy/
    examples/
      pvc.yaml
      pod-read-write.yaml
      pod-read-only.yaml
      storageclass.yaml
  docs/
    architecture.md
    limitations.md
    operations.md
    troubleshooting.md
  hack/
    e2e/
      create-kapsule-cluster.sh
      create-sfs-pool.sh
      install-driver.sh
      run-e2e.sh
      cleanup.sh
  test/
    e2e/
    sanity/
  .github/
    workflows/
      ci.yaml
  Dockerfile
  Makefile
  README.md
  LICENSE
  CONTRIBUTING.md
  SECURITY.md
```

Keep files focused. Do not build a large single `driver.go` file.

## 9. Implementation Requirements

### 9.1 Language

Use Go.

Rationale:

- Kubernetes CSI ecosystem is Go-first;
- Scaleway official drivers are Go;
- CSI sidecars and examples are Go-oriented;
- fake clients, mounters, and CSI sanity tests are straightforward in Go.

### 9.2 CSI Services

Implement:

- Identity service;
- Controller service;
- Node service.

Both the controller and node CSI sockets must implement all three Identity
RPCs: `GetPluginInfo`, `GetPluginCapabilities`, and `Probe`. Their identity
responses are identical even though Controller and Node services are deployed
in separate pods:

- `GetPluginInfo.name` equals the configured immutable CSI driver name and
  satisfies the CSI domain-name and 63-byte constraints;
- `GetPluginInfo.vendor_version` is the non-empty immutable semantic release
  version of the running driver binary;
- `GetPluginCapabilities` advertises exactly `CONTROLLER_SERVICE` in v1;
- it does not advertise `VOLUME_ACCESSIBILITY_CONSTRAINTS` or another plugin
  capability that v1 does not implement.

The CSI listener accepts only a clean absolute Unix path within the portable
103-byte bound. Its directory must already exist, be a real directory with no
symlink-resolved alias, become non-writable by group and other, and remain
traversable by the non-root sidecars. Kubernetes may initially present the
exact owned `emptyDir` or plugin hostPath directory as group/other writable;
after proving its resolved identity, unchanged inode, directory type, and
effective-UID ownership, the root driver may atomically narrow only those write
bits through the opened directory descriptor. It never creates the directory,
broadens a permission, or changes an aliased, replaced, or foreign directory.
A pre-existing symlink, regular file, or directory at the socket
path is foreign and fails startup. A live socket also fails startup. The only
entry eligible for unlink is an exact Unix socket for which a bounded local dial
conclusively returns connection refused and whose inode/type remain unchanged
across the liveness check; every other dial result is ambiguous and fails
closed. The listener owns unlink-on-close and verifies mode `0666` after bind.
That mode is necessary because kubelet and the non-root CSI sidecars do not
share the root driver process group; the dedicated pod/hostPath scopes the CSI
endpoint. The private admin socket retains its stricter `0600` contract.

The CSI sanity gate must use the split controller and node endpoints and assert
that Controller tests actually ran. A suite that silently skipped Controller
tests because the node endpoint omitted `CONTROLLER_SERVICE` is a failure.

Required Controller methods:

- `CreateVolume`
- `DeleteVolume`
- `ControllerPublishVolume`
- `ControllerUnpublishVolume`
- `ControllerGetCapabilities`
- `ValidateVolumeCapabilities`

`ControllerGetCapabilities` must advertise exactly the capabilities required by
v1:

- `CREATE_DELETE_VOLUME`;
- `PUBLISH_UNPUBLISH_VOLUME`.

It must not advertise expansion, snapshot, clone, list, or capacity capabilities
in v1.

Required Node methods:

- `NodeStageVolume`
- `NodeUnstageVolume`
- `NodePublishVolume`
- `NodeUnpublishVolume`
- `NodeGetInfo`
- `NodeGetCapabilities`

`NodeGetCapabilities` must advertise:

- `STAGE_UNSTAGE_VOLUME`.

It must not advertise expansion in v1.

`NodeGetVolumeStats` and `GET_VOLUME_STATS` are out of scope for v1. Logical
PVCs are subdirectories without hard quotas; returning parent `statfs` as if it
were per-PVC capacity would be misleading, and recursive per-directory scans are
too expensive and fragile for production. A future version may add this only if
the README and metrics clearly state whether values are parent-scoped or
PVC-scoped.

`ValidateVolumeCapabilities` and `CreateVolume` accept only mounted filesystem
volumes. `CreateVolume` rejects a block capability with `InvalidArgument`;
`ValidateVolumeCapabilities` returns `0 OK` with `confirmed` unset and a clear
message for the same well-formed but unsupported capability.

`ValidateVolumeCapabilities` accepts an omitted `volume_context` and resolves
the read-only check from the parsed handle plus authoritative allocation and
ownership records. A non-empty context must match every immutable field. This
exception exists solely for CSI interoperability and never applies to a
side-effecting publish or node RPC.

### 9.3 Sidecars

The Helm chart must deploy standard CSI sidecars:

- `external-provisioner`
- `external-attacher`
- `node-driver-registrar`
- `livenessprobe`

Use explicit version tags for reporting and immutable image digests for the
production pull identity. Do not use `latest` or tag-only production images.

`external-provisioner` must be configured with:

```text
--extra-create-metadata=true
--worker-threads=5
```

`external-attacher` must be configured with `--worker-threads=5`. Both worker
counts remain configurable within the release-tested limit, while the driver
semaphore is the authoritative concurrency control.

All sidecars that support leader election and write shared Kubernetes state must
have leader election enabled.

Every production sidecar image must be rendered by immutable digest as defined
in section 7.4; an explicit tag remains required for operator-facing version
reporting but is not the pull identity.

### 9.4 CSIDriver Manifest

The Helm chart must create a `CSIDriver` object.

Required v1 manifest:

```yaml
apiVersion: storage.k8s.io/v1
kind: CSIDriver
metadata:
  name: file-storage-subdir.csi.urlab.ai
spec:
  attachRequired: true
  podInfoOnMount: false
  volumeLifecycleModes:
    - Persistent
  fsGroupPolicy: None
```

The driver owns directory UID/GID/mode in v1. If a future version supports
Kubernetes `fsGroup` semantics, it must document precedence between
StorageClass UID/GID/mode and pod `securityContext.fsGroup`, and must test the
performance cost of ownership changes.

### 9.5 Idempotency

Every CSI operation must be idempotent:

- replaying a compatible create for a non-terminal volume name returns the same
  logical mapping;
- deleting an already-deleted volume succeeds;
- attaching an already-attached parent succeeds;
- mounting an already-mounted parent succeeds;
- creating an existing directory succeeds if it belongs to the volume;
- publishing an already-published volume succeeds if it targets the same path.

The driver must persist an allocation record before returning a successful
`CreateVolume` response. This record is required because CSI provisioners may
retry `CreateVolume` before the Kubernetes PersistentVolume has been created.

Required v1 implementation:

- create one ConfigMap per logical volume in the driver namespace;
- derive `logicalVolumeID` deterministically from `driverName` and
  `CreateVolumeRequest.name`;
- name the ConfigMap deterministically from `logicalVolumeID`, for example
  `sfs-subdir-volume-<logicalVolumeID>`;
- treat ConfigMap creation as the atomic idempotency lock for
  `CreateVolumeRequest.name`;
- label and index it only with bounded Kubernetes-safe values. Raw user or
  provider values must be stored in ConfigMap `data` or annotations, not labels;
- store a canonical request hash covering the normalized capacity range, pool,
  capabilities, access modes, and StorageClass parameters. The hash is an
  integrity field, not the sole replay decision;
- store pool name, parent filesystem ID, directory path, original capacity
  range, selected capacity bytes, normalized immutable parameters,
  `mappingHash`, `reservesCapacity`, delete policy, UID/GID/mode, creation
  timestamp, and state;
- use optimistic Kubernetes updates to avoid concurrent allocation races;
- create one permanent fixed-name reservation-journal ConfigMap per configured
  pool before serving; it uses schema version `1`, exact states `Idle` and
  `Pending`, installation and active-cluster identity, a monotonic generation,
  and, only while pending, the complete canonical `Reserved` allocation; its
  name is derived from a bounded SHA-256 pool-name hash, it is never deleted or
  reused, and malformed, missing-after-bootstrap, foreign, unavailable, or
  mismatched journals keep the controller non-serving;
- create one fixed installation-wide reservation-journal-set ConfigMap before
  any fresh-install provider or filesystem mutation. It commits the sorted
  permanent pool set with exact states `Initializing` and `Ready`. Fresh
  bootstrap and an online pool addition first persist `Initializing` with the
  pending pool names, create their generation-zero `Idle` journals, and only
  then CAS the set to `Ready`. A committed journal that is absent is corruption,
  never a bootstrap signal. Removing a committed pool online is unsupported;
- keep terminal allocation records after `DeleteVolume` completes. `Deleted` is
  a non-reserving tombstone; `Archived` and `Retained` reserve capacity until
  manual garbage collection;
- treat the PV volume handle and volume context as immutable Kubernetes-facing
  state after the PV exists;
- rebuild state from both allocation records and PVs on controller startup.

Required allocation states:

```text
Reserved
CreatingDirectory
Ready
Deleting
Archived
Deleted
Retained
```

Retry rules:

- same `CreateVolumeRequest.name` with semantically compatible capacity,
  capabilities, and normalized immutable parameters returns the existing
  mapping only when the allocation record is `Ready`;
- the same compatible request resumes creation only when the allocation record
  is `Reserved` or `CreatingDirectory`;
- an incompatible request fails with `AlreadyExists` and a clear explanation of
  the conflicting immutable field;
- `AlreadyExists` during allocation record creation must never create a second
  record. The driver must read the existing record and run the semantic
  compatibility check;
- `Reserved` and `CreatingDirectory` records must be repaired before returning
  success or remain in that state with an actionable error;
- `Deleting` must fail with a clear "deletion in progress" error;
- `Archived`, `Retained`, and `Deleted` records are terminal tombstones and
  must not be returned as active mappings for a new `CreateVolume` call;
- reusing the same CSI `CreateVolumeRequest.name` after a terminal tombstone
  is unsupported for the lifetime of the installation, even after GC or compact
  tombstone conversion;
- capacity reservations are released only when the allocation record reaches a
  terminal state that no longer reserves capacity;
- `DeleteVolume` retries must be state-driven. `Deleting` resumes the recorded
  operation, `Archived`/`Retained` return success without moving data, and
  `Deleted` returns success without touching the filesystem. An unsafe or
  ambiguous observation leaves the last state unchanged and returns its
  actionable recovery error.

Do not introduce a CRD in v1 unless ConfigMap-backed allocation records prove
insufficient during implementation or testing.

### 9.6 Error Handling

Errors must be returned as proper gRPC status errors:

- invalid configuration -> `InvalidArgument`;
- missing parent filesystem -> `NotFound`;
- no parent has enough logical capacity -> `ResourceExhausted`;
- Scaleway API unavailable -> `Unavailable`;
- permission denied -> `PermissionDenied`;
- existing volume name with incompatible immutable parameters ->
  `AlreadyExists`;
- unsupported capacity range -> `OutOfRange`;
- volume still referenced by a VolumeAttachment -> `FailedPrecondition`;
- unsafe path -> `FailedPrecondition`;
- transient attach or mount readiness failure -> `Unavailable` with context;
- local invariant or unrecoverable mount implementation failure -> `Internal`
  with context.

Every error must include enough context to debug the issue without exposing
secrets.

### 9.7 Logging

The driver emits structured JSON logs on standard output. Every completed CSI
RPC is logged once with its operation, final gRPC code, and duration. Provider,
mount, inventory, recovery, and parent-health failures are logged at warning or
error level with their bounded operational context. Stable parent degradation
and recovery are logged on transition rather than on every polling pass.

Where relevant and available, logs include:

- CSI operation name;
- logical volume ID;
- pool name;
- parent filesystem ID;
- node ID when relevant;
- sanitized path when relevant.

Logs must not include:

- access keys;
- secret keys;
- bearer tokens;
- raw Secret contents.

### 9.8 Metrics

Expose Prometheus metrics on the controller and node pods.

Minimum controller metrics:

```text
sfs_subdir_pool_parent_capacity_bytes
sfs_subdir_pool_parent_allocated_bytes
sfs_subdir_pool_parent_archived_reserved_bytes
sfs_subdir_pool_parent_retained_reserved_bytes
sfs_subdir_pool_parent_available_bytes
sfs_subdir_pool_parent_actual_free_bytes
sfs_subdir_pool_parent_actual_used_bytes
sfs_subdir_pool_parent_observed_size_bytes
sfs_subdir_pool_parent_statfs_block_size_bytes
sfs_subdir_pool_parent_statfs_available_blocks
sfs_subdir_pool_parent_physical_safety_threshold_bytes
sfs_subdir_pool_parent_statfs_sample_timestamp_seconds
sfs_subdir_pool_parent_volumes
sfs_subdir_create_volume_total
sfs_subdir_delete_volume_total
sfs_subdir_scaleway_api_errors_total
sfs_subdir_parent_metadata_refresh_timestamp_seconds
sfs_subdir_controller_ready
sfs_subdir_controller_leader
sfs_subdir_reconciliation_last_success_timestamp_seconds
sfs_subdir_allocation_records
sfs_subdir_node_attachment_slots_used
sfs_subdir_node_attachment_slots_limit
sfs_subdir_attachment_inventory_last_success_timestamp_seconds
sfs_subdir_unknown_attachments
sfs_subdir_published_node_fences
sfs_subdir_parent_condition
sfs_subdir_eligible_nodes_expected
sfs_subdir_eligible_nodes_ready
sfs_subdir_node_config_generation_mismatches
sfs_subdir_csi_operations_total
sfs_subdir_csi_operation_duration_seconds
sfs_subdir_controller_mutations_inflight
sfs_subdir_controller_mutations_queued
```

Minimum node metrics:

```text
sfs_subdir_node_parent_mounts
sfs_subdir_node_stage_volume_total
sfs_subdir_node_publish_volume_total
sfs_subdir_mount_errors_total
```

Metric labels must remain bounded. The exact v1 label contract is:

- parent byte/timestamp metrics use `pool,parent`, where both values come only
  from the validated configured-parent allowlist;
- parent volume counts use `pool,parent,state`, and parent conditions use
  `pool,parent,condition`, with closed state and condition domains;
- allocation counts use `pool,state`; permanent records from an offline
  decommissioned parent are aggregated under the fixed `_historical` pool label
  instead of retaining an arbitrary old pool name;
- unknown attachments and published-node fences are aggregate gauges using
  `pool,state`; their state domains are closed and contain no Instance or node
  identity;
- authenticated Scaleway API errors use only the five closed provider
  operation names; CSI operation and duration metrics use only an implemented
  RPC name and canonical gRPC code;
- node parent-mount counts use only configured `pool`; all remaining listed
  controller and node metrics have no driver-supplied label.

Metrics must not use volume IDs, PVC names, paths, node IDs, Instance IDs,
request names, raw provider responses, or other unbounded values as labels.
Exact Instance, node, and volume identities belong in structured events and
logs. Configured parent IDs are the sole resource-identity exception because
the validated installation-wide allowlist bounds their cardinality before the
metrics registry is created.

Every implemented unary CSI RPC is observed after its handler has fixed the
response and canonical gRPC status. The generic counter and duration histogram
record every completion, and the dedicated CreateVolume, DeleteVolume,
NodeStageVolume, and NodePublishVolume counters likewise include both
successful and failed completions. An internal metrics-registry failure is
reported to the serving supervisor out of band: it must terminate the process,
but it must never replace the already-fixed RPC result and thereby make a
possibly completed storage mutation ambiguous to the caller.

The authenticated Scaleway adapter increments the closed provider-error
counter only when the corresponding API call fails. The node mount adapter
likewise increments `sfs_subdir_mount_errors_total` for failed mount-table,
parent-mount, bind, or exact-unmount operations. In both adapters an internal
metrics failure is delivered to the serving supervisor out of band and never
replaces the exact provider or kernel result, including an ambiguous attach,
detach, bind, or unmount result.

The node initializes and periodically refreshes
`sfs_subdir_node_parent_mounts` from one coherent mount-table snapshot. Only an
exact configured `virtiofs` parent identity contributes to its configured-pool
count; absence contributes zero, while a stacked or foreign target is an
internal node-service failure rather than a plausible mount. This observation
uses no controller or public-provider dependency.

The repository must provide sample Prometheus alert expressions for these
production conditions:

- controller not ready or no current Lease holder;
- reconciliation or attachment inventory older than its documented maximum;
- unknown or foreign attachments, or published-node fences present;
- eligible-node count below the expected count, a missing Ready node plugin, or
  a node configuration-generation mismatch;
- sustained node mount errors;
- parent free space below warning or critical thresholds;
- parent provider status not `available` or
  `critical-size-regression` present.

The chart must not install a `PrometheusRule` or assume the Prometheus Operator
by default. The examples are opt-in integration material with documented alert
meaning, threshold rationale, and first operator action.

Probe semantics:

- the controller CSI `Probe` returns success with `ready=false` until
  configuration, installation and cluster identity, parent ownership, provider
  metadata, eligible-node registration, attachment budget, controller parent
  mounts, leadership, and initial reconciliation succeed;
- the node-plugin CSI `Probe` returns success with `ready=false` only until its
  local configuration, Scaleway metadata identity, CSI socket, mount helper, and
  driver parent-mount root are initialized and valid. It must not depend on
  controller leadership, parent ownership, the Scaleway public API, controller
  mounts, or controller reconciliation;
- each component's CSI `Probe` reads only its own cached readiness state and
  must not perform a Scaleway API call, filesystem traversal, mount, or
  reconciliation itself;
- the standard CSI livenessprobe sidecar in each pod exposes `/healthz` for that
  pod's CSI endpoint. The driver container's Kubernetes startup and readiness
  probes use that endpoint over the shared pod network;
- controller readiness becomes false after leadership loss or when a required
  global invariant prevents the controller from safely classifying requests.
  After successful startup, a transient or error status limited to one parent
  is exposed as a parent-degraded condition and RPC-specific error rather than
  making unrelated parents globally unavailable. Node readiness becomes false
  only when its local node-service prerequisites are no longer valid;
- the driver process exposes a separate shallow `/livez` endpoint for the
  driver container's Kubernetes liveness probe. It reports only process and
  internal event-loop health and is not exposed by a Service;
- Scaleway/API failure during startup, leadership loss, or an unreadable global
  inventory required for every request must remove readiness without failing
  `/livez` or causing a restart loop. A runtime failure scoped conclusively to
  one parent keeps global readiness and blocks only that parent's dependent
  operations;
- the leadership watchdog must terminate the process after the configured
  renewal deadline so a stale controller cannot continue accepting mutations.

Cached controller readiness is derived from independent startup, periodic
global-maintenance, checkpoint-quiesce, and shutdown conditions through one
aggregated state. Clearing one condition must not hide another active failure.
A global maintenance degradation rejects new CreateVolume and
ControllerPublishVolume work with `Unavailable`, while DeleteVolume and
ControllerUnpublishVolume remain available under their normal ownership and
fencing checks so cleanup can improve safety.

The controller and node pods must wire these endpoints explicitly in the chart;
one endpoint must not be reused for contradictory readiness and liveness
semantics. Tests must prove that an unavailable provider keeps cold startup
unready without restarting it, that a runtime failure scoped to one parent does
not block unrelated parents, that neither condition makes a healthy node plugin
unready, and that a wedged process or expired controller leadership watchdog
does terminate the affected process.

The process serves `/livez` and `/metrics` through two distinct one-shot HTTP
servers and listeners. Each server routes only its exact unescaped path;
cross-endpoint, unknown, and encoded-alias paths return 404 before a component
handler. Component handlers retain the GET/HEAD-only contract. The server uses
a 5-second header timeout, 10-second read timeout, 15-second write timeout,
30-second idle timeout, and 16-KiB header bound. The standard library request
error logger is disabled because it can echo untrusted request bytes;
structured runtime diagnostics remain the bounded logging authority.

After pure runtime configuration validation, the controller constructs a local
non-mutating serving shell before Kubernetes, provider, mount, recovery, or
operator-approval work. That shell serves shallow `/livez`, metrics, the
controller CSI socket with cached `Probe(ready=false)`, and the private admin
handshake. Valid Controller RPCs return `Unavailable`, and admin mutations
return the protocol `unavailable` error, until one complete active runtime has
been installed. Installation atomically publishes the leadership-guarded cores
and admin handler, then flips readiness only after parent bootstrap and startup
reconciliation succeed. This permits an abnormal-takeover or missing-Lease
approval wait to remain live and observable without exposing a provisional
mutation path. A permanent startup error still terminates the process after all
local serving goroutines have drained.

Listener bind accepts only the already-validated numeric TCP address and honors
startup cancellation. Runtime cancellation first cancels every request context,
then closes listeners through `http.Server.Shutdown` and permits a five-second
graceful drain. A drain timeout force-closes active connections, returns a
runtime error, and must not be converted into successful shutdown. The serving
call joins its shutdown supervisor before returning, owns listener close, and
rejects reuse of the same server instance. Provider unavailability,
reconciliation delay, or leadership loss changes cached CSI readiness but does
not cancel the shallow liveness server; only process shutdown or its defined
internal-failure path does so.

## 10. Pool Accounting and Recovery

### 10.1 Source of Truth

The driver's allocation record is the authoritative source of truth for
lifecycle state, immutable mapping, capacity reservation, deletion, recovery,
and accounting.

Kubernetes PersistentVolumes are the Kubernetes-facing binding evidence after a
volume is created. They are immutable projections and secondary recovery inputs,
not the primary accounting source.

The driver-owned ownership record is the filesystem ownership proof. Destructive
operations require agreement between the allocation record and the ownership
record. Its mirrored `publishedNodeIDs` set is intentionally co-authoritative
for destructive-operation fencing so namespace loss cannot erase that safety
evidence.

The CSI volume handle is lookup identity only. It is not a complete mapping.

The controller must read PVs with:

- `spec.csi.driver == <driverName>`;
- `spec.csi.volumeHandle` matching the v1 handle format.

It must reconstruct or validate:

- logical volume ID and mapping hash from the compact handle;
- parent filesystem ID, pool name, base path, directory name, and requested
  capacity from immutable PV CSI attributes when the allocation record needs
  recovery;
- ownership and driver identity from the driver-owned ownership record.

This dual model is intentional:

- allocation records make `CreateVolume` idempotent before PV creation;
- allocation records make `DeleteVolume` recoverable after PV deletion begins;
- PVs remain the durable Kubernetes-native source after binding;
- startup recovery can reconcile records and PVs without a separate database.

Logical reservation accounting must be computed from allocation records only.
If both a PV and an allocation record exist for the same `logicalVolumeID`, the
controller must de-duplicate by `logicalVolumeID` and count the allocation
record once. If immutable fields conflict between PV, allocation record, and
ownership record, the controller must fail closed and refuse new provisioning
until manual recovery resolves the conflict.

Detailed allocation records use `recordKind: detailed` and must include at
least:

```text
schemaVersion
recordKind = detailed
recordRevision
driverName
activeClusterUID
state
installationID
createVolumeRequestName
requestHash
originalRequiredBytes
originalLimitBytes
selectedCapacityBytes
normalizedCreateParameters
logicalVolumeID
volumeHandle
volumeHandleHash
mappingHash
poolName
parentFilesystemID
basePath
basePathHash
directoryName
reservesCapacity
deletePolicy
deleteResult
deleteOperationID
deleteOperation
deleteSourcePath
deleteTargetPath
deletePreparedAt
deleteRemoveStartedAt
deleteCompletedAt
directoryUid
directoryGid
directoryMode
createdAt
updatedAt
deletedAt
archivedPath
retainedPath
quarantinePath
publishedNodeIDs
gcRequestID
gcRequestedMode
gcExpectedState
gcRequestedAt
gcOperationID
gcTargetPath
gcQuarantinePath
gcStartedAt
gcRemoveStartedAt
gcCompletedAt
recoveryOperationID
recoverySource
recoveredAt
```

`publishedNodeIDs` is a deduplicated array of exact CSI node IDs, bounded by the
number of eligible nodes and updated only through the publish/unpublish rules in
section 6.14.

The ConfigMap stores the allocation record as canonical JSON in the single data
key `record.json`; indexed bounded values are duplicated only in the labels
defined in section 11.4. `schemaVersion` is the string `"1"` for the initial
schema, timestamps are RFC 3339 UTC, byte counts and revisions are base-10 JSON
integers, and duplicate JSON keys or invalid field types are rejected. All
untrusted durable JSON readers reject nesting deeper than 100 container levels
before typed decoding; v1 schemas are intentionally far shallower. Every update
uses Kubernetes `resourceVersion` compare-and-swap.

`recoveryOperationID`, `recoverySource`, and `recoveredAt` are an optional
all-or-none audit triplet on the detailed allocation record. Normal CSI create
leaves them absent. `recoverySource` is the closed value `ownership-only` after
both allocation and PV absence were conclusively proven, or
`pv-and-ownership` when the missing allocation is reconstructed from an exact
current PV generation plus authenticated ownership. Both paths set a fresh UUID
operation ID and canonical recovery observation time before their create-only
compare-and-swap. The triplet then remains immutable across detailed allocation
updates and is omitted by the closed compact tombstone projection. It never
exists in the ownership record and never authorizes a filesystem mutation.

Allocation records must set `reservesCapacity` consistently with the state
machine:

- `Reserved`, `CreatingDirectory`, `Ready`, `Deleting`, `Archived`, and
  `Retained` set `reservesCapacity: true`;
- `Deleted` sets `reservesCapacity: false`.

`archive` and `retain` states continue to reserve their original logical bytes
until a documented garbage-collection process removes the archived or retained
data and updates the record.

`Deleted` records are terminal non-reserving tombstones. The deterministic
per-volume ConfigMap must remain for the lifetime of the installation so an old
handle can never resolve to a newer volume. After the operator's chosen detailed
record retention window, the controller may update that same ConfigMap in place
to a compact schema using resource-version compare-and-swap. It must not delete
the ConfigMap or move the proof into a shared index.

The window is configured by
`controller.detailedTombstoneRetention`; the v1 chart default is `720h` (30
days), it must be positive, and it is rendered into the closed runtime document
as `detailedTombstoneRetentionSeconds`. The active controller rechecks the
window under leadership, normal global mutation admission, and the exact
logical-volume lock immediately before compaction. Background selection skips
detailed tombstones whose parent has completed offline decommission because v1
must not remount an unconfigured historical parent merely to authenticate a
compaction peer. Those Kubernetes records may remain detailed permanently.
An eligible configured-parent ownership mismatch or unreadable record degrades
controller maintenance readiness and is retried; it is never treated as a
reason to delete or synthesize evidence.

A compacted tombstone uses the distinct `recordKind: compactDeleted` variant.
It contains this required closed field set:

```text
schemaVersion
recordKind = compactDeleted
recordRevision
driverName
installationID
activeClusterUID
createVolumeRequestName
logicalVolumeID
volumeHandleHash
mappingHash
state = Deleted
parentFilesystemID
directoryName
reservesCapacity = false
deleteResult
updatedAt
deletedAt
```

Only the following terminal audit fields may additionally be present when they
were populated by the detailed record: `deleteOperationID`, `deleteOperation`, `archivedPath`,
`retainedPath`, `quarantinePath`, `deleteCompletedAt`, `gcOperationID`,
`gcTargetPath`, `gcQuarantinePath`, and `gcCompletedAt`. Unknown fields are
rejected. Compaction is one resource-version compare-and-swap from a
schema-valid `recordKind: detailed`, `state: Deleted`,
`reservesCapacity: false` record; it never changes identity or terminal outcome.
Compact tombstones do not require their historical parent to remain configured
and are never used for filesystem mutation, reconstruction, or capacity
reservation.

A `deletedUnknown` record remains in its separate minimal shape below and is
never forced into the detailed or compact requirements.

Conclusive-absence deletion uses a distinct `recordKind: deletedUnknown`
variant under schema version `"1"`. It contains only facts recoverable from the
request and successful absence checks:

```text
schemaVersion
recordKind = deletedUnknown
recordRevision
driverName
installationID
activeClusterUID
logicalVolumeID
volumeHandleHash
mappingHash
state = Deleted
reservesCapacity = false
absenceReason
createdAt
updatedAt
deletedAt
```

Fields such as create request name, capacity, pool, parent, base path,
directory, and delete policy must be absent rather than invented. Readers,
startup reconciliation, compaction, and `DeleteVolume` must explicitly support
this variant. It is never eligible for filesystem mutation, capacity
accounting, active-volume reconstruction, or `CreateVolume` name reuse.

`recordKind` is the only v1 allocation-schema discriminator. Readers must
implement exactly `detailed`, `compactDeleted`, and `deletedUnknown`; they must
reject an unknown kind and must not infer the shape from missing fields. Because
no prior public schema exists, v1 must not emit the ambiguous legacy value
`recordKind: full`.

### 10.2 Rebuild on Startup

On startup, the controller must:

1. load pool config;
2. fetch parent metadata from Scaleway;
3. run the node compatibility and attach-limit preflight;
4. attach and mount every configured parent required for controller lifecycle,
   including `draining` parents with existing allocation or ownership records;
5. collect fresh `statfs` data for mounted parents;
6. list allocation records owned by the driver;
7. list existing PVs owned by the driver;
8. reconcile allocation records, PVs, and driver-owned ownership records;
9. rebuild logical allocation state from allocation records, including archived
   and retained reservations on both active and draining parents;
10. log a clear pool summary.

The mutating crash-window pass runs only after the complete allocation, PV,
parent-claim, and ownership inventory has been paired and every permitted
allocation reconstruction has completed. It reads the allocation set once
in stable order. `Reserved`/`CreatingDirectory` resume creation from their
persisted parent without placement or current defaults; `Ready` restores only
the conservative published-node union; `Deleting` and delete-created terminal
lags resume through the delete state machine; execute/progress GC resumes
through the GC state machine; stable `Archived`/`Retained` pairs restore the
fence union and validate complete terminal evidence. Detailed GC and delete
repair paths never substitute for one another. `compactDeleted` and
`deletedUnknown` records cause no per-record follow-up API or filesystem read in
this pass; their pairing/absence contract was already established by the
inventory phase. Any first conflict stops startup serving and later records are
not opportunistically mutated.

The inventory phase itself is read-only and validates the complete set before
the first recovery write. Every configured parent has exactly one matching
parent-global claim and complete ownership enumeration; duplicate parent,
allocation, PV-name, PV-logical-ID, or ownership identities fail closed. An
allocation/PV pair must match the full immutable context, and a configured
detailed allocation must satisfy the closed lifecycle predecessor table against
its ownership. A leftover PV requires matching detailed ownership and becomes a
`pv-and-ownership` recovery; leftover detailed or compact ownership without a
PV becomes an `ownership-only` recovery. A PV with no ownership evidence, a PV
paired with compact ownership, or a changed mapping is not reconstructable.
Only non-reserving `Deleted` allocation tombstones from a completed offline
decommission may reference an unconfigured parent without online ownership or
PV evidence. Recovery needs are sorted and isolated from the input snapshot;
PV-backed creates run first, ownership-only creates second, and the lifecycle
pass rereads the allocation list last. Failure at any step stops the sequence
and keeps startup non-serving.

Stale allocation records without a matching PV must not be deleted
automatically unless the directory state has been checked and the configured
cleanup policy has been applied.

If a PV exists without an allocation record, the controller must reconstruct the
record from the volume handle, PV CSI attributes, and driver-owned ownership
record, or fail closed with a recovery message.

PV-backed reconstruction requires the exact PV name, UID, resourceVersion,
driver name, complete handle, and closed immutable `volume_context`. The handle
and context mapping must agree with each other and every immutable field in the
checksum-authenticated detailed ownership record before any recovery proof or
write. A fresh read must then prove that exact PV generation remains current and
the deterministic allocation name remains absent. The controller writes one
create-only allocation with `recoverySource = pv-and-ownership`; a committed but
unacknowledged create is accepted only after deterministic reread validates the
same PV/ownership pair and audit triplet. A compact ownership record, changed
PV generation, missing context field, unavailable read, or conflicting existing
allocation fails closed. No StorageClass, Helm, or pool default fills a field.

If both allocation record and PV are missing but a matching ownership record
exists, the controller may reconstruct a detailed allocation record only from the
complete immutable recovery envelope required by section 6.13. It must first
persist the reconstructed record with create-only compare-and-swap, set the
allocation audit triplet `recoveryOperationID`, `recoverySource =
ownership-only`, and `recoveredAt`, and then use the normal lifecycle state
machine. `recordRevision` is initialized from the authenticated ownership
revision and `updatedAt` is the recovery observation time. For `Archived` and
`Retained`, the allocation-only `deleteResult` is derived from the authenticated
terminal ownership state as the closed values `archived` or `retained`; this is
a schema projection, not a current-default lookup. A missing field, checksum
mismatch, or disagreement with the handle fails closed; current Helm or
StorageClass defaults are never substituted.

`Reserved` and `CreatingDirectory` are repairable startup states:

- if the data directory is missing, resume creation;
- if the data directory exists at the expected safe path, is empty, and the
  allocation record proves this create operation reached `CreatingDirectory`,
  write or repair the driver-owned ownership record and continue;
- if the data directory already has a matching ownership record, continue;
- if the data directory contains unexpected data without a matching
  driver-owned ownership record, fail closed with a manual recovery message.

For `Ready`, `Deleting`, `Archived`, and `Retained`, a missing ownership record or
an immutable mapping mismatch is not automatically repaired. A state-only lag
caused by a documented crash point may be repaired when the record has the same
driver, installation, handle, mapping hash, parent, base path, and directory,
and its revision/state is exactly the expected predecessor of the persisted
allocation transition. Any other mismatch fails closed and requires manual
recovery. This narrow rule makes prepared state transitions resumable without
allowing the controller to invent ownership.

For `Deleted`, startup applies the terminal pairings and forward-only repair
table in section 10.6. A detailed `Deleted` allocation with matching compact
ownership is the normal retention state. A compact allocation and compact
ownership must agree on driver, installation, cluster, create request name,
logical volume, handle hash, mapping hash, parent, directory identity, operation
IDs, and terminal result. A missing allocation tombstone may be reconstructed
only from a schema-valid matching compact ownership tombstone as defined in
section 6.13. A detailed `Deleted` allocation with the exact detailed ownership
predecessor may finish only the corresponding ownership compaction without
touching the filesystem. A missing compact ownership tombstone, missing field,
or any other disagreement is not repaired from current configuration and keeps
the controller non-serving when the parent is currently configured. For a
historical parent removed by the completed offline decommission procedure, the
controller validates the non-reserving Kubernetes tombstone only and does not
remount the parent to require or reconstruct the compact ownership peer. The
separate `deletedUnknown` variant has no ownership record and follows its
conclusive-absence contract.

Same-cluster namespace recovery does not require a separate database. V1
recovery is supported only from a controller-generated quiesced metadata
checkpoint; an arbitrary online export is not a consistent backup.

The bundled admin CLI must expose controller-local `checkpoint prepare` and
`checkpoint resume`, plus the operator-side offline `checkpoint restore`
workflow defined below. Operators invoke prepare/resume with `kubectl exec`
against the current controller Pod; no network Service or second controller is
introduced. `checkpoint prepare` carries a unique request ID and performs this
closed protocol while the controller still owns its Lease and parent mounts:

1. atomically enter checkpoint mode, become unready, reject every new mutating
   CSI/admin request, pause compaction and background repair, and wait for the
   process-wide mutation count to reach zero;
2. reject preparation when any allocation is `Reserved`, `CreatingDirectory`,
   or `Deleting`, when a GC/delete transition or bootstrap attempt is active,
   when the journal set is not the exact configured `Ready` set, when any
   permanent pool journal is not exactly `Idle`, or when allocation and
   ownership records do not agree;
3. during the same uninterrupted quiesced interval, read the complete
   allocation, reservation-journal-set, permanent reservation-journal, and
   driver-PV sets; capture a canonical Kubernetes-object
   inventory containing source UID/resourceVersion plus a hash of each
   schema-defined recoverable object projection, and a canonical parent
   inventory for every currently configured parent containing the parent-global
   owner hash and the relative path, content hash, revision, record kind, and
   state of every detailed or compact ownership record. Offline-decommissioned
   parents are represented only by their already-validated Kubernetes
   allocation tombstones and are not remounted for checkpoint creation;
4. emit one schema-versioned manifest containing `schemaVersion: "1"`,
   checkpoint request ID, driver name, backup timestamp, `activeClusterUID`, a
   hash of `installationID`, chart version, every rendered image digest, current
   fixed Lease name `scaleway-sfs-subdir-csi-controller`, Lease UID and holder
   evidence, Kubernetes-object inventory count and
   aggregate SHA-256, and every per-parent aggregate digest and record count;
5. remain quiesced while the backup tool exports the exact objects and verifies
   their source resourceVersions and content hashes against the detailed
   inventory, then writes that inventory beside the objects in the external
   package. Any changed, missing, extra, duplicate, or unreadable object
   invalidates the attempt;
6. only after a complete package is durably written does the operator run
   `checkpoint resume`. The controller then performs a full reconciliation,
   leaves checkpoint mode, and becomes ready. A failed export is discarded and
   resumed in the same way; it is never labelled as a completed checkpoint.

The prepare result is one immutable process-local candidate containing the
bounded manifest, the exact canonical detailed Kubernetes-object inventory,
and one exact canonical detailed inventory per configured parent. The
controller retains an isolated copy only until resume. Export verification must
match all of those captured bytes before verifying the exported object and
ownership contents. In particular, the restore-stable Kubernetes aggregate
deliberately excludes source UID/resourceVersion, so aggregate equality alone
cannot prove that the backup tool exported the same quiesced API generations;
the detailed inventory must be byte-for-byte equal to the captured candidate.
The captured holder evidence must also equal both the complete holder-evidence
annotation set and `holderIdentity` of that same quiesced Lease read. A stale,
mixed-generation, absent, or malformed holder field invalidates the candidate;
the controller must not combine holder evidence supplied independently of the
Lease snapshot.

The `checkpoint.prepare` control response is a canonical, schema-versioned
ticket bounded to 2 MiB. It contains the complete canonical manifest and its
SHA-256, plus the exact SHA-256 and byte size of the detailed Kubernetes-object
inventory and each sorted parent inventory. It does not copy detailed
inventories through the controller-local socket: at the supported scale those
files may each be up to 32 MiB. The checksum-verified release `csi-admin`
export path builds the detailed inventories and package, compares every ticket
commitment, then runs the complete object/ownership package verifier over those
same bytes. Size is checked before hashing or decoding. A source
UID/resourceVersion-only change is detected by the detailed Kubernetes
inventory commitment even though the restore-stable aggregate is intentionally
unchanged. A missing, extra, non-canonical, size-changed, or digest-changed
inventory invalidates the export. The ticket is control evidence only and is
not the immutable restore Secret; `checkpoint.json` remains the embedded
manifest bytes.

The released operator grammar is:

```text
csi-admin checkpoint prepare --namespace=<namespace> --release=<helm-release> --request-id=<uuid> --output-file=/absolute/checkpoint.tar [--kubeconfig=/absolute/path] [--context=<name>] [--timeout=30m]
csi-admin checkpoint resume --namespace=<namespace> --release=<helm-release> --request-id=<uuid> [--kubeconfig=/absolute/path] [--context=<name>] [--timeout=30m]
csi-admin checkpoint restore --namespace=<namespace> --release=<helm-release> --request-id=<uuid> --archive-file=/absolute/checkpoint.tar --identity-secret=<name> --identity-key=<key> --mode=<dry-run|execute> [--kubeconfig=/absolute/path] [--context=<name>] [--timeout=30m]
```

Prepare uses the caller's kubeconfig to select the one exact release controller
Pod, invokes the private prepare command, and passes only the canonical ticket
to the private export command over `kubectl exec -i`. The controller performs a
fresh complete startup-inventory read while the barrier remains closed,
reconstructs every recoverable Kubernetes projection, parent owner, detailed
inventory, and ownership record, requires byte-for-byte equality with the
process-local candidate, and runs the complete package verifier before the
stream success prelude. The stream is a deterministic POSIX tar containing
only regular mode-`0600` files: `format.json`, `checkpoint.json`, the detailed
Kubernetes inventory, sorted object metadata/projections, and sorted parent
owner/inventory/record files. The v1 stream refuses an archive above 1 GiB,
well above the supported scale envelope but finite for a compromised or
incompatible peer.

The success prelude commits the exact archive byte count. After those bytes the
server emits a canonical completion trailer carrying that same count and the
archive SHA-256. The in-Pod client returns success only after receiving and
matching the entire archive and trailer; EOF alone is never completion. The
operator writes to a unique same-directory mode-`0600` temporary inode, calls
`fsync`, publishes it with no-replace hard-link semantics, synchronizes the
directory, removes the temporary name, and synchronizes the directory again.
An existing output path is rejected before cluster access and again by the
atomic publish primitive; a failed or truncated export never replaces it.
Resume remains a separate explicit command and is run only after the operator
has retained the successful manifest/archive digests and completed package.

Restore accepts only the exact deterministic archive emitted above. Before any
Kubernetes read or mutation it opens one clean absolute non-symlink regular
file, bounds it to 1 GiB, rejects every non-regular, linked, duplicate,
non-`0600`, unsafe, unknown, oversized, missing, or non-canonical tar entry,
runs the complete checkpoint package verifier, regenerates the deterministic
archive, and requires the regenerated complete size and SHA-256 to equal the
input. Reordered headers, altered padding, appended data, and a merely
well-formed but non-canonical tar are rejected. The explicit request ID must
equal the embedded completed checkpoint request.

The restore command runs before any release node Pod exists and only while an
existing matching controller Deployment is absent or stable at zero replicas;
the node DaemonSet must be absent and the fixed controller Lease absent or
empty. It verifies the current `kube-system` UID, the caller-selected restored
identity Secret key and its checkpoint hash, the exact surviving driver-PV set,
and every immutable PV mapping. It never creates, updates, or deletes a PV.
Every archived allocation projection must be in the same target namespace,
decode to one exact v1 allocation variant, match the manifest identity, and use
its deterministic ConfigMap name. A dry-run performs all archive, cluster,
identity, PV, existing-allocation, and existing-checkpoint-Secret checks without
mutation.

Execute first creates only missing exact all-`Idle` reservation journals and
then the exact `Ready` journal-set commitment. It uses create-only ConfigMaps;
an existing, ambiguous, extra, or conflicting journal fails closed unless a
conclusive reread proves the exact checkpoint record. It then creates only
missing exact allocation ConfigMaps through the normal create-only
AllocationStore schema. An `AlreadyExists`, timeout, or ambiguous create is
success only after a conclusive reread returns the exact checkpoint record; a
conflicting or extra allocation fails closed. After all journals and allocations
exist, restore rereads the complete journal/allocation/PV set and requires its
restore-stable aggregate to equal the manifest. Only then does it create the
fixed externally owned immutable Opaque `sfs-subdir-checkpoint` Secret with the
sole `checkpoint.json` data key. An existing Secret is accepted only when its
complete envelope and manifest bytes are exact; an existing Secret with a
missing allocation is inconsistent and is not used to authorize repair. This
allocation-first, Secret-last order makes an interrupted restore safely
idempotent while ensuring the recovery marker never advertises an incomplete
Kubernetes object set. Parent inventory equality and offline provider fencing
remain controller startup responsibilities and are not replaced by archive
verification.

A persisted GC `dry-run` request is bounded audit evidence, not an active
filesystem transition. It is checkpoint-eligible only when the allocation has
no `gcOperationID` or GC progress fields and the matching detailed ownership
record has no GC request or progress fields, as required by section 10.6. A
persisted `execute` request or any GC progress on either side is active and
rejects checkpoint preparation. This distinction prevents an earlier completed
dry-run from permanently making later checkpoints impossible without weakening
the quiesce boundary around mutating GC.

Checkpoint preparation and resume are idempotent by request ID. `SIGTERM`,
Lease loss, or a process crash while quiesced invalidates the candidate; no
graceful-release marker may be interpreted as checkpoint completion. Tests must
attempt create, delete, publish, unpublish, GC, compaction, bootstrap, and
background repair between every phase and prove that none crosses the quiesce
barrier.

Resume reconciliation reuses the production startup reconstruction and
lifecycle state machines while the global gate remains closed. MutationGate
grants only the active checkpoint request one unforgeable, callback-scoped
internal admission capability; ordinary CSI, GC, compaction, bootstrap, and
background callers continue to receive the quiesced error. The capability is
bound to the exact gate instance and request ID, is cleared before the callback
returns, and the gate again drains any admitted holder before it can reopen.
Retaining its derived context cannot authorize a later operation. A failed or
cancelled reconciliation clears the internal capability but leaves the global
checkpoint barrier and readiness closed for an explicit retry or operator
decision. Resume first resolves every exact `Pending` reservation journal,
validates the complete committed set as `Idle`, then captures the fresh startup
inventory and runs lifecycle reconciliation; this lets a failed prepare reopen
safely without discarding an ambiguous reservation.

The minimal backup set is:

- installation identity Secret;
- the fixed reservation-journal-set ConfigMap and every permanent per-pool
  reservation-journal ConfigMap, all proven `Ready`/`Idle` under quiesce;
- allocation and permanent tombstone ConfigMaps;
- Helm values and pinned chart version;
- PV manifests for this driver;
- the detailed Kubernetes-object inventory used to verify the exported package;
- the checkpoint manifest, packaged for restore as the data of a fixed
  externally owned immutable Secret named `sfs-subdir-checkpoint`.

Helm never creates or owns `sfs-subdir-checkpoint`. During restore, the
operator recreates it in the driver namespace from the completed package before
starting the controller. It is an `Opaque`, `immutable: true` Secret with the
single data key `checkpoint.json`; unknown or missing data keys are rejected.
The controller may only get this exact Secret by name; it verifies the canonical
manifest SHA-256, schema, request ID, identity/object hashes, and parent
inventories before recovery discovery. A manifest with any other Lease name is
incompatible and rejected before provisional acquisition. The controller cannot create, update,
patch, or delete it. A normal startup with a valid live Lease does not consume a
stale checkpoint Secret. The operations guide requires deleting the Secret
after a successful recovery and retaining the external backup package.

The in-cluster checkpoint manifest is intentionally O(number of parents plus
images), not O(number of volumes). Per-object entries stay in the external
package; the Secret contains only their count and aggregate digest and must stay
within the Kubernetes Secret size limit at the supported scale envelope. The
controller applies a 1 MiB maximum to the exact canonical `checkpoint.json`
bytes before the manifest is emitted or decoded; oversized input is rejected
before JSON allocation. On
restore, Kubernetes assigns new UIDs and resourceVersions, so the controller
recomputes the aggregate from canonical recoverable content and logical object
identity, excluding server-assigned metadata and status. Source
UID/resourceVersion are export-consistency evidence only and are never expected
to survive recreation. Missing, extra, duplicated, or content-changed objects
change the aggregate and keep recovery non-serving.

For v1 the restore-stable Kubernetes-object set is closed to the fixed
reservation-journal-set ConfigMap, every permanent per-pool reservation-journal
ConfigMap, allocation and permanent-tombstone ConfigMaps owned by this
installation, plus every current PV whose CSI driver equals this driver name.
Journal projections are their complete canonical closed-schema records and must
describe the exact configured `Ready` set with every member `Idle`. An
allocation ConfigMap projection is
the complete canonical `record.json`; its deterministic object name and
namespace form the logical inventory identity, and AllocationStore has already
validated every required deterministic label. A PV projection contains its
name as logical identity and the exact driver name, volume handle, and complete
closed immutable `volume_context` as canonical content. This is the complete
driver-authoritative PV mapping; UID, resourceVersion, other server-assigned
metadata, and status are deliberately excluded. The installation identity is
committed separately by `installationIDHash`, while chart version and the
closed rendered-image set have their own manifest fields. All three commitments
must match together; none may substitute for another.

A fresh installation with no allocation record and no driver PV still has the
fixed journal-set object plus one permanent `Idle` journal per configured pool.
Those objects form an explicit non-empty detailed Kubernetes inventory; empty
allocation and PV subsets are not treated as omitted or unavailable reads. The
complete journal enumeration, installation hash, chart/image commitments, and
non-empty configured-parent inventories are all still required. This preserves
checkpoint support before the first PVC without granting the controller API
read access to the separately kubelet-projected identity Secret.

External detailed inventory JSON is canonical, closed-schema, sorted, and
bounded to 32 MiB per inventory file. Each schema-defined recoverable Kubernetes
object projection is canonical JSON bounded to 2 MiB, above the size supported
for the namespaced ConfigMaps and Secrets in this backup set. Empty,
non-canonical, oversized, duplicate-key, duplicate-identity, or unknown-field
input invalidates the checkpoint package rather than being normalized during
verification.

Each configured-parent inventory digest is SHA-256 over canonical JSON whose entries are
sorted by normalized relative ownership-record path. Each entry contains only
that path, the SHA-256 of the complete record bytes, revision, record kind, and
state. Parent inventories are sorted by parent filesystem ID in the checkpoint
manifest. Duplicate paths, duplicate parent IDs, non-normalized paths, checksum
errors, and unknown schemas invalidate the checkpoint rather than being
silently skipped.

The recovery point objective is the most recent completed checkpoint. Runtime
PVC or GC changes after that checkpoint are not silently discarded: ownership
records on currently configured parents that are newer than, missing from, or
inconsistent with the restored checkpoint, including an extra or missing
permanent compact tombstone, change the per-parent inventory digest and keep the
controller non-serving. Historical unconfigured parents are instead protected
by the Kubernetes-object aggregate containing their non-reserving allocation
tombstones. The
targeted manual recovery procedure is then required. Automatic v1 recovery must
not invent a forward transition or overwrite newer parent metadata. The backup
tooling must reject a checkpoint while a transition is active, while the
controller is not in its exact quiesced request, when the exported object hashes
do not match its manifest, or when an inventory entry is missing or duplicated.

After namespace loss, restore the namespace-scoped identity and allocation
records plus the fixed immutable checkpoint Secret before starting the
controller. Namespace deletion does not remove cluster-scoped PVs: the normal
flow preserves them and verifies them against the backed-up PV manifests rather
than creating duplicates. A missing or changed driver PV is a targeted recovery
condition, not something this automatic namespace-recovery path recreates. The
controller starts non-serving, mounts configured parents only for recovery
discovery, reconciles restored records against live PVs and parent-global and
per-volume ownership records, and becomes ready only when immutable mappings
agree and the offline fencing gate below is complete. A missing in-progress or
terminal record must not be guessed from directory names; targeted recovery
uses the driver-owned ownership record and fails closed on ambiguity.

Namespace recovery is supported only in the same Kubernetes cluster in v1. The
runtime `activeClusterUID` must match every parent-global owner record. A
different cluster UID is a hard stop even when all restored Secrets, ConfigMaps,
PVs, namespace names, and Helm values match. Cross-cluster recovery requires a
future offline parent-claim transfer protocol and must not be documented as a
supported v1 procedure.

The historical Lease evidence in a checkpoint is audit data, not proof of the
last controller after normal operation resumes. A controller created after the
checkpoint may have run on another Instance. Therefore missing-Lease recovery
must never fence only the checkpoint holder. An absent Lease and a newly created
empty Lease are equivalent for recovery decisions; neither bypasses the offline
gate.

When the Lease is absent or empty, the controller must acquire it provisionally
and attach/mount configured parents only for provider inventory and read-only
inspection of parent-global owner records. Read-only here is an operation-level
restriction; the mount uses the single release-tested flagless `virtiofs`
profile. The controller must not create or update metadata, repair state, serve
mutating CSI calls, or perform directory operations during discovery. A
different Pod UID must never overwrite or provisionally acquire a Lease with a
non-empty previous holder, even when it is expired, before the exact abnormal
takeover approval has been validated and consumed.

The provisional acquisition persists a closed all-or-none discovery marker on
the same Lease containing schema version `1`, state `Provisional`, the canonical
UTC condition-observation timestamp, installation ID, active cluster UID, and
holder Pod UID. Renewal and same-Pod reacquisition preserve this marker and
remain read-only; restarting or draining the local renewal loop cannot promote
it. A conclusive empty-installation discovery is promoted only by a separate
resourceVersion CAS that immediately follows a fresh complete absence proof and
clears the marker. Missing-Lease approval must be newer than the marker's exact
timestamp and consumes the marker together with approval audit evidence in its
single acquisition CAS. Graceful release and parent bootstrap are forbidden
while the marker remains active.

Fresh-installation discovery requires a complete empty allocation and driver-PV
inventory before the first attach and repeats both inventories after every
parent has been inspected. Before attaching each parent, its complete regional
inventory and the controller Instance inventory must both prove that the parent
is detached. The controller records that observation only in process memory,
then attaches and mounts the parent, revalidates that the exact current
controller Instance is its sole available attachment, proves the immutable
parent claim absent, and requires the literal filesystem root to be empty. A
partial retry in the same process may recognize only an exact attachment whose
empty observation it recorded before issuing the attach. The all-parent proof
hands those process-local observations to bootstrap only after successful Lease
promotion; bootstrap must persist the per-parent attempt journal before its
first filesystem write. A restart loses this evidence, so a pre-attached parent
without a matching durable journal can never be adopted as fresh and requires
the operator-approved recovery path.

- if no allocation record, ownership record, driver PV, or parent-global owner
  record exists, the separate discovery-promotion CAS establishes a fresh
  installation and the holder may then initialize the parent claims without
  recovery approval;
- if any durable driver state exists, recovery is offline in v1. Before
  approval, the operator must recreate or scale down the Kapsule worker pools so
  every pre-recovery workload/controller Instance is conclusively stopped or
  deleted. Fresh regional inventories for every configured parent must then
  contain no attachment to an old or unknown Instance; only the new provisional
  recovery controller Instance may appear;
- the immutable recovery approval must bind the checkpoint request ID and
  complete manifest SHA-256, and must attest the all-pre-recovery-Instances
  fencing scope. Waiting for Lease expiry or fencing only the checkpoint holder
  is never sufficient;
- if any parent claim has a different `activeClusterUID`, recovery approval
  cannot authorize it and the controller fails closed.

The Scaleway API can exhaustively enumerate every attachment of each configured
parent, but it cannot infer historical Kubernetes-cluster membership for an
already detached Instance. Consequently, completeness of the
all-pre-recovery-Instances offline set is an explicit operator attestation in
the immutable approval, after the documented worker-pool replacement or
scale-down. The controller independently verifies the provider-observable half
with fresh reads: the provisional controller Instance has the exact runtime
project, region, zone, and running state; every configured parent is available;
each complete regional inventory contains exactly one attachment, to that
provisional Instance in its exact zone; and the Instance inventory contains the
same configured parents in `available` state and no other File Storage. Any old,
unknown, duplicate, cross-zone, transitional, missing, or contradictory entry
keeps recovery non-serving. This check covers every configured parent and never
uses the historical checkpoint holder as the recovery fence scope.

### 10.3 Concurrent Provisioning

The v1 deployment must run one controller replica. This is the simplest
production-safe model for a driver that owns parent mounts, pool accounting,
directory creation, archive, retain, delete, and startup repair.

The controller leadership Lease name is the non-configurable v1 constant
`scaleway-sfs-subdir-csi-controller`. Helm values must not expose an override.
The fixed name is included in parent claims, checkpoint manifests, upgrade
compatibility checks, RBAC `resourceNames` where supported, and preflight tests.
Changing it requires an explicit future offline migration; it is not a normal
chart customization.

The Helm chart must render the controller Deployment with `strategy: Recreate`
in v1. The controller must acquire a Kubernetes Lease before executing any
driver-owned mutation:

- startup reconciliation and repair;
- parent attach/mount for controller lifecycle operations;
- `CreateVolume`;
- `DeleteVolume`;
- archive, retain, delete, and GC;
- node compatibility and attach-budget state updates.

The only pre-approval exception is the bounded provider inventory and read-only
parent-owner discovery described in section 10.2. It still requires the
provisional Lease and must have no filesystem write or repair path.

The Lease is leadership coordination, not storage fencing. On every acquisition
or same-holder reacquisition, one resource-version compare-and-swap must set the
holder identity and these fixed holder-evidence annotations from validated
runtime identity:

```text
coordinationSchemaVersion = 1
holderPodUID
holderNodeName
holderCSINodeID
holderInstanceID
holderZone
holderInstallationID
holderActiveClusterUID
```

Renewals preserve this evidence. A mismatch between `holderIdentity` and
`holderPodUID`, or between the recorded installation/cluster and runtime, makes
the controller non-serving. A committed-but-unacknowledged acquisition,
approval-consumption, or renewal CAS is accepted only after a fresh Lease read
proves an advanced resourceVersion with the exact expected holder and complete
unchanged annotations. The prior unchanged generation remains a retryable
failure; any advanced divergent generation is conclusive Lease loss.
Renewal and bootstrap-journal updates within one leadership session share one
context-cancellable update gate. A bootstrap write or clear therefore cannot
race a renewal and overwrite its resourceVersion generation. The complete
expected annotation map is compared after every update; unrelated annotations
are preserved. A successful bootstrap-journal CAS renews the Lease and advances
the same local renewal watchdog, while a conclusive conflict cancels leadership
before the caller can continue to a provider or filesystem mutation. Replaying
an already-recorded exact bootstrap attempt still performs this CAS, because
the attach path must freshly prove the current leadership generation.
Acquisition attempts within one process are serialized by a context-cancellable
gate. Every acquired generation has one renewal session. Before same-process
reacquisition or approval-based promotion of a provisional recovery holder, the
prior session must cancel its leadership context, stop and join its renewal
loop, and report a clean drain. This drain occurs before the approval's fresh
provider fence and promotion compare-and-swap, so an old renewal cannot race or
overwrite approval consumption. Fresh-installation promotion instead keeps the
provisional renewal active throughout its potentially slow read-only provider
and filesystem absence proof. A failed proof leaves that provisional session
active; a successful proof immediately stops and joins it before reloading the
latest Lease generation and clearing the discovery marker by compare-and-swap.
A session ended by Lease loss, renewal deadline, or external cancellation is
not locally reusable; the watchdog exit path remains mandatory. A container
restart with the same Pod UID is a new runtime and still follows the normal
same-holder compare-and-swap rule.

Required behavior:

- a controller without the Lease is unready and rejects mutating operations;
- Lease loss cancels operation contexts, prevents new irreversible steps, and
  triggers a process exit through an independent watchdog;
- long delete and GC loops must check cancellation between bounded units of work;
- graceful shutdown follows the bounded cancellation and release protocol below;
- the same Pod UID may reacquire leadership after a container restart;
- a different Pod UID may take over automatically only by consuming a valid,
  unconsumed graceful-release marker;
- a different Pod UID must not take over an expired Lease whose previous holder
  did not clear it solely because time elapsed;
- abnormal takeover requires the documented operator procedure to confirm the
  previous process or Instance is stopped, then approve that exact previous
  holder identity through the immutable operator approval Secret;
- the approval is provider-verified, consumed, and audited in the Lease before
  the successor mutates; a later abnormal failure remains recoverable through
  a distinct newly fenced approval rather than exhausting the Lease forever.

The cancellation rule includes cold-start mutations. Immediately after a
leadership session starts, the controller derives one startup context from the
process lifetime and that exact session generation. Parent bootstrap, claim
installation, startup inventory, allocation reconstruction, lifecycle repair,
and initial maintenance all use that context. Approval-based promotion creates
a new context from the promoted generation before any mutating startup work.
A point-in-time `RequireActiveLeadership` check without propagation of the
session context is insufficient.

The same Lease stores the bounded graceful-handoff evidence in fixed
annotations. No second coordination object is introduced. After all mutations
have drained, the current holder may release gracefully only when no bootstrap
attempt exists and checkpoint mode is inactive. It performs one
resource-version compare-and-swap that verifies `holderIdentity` still contains
its exact Pod UID, clears `holderIdentity`, and writes:

```text
gracefulReleaseSchemaVersion = 1
gracefulReleaseState = Released
gracefulReleaseHolderPodUID = <exact releasing Pod UID>
gracefulReleaseRequestID = <random UUID>
gracefulReleasedAt = <RFC 3339 UTC timestamp>
gracefulReleaseLeaseUID = <current immutable Lease metadata.uid>
gracefulReleaseInstallationID = <current installation ID>
gracefulReleaseActiveClusterUID = <current cluster UID>
```

The shutdown or uninstall request ID must already own the closed process-wide
mutation barrier with zero in-flight mutations. Before the release CAS, the
runtime cancels leadership, stops and joins its renewal loop, and rereads the
exact Lease UID/generation so a renewal cannot race the holder clear. The
release finalization context retains the operator deadline and process-shutdown
lifetime but is deliberately not canceled by the leadership session it is
stopping; the exact UID, holder, generation, and compare-and-swap checks are
the authority for this terminal step. All earlier phases remain bound to active
leadership. A
committed-but-unacknowledged release is accepted only after a fresh read proves
the exact empty holder, preserved holder evidence, Lease-bound marker, and same
request ID. After success the runtime is terminal and cannot reacquire locally.

The process exits only after that update succeeds. If it cannot persist the
release, it exits with the previous holder left uncleared, so the abnormal
takeover rule remains conservative. The implementation must not rely on the
standard client-go empty-holder update alone because that update does not retain
the previous Pod UID.

A successor accepts automatic handoff only when the Lease has no holder, the
complete marker is schema-valid, its recorded Lease UID equals the current
immutable `metadata.uid`, its installation and cluster IDs equal runtime, and
the releasing Pod UID equals the preserved holder evidence, and no bootstrap
annotation exists. It acquires leadership, writes its new holder evidence, and
removes only the graceful marker in one compare-and-swap, preserving unrelated
metadata, so the evidence is consumed exactly once. A missing or recreated
Lease, an empty Lease without the marker, malformed or already consumed
evidence, and an expired non-empty holder never qualify as graceful handoff. A
provisional discovery holder must not erase an expired non-empty identity before
the corresponding approval is consumed. The runtime controller, not Helm, owns
the Lease object; Helm install, upgrade, and rollback therefore must not render,
patch, or delete the live holder, bootstrap attempt annotations, holder evidence,
or graceful-release annotations.

Required production defaults:

```text
leaseDuration: 30s
renewDeadline: 20s
retryPeriod: 5s
```

The chart must validate `retryPeriod < renewDeadline < leaseDuration`. The
independent watchdog must stop mutations and terminate the process no later than
`renewDeadline` after the last successful renewal.

This deliberately trades automatic controller failover for data safety in v1.
The chart must not claim controller HA or automatic failover.

Operator approval is a fixed immutable Secret named
`sfs-subdir-controller-approval`, deliberately using a resource type that the
controller cannot create, update, patch, or delete. Helm must not own or
generate it. The operator creates it only after observing the blocked recovery.
It contains canonical values for:

```text
schemaVersion = 1
mode = abnormal-takeover | missing-lease-recovery
requestID
installationID
activeClusterUID
previousHolderPodUID
previousHolderNodeName
previousHolderCSINodeID
previousHolderInstanceID
previousHolderZone
checkpointRequestID
checkpointManifestSHA256
recoveryFenceScope
reason
approvedAt
expiresAt
```

The previous-holder fields are required only for `abnormal-takeover`.
`missing-lease-recovery` instead requires the exact completed checkpoint request
ID and manifest SHA-256 plus
`recoveryFenceScope = all-pre-recovery-instances`; historical holder fields may
be retained for audit but are never treated as the latest-holder fence.

The Secret must set `immutable: true`, have a maximum validity of one hour, and
be newer than the controller's observed takeover/recovery condition. The
controller gets only this exact Secret by name. It validates the Secret's
immutable Kubernetes UID, schema, expiry, installation, cluster, mode, and exact
previous-holder evidence against the Lease. It then performs a fresh provider
read of the recorded Instance. Abnormal takeover is allowed only when that
Instance is conclusively absent, `stopped`, or `stopped in place`; running,
starting, stopping, locked, unknown, or unreadable state remains non-serving. A
stopped Instance's configured parent attachments must be explicitly detached
and both provider inventories must report absence before activation. If a
deleted Instance still has an orphan regional attachment, takeover remains
fail-closed and requires Scaleway support.

For `abnormal-takeover`, every previous-holder field is required and must match
the uncleared Lease. For `missing-lease-recovery`, the checkpoint fields must
match the immutable checkpoint Secret and the controller must verify the
offline all-Instance fencing gate from section 10.2. A disagreement between the
checkpoint, approval, cluster identity, attachment inventory, and runtime keeps
the controller non-serving.

After all checks, the successor consumes approval and acquires leadership in one
Lease compare-and-swap. The Lease records the latest approval Secret UID,
request ID, mode, consuming Pod UID, and consumption timestamp before any
mutation. The currently recorded Secret UID or request ID cannot authorize the
next transition. Kubernetes cannot recreate a deleted Secret UID, and the
operations guide requires a globally fresh UUID request ID, deleting the
approval Secret after successful consumption, and retaining prior audit
evidence. A new approval requires deleting and recreating the immutable Secret,
which gives it a new Kubernetes UID.

The closed Lease annotation names for that bounded latest-consumption tuple are:

```text
approvalConsumptionSecretUID
approvalConsumptionRequestID
approvalConsumptionMode
approvalConsumptionPodUID
approvalConsumedAt
```

They are all-or-none. Partial, malformed, or unknown `approvalConsumption*`
annotations keep the controller non-serving. Normal renewal and
graceful-release updates preserve this audit tuple unchanged. A later approved
recovery may replace it only in the same holder-acquisition compare-and-swap,
after the complete new approval and provider/offline fence have succeeded. The
new tuple must have a distinct Secret UID, a distinct request ID, and a strictly
newer consumption timestamp. Reuse of either currently recorded identity, an
older/equal tuple, or a standalone clear/patch remains forbidden. This bounded latest-audit design
supports repeated independently fenced failures without unbounded Lease
annotations; operators retain earlier tuples in the incident evidence before
authorizing the next recovery.

`missing-lease-recovery` uses the same object and one-time consumption rule. The
provisional Lease records when recovery was first observed; the approval must be
created afterward and match the installation, `activeClusterUID`, checkpoint
request ID, manifest digest, and all-Instance fencing scope. It is accepted only
when every parent claim matches the current cluster, all pre-recovery Instances
have been stopped or deleted, and fresh provider inventories contain no old or
unknown attachment. It never authorizes a different cluster UID. Checkpoint
holder evidence is retained only for audit and cannot reduce this fencing
scope.

CSI sidecar leader election must still be enabled where supported, but it is
not sufficient to protect driver-internal mutations.

Inside the single controller process, protect pool selection and accounting with
an in-process lock and use a per-logical-volume lock for create, publish,
unpublish, delete, and GC transitions.

For a new CreateVolume name, the selected pool lock remains held through the
durable reservation journal, creation or conclusive deterministic reread of the
initial `Reserved` allocation ConfigMap, and conclusive journal completion.
Releasing it after selection but before that reservation would allow two
different logical-volume locks to consume the same aggregate capacity budget.

Each configured pool has one permanent fixed-name ConfigMap journal. It is
created once, never deleted or name-reused, has a closed schema, and contains a
monotonic CAS generation plus either exact state `Idle` or `Pending`. `Pending`
contains the complete canonical `Reserved` allocation. The controller must
conclusively observe `Pending` before emitting the allocation POST. It returns
the journal to `Idle` only after the exact allocation is authoritative. A stale
update from an older controller generation uses the previous resourceVersion
and therefore conflicts with a successor's CAS; the Lease is not used as a
storage fence.

One fixed journal-set ConfigMap commits which per-pool journals are permanent.
It is created before fresh-install provider or filesystem mutation. Its
`Initializing` state makes a crash during first bootstrap or a pool addition
resumable without treating a missing established journal as new. Operational
startup may create only journals listed in `PendingPools`; after `Ready`, a
missing committed journal is a fatal durable-state inconsistency. V1 does not
remove a committed pool journal online.

If either the allocation or journal completion remains ambiguous, the process
also retains a fail-closed in-memory marker as defense in depth. Every successor
ensures all fixed journals and resolves every `Pending` intent before serving.
It reissues only the exact deterministic allocation from the journal, repeats
startup inventory/reconciliation when that creates a record, and keeps the
controller non-serving on an unavailable, malformed, mismatched, or unresolved
journal. A plain startup inventory without this journal protocol is not a
sufficient inter-generation barrier.

The process-local marker is not durable authority. Any path that conclusively
proves the exact allocation and an `Idle` journal, including an observation of
an already-applied journal CAS, removes it under the pool lock before normal
admission resumes. A retry whose allocation is still absent resolves its own
exact `Pending` intent before consulting the placement marker; an intent for a
different logical volume remains blocked. If the journal is conclusively
`Idle`, the retry may clear only the process-local marker. Before a subsequent
fresh `Begin`, the controller advances every `Idle` journal in the committed
journal-set. Because an older ambiguous `Begin` may target a different journal
ConfigMap, this installation-wide generation fence is required: advancing only
the newly selected pool would not invalidate the old pool's resourceVersion.

Cold start reconciles every `Pending` journal before lifecycle reconciliation.
It then performs a fresh complete allocation/PV/ownership inventory and runs
the lifecycle state machine once against that post-journal view. This ordering
prevents a `Reserved` allocation from advancing to `Ready` before its exact
Pending intent has been completed.

The controller must also use Kubernetes optimistic concurrency on allocation
records. The in-process lock prevents local races; Kubernetes resource versions
prevent stale writes after retries, restarts, or future HA work.

All mutating Controller RPCs and controller-owned background mutations must
also pass through one process-wide, context-aware semaphore before acquiring a
pool or per-volume lock and before any provider or filesystem mutation. The
production default and maximum supported v1 value are 10 concurrent mutations,
matching the tested scale envelope. Waiting for the semaphore must honor caller
cancellation and deadlines. A fixed acquisition order of global semaphore,
then per-logical-volume lock when needed, then pool lock when needed preserves
the existing create idempotency gate and prevents lock inversion. No code path
may acquire those locks in the opposite order. The controller must expose
bounded inflight and queued mutation gauges.

Create-only allocation reconstruction from authenticated PV/ownership or
ownership-only evidence follows the same admission and lock order. This applies
both at startup and during the internally authorized reconciliation performed
by `checkpoint resume`: active leadership is checked first, then the global
mutation gate is acquired, then the exact logical-volume lock, and only then may
the deterministic allocation ConfigMap be created or reread to resolve an
ambiguous create result. The checkpoint callback capability authorizes that
specific gate acquisition; it is not permission to bypass the gate.

The shared controller attachment/mount adapter additionally serializes one
`(nodeID,parentFilesystemID)` provider attach check and one parent mount
check-and-act at a time. These keyed sections are acquired only after the
global/per-volume/pool locks already owned by the calling state machine and
never call back into an outer lock. They prevent duplicate attach calls and
stacked controller mounts without changing the normative outer lock order.

Graceful termination is a bounded safety protocol, not a shorter normal
provisioning timeout:

1. on `SIGTERM`, immediately become unready and reject new mutations;
2. cancel every active RPC and internal mutation context so state machines stop
   at their next persisted safe boundary;
3. wait no longer than `controller.shutdownDeadline` for all mutation holders
   to exit and for durable records to be internally consistent;
4. write the graceful-release marker only when no mutator remains, no bootstrap
   attempt exists, checkpoint mode is inactive, leadership is still held, and
   the Lease compare-and-swap succeeds;
5. if the deadline, cancellation, consistency check, or marker write fails,
   exit without a graceful marker. The successor must then use abnormal
   takeover rather than assume a clean handoff.

The production defaults are a 90-second shutdown deadline and a 120-second pod
termination grace period. The grace period must exceed the shutdown deadline by
at least 30 seconds. This shutdown deadline applies only after pod termination
begins; normal attachment and deletion operations retain their documented
deadlines. CSI sidecars may terminate in any order, so correctness must not
depend on a sidecar remaining alive after the driver receives `SIGTERM`.
Deployment `progressDeadlineSeconds` must cover the complete startup-probe
budget plus at least five minutes; the production default is 3900 seconds for
the 3600-second startup budget.

### 10.4 Pool Full Behavior

If no parent has enough logical available capacity, `CreateVolume` must fail
with `ResourceExhausted`.

The error must include:

- pool name;
- requested size;
- number of parents;
- total observed capacity;
- `maxLogicalOvercommitRatio`;
- total logical capacity;
- total logical allocation;
- actual free bytes when available;
- recommendation to resize a parent or add another parent if node attach limits
  allow it.

### 10.5 Actual Parent Free-Space Guardrail

The driver must collect real parent filesystem usage with `statfs` where the
parent is mounted.

`CreateVolume` must refuse new allocations when either threshold would be
breached after accepting the requested PVC:

- `minFreeBytes`;
- `minFreePercent`.

This check complements logical accounting. It does not replace it.

Logical accounting already removes the configured reserve from usable capacity
before accepting reservations. The `statfs` check independently protects
against unmanaged existing data, workloads that exceed reservations, and
physical consumption that logical records cannot predict.

The placement input is a closed snapshot containing exactly one entry for every
configured parent. Provider or `statfs` unavailability is represented explicitly
on that parent's entry, never by omission. A missing, extra, or duplicate
parent, configured/candidate state disagreement, or capacity tuple that cannot
be recomputed exactly from observed size, configured reserve/ratio, and
aggregate allocation is an inventory error and fails closed before tie-breaking.
If any active provider-unavailable or node-incompatible entry could change a
failed selection, the result is respectively transient inventory unavailability
or failed node compatibility, never the conclusive pool-full error.

Required behavior:

- every active parent considered for placement must have fresh `statfs` data;
- stale, missing, or failed `statfs` data excludes that parent for the current
  request or fails the request with a clear transient error;
- on Linux, define `blockSizeBytes` as the positive `f_bsize` value and define
  `actualAvailableBytes` as checked multiplication of `f_bavail` by
  `blockSizeBytes`. The driver must use space available to an unprivileged
  writer (`f_bavail`), not total free blocks (`f_bfree`);
- reject the sample as `Unavailable` when `f_bsize` or `f_bavail` is negative or
  invalid, multiplication overflows, the authoritative observed parent size is
  zero, or computed available bytes exceed that observed size;
- compute the physical safety threshold as
  `max(minFreeBytes, ceil(observedParentSizeBytes * minFreePercent / 100))`
  with checked integer arithmetic;
- before subtracting, require `requestedBytes <= actualAvailableBytes`; then
  accept the physical guard only when
  `actualAvailableBytes - requestedBytes >= physicalSafetyThresholdBytes`;
- percentage configuration must be validated in the closed range `[0, 100]`;
- if one parent is below reserve but another active parent is valid, the driver
  must select the valid parent;
- no allocation record may remain in a reserving state after a free-space
  guardrail failure unless the failure state explicitly documents why capacity
  is still reserved and how to recover.

The implementation must expose the raw block values, computed available bytes,
observed parent size, threshold, and sample timestamp as bounded metrics and
structured diagnostics. Unit tests must cover zero, boundary equality,
one-byte-below threshold, negative/invalid fields, overflow, available larger
than observed size, and percentage-rounding cases.

### 10.6 Archived and Retained Data Accounting

Archived and retained data can still consume physical parent capacity after a
PV is deleted.

The driver must create durable records for archived, retained, and deleted
terminal states.

Archived and retained records must remain visible in metrics and pool summaries
until an operator runs documented garbage collection because the data still
consumes parent capacity.

Deleted records are non-reserving tombstones. They are kept for idempotent CSI
retries and auditability, not capacity accounting.

Allocation and ownership writes follow one closed crash-recovery table. An
"exact predecessor" means the same immutable volume identity and revision/state
immediately preceding the same persisted operation ID and paths. Only that
one-sided lag may move forward automatically:

| Transition | First durable write | Second durable write | Permitted crash repair |
| --- | --- | --- | --- |
| create completion | detailed ownership `Ready` after directory verification | allocation `CreatingDirectory -> Ready` | verify owner/directory/mode, then advance allocation |
| publish | add node to allocation union | mirror union to ownership | restore union allocation-first, then mirror |
| verified unpublish | remove node from ownership | remove node from allocation | restore union first; retried unpublish repeats fencing and removal |
| delete prepare | allocation `Ready -> Deleting` with `deleteOperationID` and paths | ownership `Ready -> Deleting` with the same intent | advance only the exact `Ready` ownership predecessor |
| delete remove-start | allocation `Deleting` adds `deleteRemoveStartedAt` | matching ownership `Deleting` adds the same evidence | mirror only the exact ownership predecessor for the same delete operation and paths; removal waits for agreement |
| archive/retain completion | allocation `Archived` or `Retained` | matching detailed ownership terminal state | advance only the matching `Deleting` ownership predecessor |
| physical delete completion | detailed allocation `Deleted` | compact ownership `Deleted` | compact only the matching `Deleting` ownership predecessor |
| GC prepare/progress | allocation GC phase and immutable `gcOperationID`/paths | matching detailed ownership phase | advance only the exact prior ownership phase for the same GC |
| GC completion | detailed allocation `Deleted` | compact ownership `Deleted` | compact only the matching terminal GC predecessor |
| allocation compaction | ownership is already compact `Deleted` | allocation detailed `Deleted -> compactDeleted` | retry allocation compare-and-swap only |

Two different operation IDs, paths, mappings, successor states, or terminal
outcomes are a true conflict and remain fail-closed. A predecessor on one side
and the exact successor on the other is an expected crash window, not a
conflict. No recovery rule may move either record backward.

The valid terminal pairings are explicit:

- `Archived` allocation plus matching detailed `Archived` ownership;
- `Retained` allocation plus matching detailed `Retained` ownership;
- detailed `Deleted` allocation plus matching compact `Deleted` ownership
  during the allocation retention window;
- compact `Deleted` allocation plus matching compact `Deleted` ownership after
  allocation compaction;
- `deletedUnknown` allocation with no ownership record.

Garbage collection is not automatic in v1.

The v1 project must provide a documented admin garbage-collection path for
terminal records. The bundled admin CLI must submit a request to the active
leader by compare-and-swap updating only the `gcRequestID`, `gcRequestedMode`,
`gcExpectedState`, and `gcRequestedAt` request fields in the target allocation
ConfigMap. The active controller validates and executes the request. The CLI must
never change lifecycle state or filesystem paths directly.

Required behavior:

- support dry-run;
- require namespace, driver name, logical volume ID, and expected terminal
  state;
- require active controller leadership before any mutation;
- acquire the same allocation record lock/concurrency guard used by
  `CreateVolume` and `DeleteVolume`;
- validate the allocation record, mapping hash, parent filesystem ID, base
  path, delete policy, and driver-owned ownership record before any filesystem
  mutation;
- refuse to operate on non-terminal records;
- refuse to operate if any PV still references the logical volume ID;
- persist `gcOperationID`, `gcTargetPath`, a collision-resistant
  `gcQuarantinePath` under `<basePath>/.deleted`, and `gcStartedAt` before the
  first filesystem mutation;
- mirror that exact GC operation ID, source path, quarantine path, expected
  predecessor state, and phase into the detailed ownership record with a
  crash-durable revision update; no rename is allowed until both records agree;
- atomically move the validated archived or retained target once to the
  persisted GC quarantine path;
- persist `gcRemoveStartedAt` in the allocation record and then mirror it to the
  ownership record before recursively removing only that validated quarantine
  path;
- after successful archived/retained cleanup, update the allocation record to
  `Deleted`, set `reservesCapacity: false`, and preserve the original
  `deleteOperation`, archived/retained path, GC fields, and timestamps for
  auditability;
- after the allocation `Deleted` tombstone is durable, replace the exact
  matching ownership predecessor with the permanent compact `Deleted` ownership
  tombstone from section 6.13. Return success only after both terminal records
  agree on immutable identity and GC outcome;
- compact an old `Deleted` ConfigMap in place only after the configured detailed
  record retention window; never delete the deterministic per-volume tombstone;
- log an audit summary containing driver name, logical volume ID, parent
  filesystem ID, target path, previous state, final state, and operator-visible
  result.

GC retries must be idempotent and state-driven:

- allocation and ownership contain conflicting operation IDs, paths, mappings,
  or successor states -> fail closed without filesystem mutation;
- allocation contains the persisted next GC phase while ownership is its exact
  predecessor for the same `gcOperationID` and paths -> mirror that phase to
  ownership, then continue;
- source present + quarantine absent -> perform the persisted rename;
- source absent + quarantine present -> continue removal;
- source absent + quarantine absent + `gcRemoveStartedAt` set -> persist
  the `.deleted` directory durability barrier, then persist allocation
  completion and `Deleted` because matching remove intent exists and absence is
  now durable, and finally terminalize ownership;
- source absent + quarantine absent without `gcRemoveStartedAt` -> fail closed
  with a manual recovery message;
- allocation `Deleted` plus a matching detailed `Archived` or `Retained`
  ownership predecessor with the same `gcOperationID`, paths, mapping, and
  remove-started evidence -> write the compact ownership tombstone without
  touching the filesystem;
- matching persisted allocation and ownership completion -> return success
  without touching the filesystem.

The request submit boundary must preserve that idempotency after completion. A
detailed `Deleted` allocation accepts only the same completed execute request
envelope. After allocation compaction intentionally drops the request-only
fields, a compact tombstone with completed GC evidence accepts an execute retry
only when its requested expected source state is exactly derivable from the
preserved archive/retain operation. This terminal observation is read-only; it
never creates a new request or repeats filesystem work. A dry-run, mismatched
source state, absent completed GC evidence, or conflicting detailed request is
rejected.

A dry-run may persist only its bounded request/audit fields in the allocation
record. It must not write GC lifecycle fields, modify ownership metadata, or
touch the filesystem.

### 10.7 Upgrade and Schema Compatibility

The v1 release must freeze the Kubernetes-facing and recovery contract:

- CSI driver name;
- volume handle format;
- mapping hash inputs;
- required `volume_context` keys;
- allocation record schema;
- detailed and compact ownership record schemas;
- parent-global owner record schema;
- ownership record path;
- installation identity Secret contract;
- fixed controller leadership Lease name;
- Lease holder, bootstrap, and graceful-release annotation schemas;
- immutable operator approval Secret schema;
- quiesced checkpoint manifest and parent ownership inventory schema;
- controller namespace;
- canonical logical-volume, request-hash, and mapping-hash algorithms;
- pool `basePath` for any pool with existing records.

Existing PVs must never be mutated in place to change
`spec.csi.driver`, `spec.csi.volumeHandle`, or required immutable CSI
attributes.

Every released driver version must either:

- read and operate on all previously released schemas; or
- run an explicit idempotent migration before serving CSI operations.

Unknown newer schema versions must fail closed with a clear error. New fields in
future schema versions must be optional or defaulted when reading v1 records
unless a documented migration is required.

Online upgrades must support the mixed `N`/`N-1` interval created by independent
controller and DaemonSet rollouts. Version `N` must not write a schema that the
supported `N-1` node plugin cannot read until every eligible node advertises the
new `nodeConfigGeneration`. The release suite must cover old-node/new-controller,
new-node/old-controller, interruption, and rollback. A schema migration that
cannot preserve `N-1` readability is an explicit offline, non-rollbackable
upgrade: quiesce the driver, take a completed checkpoint, stop controller and
node plugins, run the idempotent migration, and start only version `N`.

The repository must include compatibility fixtures for:

- old PV CSI attributes;
- old allocation ConfigMaps;
- old ownership records;
- old permanent reservation-journal records and the installation-wide
  reservation-journal-set commitment;
- compact allocation and compact ownership tombstones;
- old parent-global owner records;
- Lease coordination evidence and operator approval Secret fixtures;
- quiesced checkpoint manifests and per-parent ownership inventories;
- archived, retained, and deleted terminal states.

The chart rejects structurally invalid candidate values, but it cannot prove the
historical contents of an externally owned identity Secret or parent claim.
The required `csi-admin upgrade preflight` command compares candidate immutable
values, the fixed Lease name, and identity hashes with live
allocation/ownership state before the old controller is stopped. The online
preflight permits a new parent but rejects removal or any pool/base-path-hash
change for an existing parent. The candidate must declare readers for every
allocation and ownership schema observed live, must read its own writer schema,
and must not write an allocation or ownership schema outside the currently
deployed N-1 node-reader contract. At least one valid Ready node configuration
generation and the candidate generation are required audit inputs. Controller
startup is the final fail-closed authority and performs no mutation when
`driver.name`, an existing pool `basePath`,
installation identity, ownership path, or schema generation disagrees. The
operations guide must state that bypassing preflight can cause a safe outage,
never silent adoption. Rollback limits must be documented: once an explicitly
offline migration writes a newer record version, older versions are unsupported
unless the release contract says otherwise.

The controller-local live reader requires active leadership before and after
the preflight snapshot. It reuses the complete startup allocation/PV/claim/
ownership validator, derives each existing parent mapping from the configured
pool plus its authenticated parent-global claim, and lists a fresh joined
Node/CSINode/node-plugin Pod inventory. Only non-deleting Ready nodes with a
Ready plugin Pod and registered driver contribute node generations; a matching
Pod without a generation is corruption, not an old default. V1 always includes
schema `1` in the current allocation and ownership reader sets even when the
installation has no per-volume records, and adds every schema observed in live
durable records. Unknown record types or incomplete parent inventories fail the
read before candidate comparison.

The permanent reservation-journal and reservation-journal-set formats are
explicitly immutable for the whole v1 compatibility line. Both use schema `1`;
every v1 controller must retain those readers and writers, and compatibility
fixtures must cover both objects. Changing either format requires an explicit
post-v1 design and migration contract; the v1 upgrade payload therefore does
not grow a second set of redundant reader/writer declarations.

Migration from the official Scaleway File Storage CSI driver or from existing
manually-created PVs is out of scope for v1 automatic behavior. The documented
safe path is: create new PVCs with this driver, copy data, validate, cut over
workloads, and keep rollback instructions.

## 11. Security Requirements

### 11.1 Path Safety

Path handling is critical.

All paths must be validated through a dedicated package. Required tests:

- rejects empty path;
- rejects `/`;
- rejects `..`;
- rejects absolute user-provided subpaths;
- rejects paths outside `basePath`;
- rejects symlink escape if deletion follows links;
- rejects symlink replacement between validation and mutation;
- rejects bind-mount or different-device entries inside a deletion tree;
- refuses recursive delete unless target is proven safe.

### 11.2 Directory Ownership

The driver must support:

- directory mode;
- UID;
- GID.

Defaults must be conservative and documented.

The driver must not recursively change ownership or mode by default. It should
set ownership and mode only on the logical volume root and on driver-owned
internal metadata directories.

Driver-owned internal metadata directories must remain owned by the driver and
must not be writable by workload UID/GID defaults. The workload-writable data
directory and the driver-owned metadata directory are separate trust zones.

### 11.3 Privileges

The node DaemonSet requires host mount privileges.

The controller also needs filesystem access for `CreateVolume` and
`DeleteVolume` directory lifecycle. The v1 implementation must run the
controller pod with the minimum mount privileges needed to attach, mount,
`statfs`, archive, retain, and delete parent file system directories in a
controller-owned path.

A privileged filesystem-manager sidecar is out of scope for v1. Do not add a
second controller filesystem architecture unless the direct privileged
controller model fails during implementation or real Scaleway e2e testing.

Do not give the controller access to the kubelet workload mount root unless it
is strictly required. Controller mounts are for provisioning cleanup only.

The Helm chart must render separate ServiceAccounts for:

- the controller pod, shared by the driver container and its controller
  sidecars because Kubernetes ServiceAccounts are pod-scoped;
- the node-plugin pod, shared by the node driver, registrar, and liveness
  container.

The Helm chart must define security contexts explicitly.

Required defaults:

- no `hostPID` unless a tested mount requirement proves it is necessary;
- no `hostNetwork` unless a tested requirement proves it is necessary;
- `readOnlyRootFilesystem: true` for non-mount containers;
- `allowPrivilegeEscalation: false` wherever mount operations do not require it;
- drop Linux capabilities on every non-mount container;
- use the explicit privileged mount-container contract below rather than an
  untested partial capability profile;
- hostPath mounts limited to the driver mount roots and kubelet plugin paths
  required by CSI;
- mount propagation enabled only on the exact mount roots that require it;
- no wildcard hostPath mounts such as `/`.

The v1 production mount security contract is deliberately conservative and
explicit:

- the mount-capable controller driver container runs `privileged: true` because
  it performs kernel `virtiofs` mounts and destructive filesystem lifecycle
  operations;
- the controller parent mount root is an `emptyDir` mounted only into the
  controller driver container. It is not a hostPath and has no access to the
  kubelet workload mount root;
- the node driver container runs `privileged: true` and receives only the exact
  kubelet plugin, plugins-registry, pod, and driver parent-mount hostPaths needed
  by CSI;
- node mount propagation is `Bidirectional` only on the exact kubelet/driver
  mount roots that must propagate mounts to the host;
- `/dev/fuse`, `hostPID`, and `hostNetwork` are not mounted or enabled in v1;
- the controller image includes the standard Linux mount utilities needed for
  `virtiofs`; no NFS client is installed for this feature;
- all non-mount sidecars run non-root where their upstream image supports it,
  with read-only root filesystems, no privilege escalation, and all capabilities
  dropped. The upstream `node-driver-registrar` is the explicit exception: it
  runs as UID/GID 0 with all capabilities dropped because it must create its
  registration socket in kubelet's root-owned shared `plugins_registry`
  hostPath. The chart must not chown that shared directory because doing so can
  break other CSI drivers;
- the dedicated driver namespace must use the Pod Security admission level
  required for these two privileged mount containers and must be documented as
  a security-sensitive namespace.

A future release may replace full privilege with a tested narrower capability
set, but v1 must not advertise an unproven `SYS_ADMIN`-only profile.

Real Kapsule e2e must run the rendered controller security context, mount a
parent with `virtiofs`, run `statfs`, create a directory, archive/delete it, and
restart or reschedule the controller.

### 11.4 RBAC

The controller pod ServiceAccount must receive the least-privilege union of the
RBAC shipped by the exact pinned external-provisioner and external-attacher
versions. The chart must derive this from the version-matched upstream manifests
rather than maintaining an incomplete hand-written subset. At minimum the union
must cover:

- PersistentVolume get/list/watch/create/delete/patch operations required by
  external-provisioner and external-attacher;
- PersistentVolumeClaim get/list/watch/update/patch operations required by
  external-provisioner;
- StorageClass get/list/watch;
- VolumeAttachment get/list/watch/update/patch, including status, for
  external-attacher and the driver's in-use deletion check;
- CSINode get/list/watch for external-attacher and node registration preflight;
- Node get/list/watch for homogeneous-node preflight;
- get only the `kube-system` Namespace object to derive the immutable
  `activeClusterUID`;
- Event create/patch/update;
- namespace-scoped Lease permissions required by sidecar leader election and
  driver leadership coordination.

Driver-specific controller permissions are:

- get/list/watch/create/update/patch ConfigMaps in the driver namespace for
  allocation records only;
- get/create/update/patch/watch the controller leadership Lease in the driver
  namespace, constrained to fixed resource name
  `scaleway-sfs-subdir-csi-controller` wherever Kubernetes RBAC supports
  `resourceNames`; the runtime ServiceAccount receives no Lease delete
  permission;
- get/list/watch Pods in the dedicated driver namespace only, so homogeneous
  node preflight can discover node-plugin Pods by the chart's fixed labels and
  verify `spec.nodeName`, absence of `deletionTimestamp`, and Ready condition;
- get only the fixed `sfs-subdir-controller-approval` and
  `sfs-subdir-checkpoint` Secrets by resource name; no Secret
  list/watch/create/update/patch/delete permissions. The kubelet projects the
  separately named identity and credential Secrets into the controller pod;
- no write access to arbitrary Secrets, workloads, Nodes, or CSINodes.

The node-plugin ServiceAccount must receive no Kubernetes API permissions unless
a pinned registrar or liveness version demonstrably requires them. Node identity
comes from the local Scaleway metadata service, not Kubernetes Node mutation.

The runtime controller ServiceAccount must not get `delete` on allocation
ConfigMaps. V1 compacts terminal records in place with update/patch and never
deletes the permanent per-volume tombstone.

The default namespace must be dedicated to the driver, for example:

```text
scaleway-sfs-subdir-csi
```

Do not install allocation records into `kube-system` by default.

Allocation ConfigMaps must use:

- a fixed name prefix;
- bounded DNS-safe owner labels;
- bounded DNS-safe driver labels;
- bounded DNS-safe volume ID labels;
- hash labels for raw or long values.

Required label schema:

```text
app.kubernetes.io/name=scaleway-sfs-subdir-csi
file-storage-subdir.csi.urlab.ai/installation-id=<bounded installation id>
file-storage-subdir.csi.urlab.ai/logical-volume-id=<bounded logical volume id>
file-storage-subdir.csi.urlab.ai/request-name-hash=<hash>
file-storage-subdir.csi.urlab.ai/volume-handle-hash=<hash>
file-storage-subdir.csi.urlab.ai/pool-name-hash=<hash>
file-storage-subdir.csi.urlab.ai/parent-filesystem-id-hash=<hash>
file-storage-subdir.csi.urlab.ai/state=<bounded state>
```

Raw CSI names, parent filesystem IDs, pool names, and paths must be stored in
ConfigMap data or annotations. The implementation must include tests for
maximum-length names, invalid label characters, and hash collision handling.

Labels are not an RBAC boundary, but they make operations and cleanup safer.

RBAC must be split into minimal Roles/ClusterRoles:

- namespace-scoped allocation record Role;
- namespace-scoped controller-leadership Lease Role;
- namespace-scoped leader-election Role;
- namespace-scoped resource-name-constrained get Role for the immutable operator
  approval and checkpoint Secrets;
- namespace-scoped node-plugin Pod read Role with get/list/watch only;
- version-matched controller sidecar ClusterRoles;
- cluster-scoped driver read access to PVs, VolumeAttachments, CSINodes, and
  Nodes where not already included by the sidecar union, plus resource-name
  constrained get access to the `kube-system` Namespace;
- node plugin permissions only where required.

### 11.5 Secrets

Secrets must be mounted as environment variables or files only in pods and
containers that strictly need them.

Scaleway API credentials must not be mounted into:

- the node plugin;
- `node-driver-registrar`;
- liveness probes;
- any container that does not call the Scaleway API.

No credentials in:

- ConfigMaps;
- StorageClasses;
- Helm rendered plain text examples;
- logs;
- README command examples with real values.

## 12. Testing Strategy

Testing is part of the deliverable, not a follow-up.

### 12.1 Supported v1 Scale Envelope

V1 must validate and document at least this envelope:

- 4 pools;
- 4 unique parent filesystems, subject to the lower live limit of every eligible
  Instance;
- 6 eligible workload nodes;
- 1,000 active logical volumes;
- 10,000 permanent compact `Deleted` tombstones;
- 10 concurrent mutating CSI requests in the single controller process.

These are tested support limits, not hardcoded driver limits. Configuration or
usage above them must be documented as unvalidated rather than silently
rejected, except where a real provider, Kubernetes, or safety limit applies.
The v1 allocation and ownership inventory safety cap is 16,384 entries. This
is deliberately above the tested 1,000 active plus 10,000 permanent tombstones
envelope so crash-temporary records fit without turning the tested support
limit itself into an outage threshold; exceeding the safety cap fails closed.
`csi-admin` uses the same 16,384 cap for allocation records and applies smaller
domain limits only after filtering driver-relevant PVs, PVCs, Pods, and
attachments; unrelated cluster objects do not consume a driver inventory
bound.

The implementation must remain O(number of records plus PVs) during startup and
must not perform an API request per compact tombstone when a paginated list or
informer cache can provide the same result. A fake-provider scale test must
reconcile the full envelope within 5 minutes and remain below the chart's default
512 MiB controller memory limit on the documented CI reference runner. Real
Kapsule E2E must provision at least 100 PVCs, while workload pods may sample a
smaller documented subset to control test cost.

### 12.2 Unit Tests

Required unit tests:

- volume handle parsing and serialization;
- volume handle length is bounded to 128 bytes or less;
- volume handle delete recovery without `volumeContext`;
- `volume_context` accepts every key/value at 128 UTF-8 bytes and a complete
  map at 4 KiB, rejects the corresponding one-byte-over boundaries including
  multi-byte input, and performs the check before durable or filesystem
  mutation;
- invalid handle rejection;
- mapping hash mismatch rejection;
- allocation record create/read/update and terminal compaction behavior;
- exact schema validation for `detailed`, `compactDeleted`, and
  `deletedUnknown`, including rejection of `recordKind: full`, unknown fields in
  compact records, and state/`reservesCapacity` combinations that do not match
  the selected variant;
- allocation state transitions;
- `reservesCapacity` accounting by allocation state;
- deterministic `logicalVolumeID` derived from `CreateVolumeRequest.name`;
- atomic allocation record creation prevents duplicate records for the same
  `CreateVolumeRequest.name`;
- parent-global owner record creation is atomic and rejects mismatched
  `installationID`;
- first-parent claim uses only the fixed root claim
  `/.sfs-subdir-csi-owner.json`; crash injection before and after temporary-file
  fsync, no-replace rename, and root-directory fsync leaves either no claim or
  one complete immutable claim, never a partially initialized metadata tree;
- a losing or stale bootstrap attempt cannot replace the fixed claim, adopt a
  foreign temporary claim, or remove a temporary claim belonging to another
  attempt;
- first-parent claim persists its bootstrap attempt before attach; injected
  crashes before attach, after an accepted or ambiguous attach, and before
  owner creation resume or roll back only the exact journaled attachment, while
  a crash after matching owner creation clears only the stale attempt;
- bootstrap replay rejects same-attempt claim from a changed Instance and
  permits only provider-fenced offline rollback there; it rejects a changed parent or
  cluster, foreign or additional attachment, existing logical state, and a
  malformed or missing attempt record;
- two installations racing from the same live controller Instance cannot use
  either bootstrap journal to detach the other's attachment; automatic rollback
  from any live Instance is rejected;
- parent-global ownership rejects a copied installation identity from a
  different `activeClusterUID`;
- duplicate parent IDs across pools and claims from another installation are
  rejected;
- `CreateVolume` rejects reuse of a terminal `Archived`, `Retained`, or
  `Deleted` tombstone permanently in v1;
- compatible capacity-range replay returns the existing volume even when the
  request hash differs;
- a compatible `Ready` replay returns the existing mapping without provider
  refresh, `statfs`, capacity, lifecycle, or placement checks;
- `Reserved` and `CreatingDirectory` replay uses the persisted parent without
  selecting a new one;
- incompatible capacity, capabilities, or immutable parameters return
  `AlreadyExists`;
- request map and capability ordering do not change canonical compatibility;
- `DeleteVolume` state transitions for archive/delete/retain/retry;
- `DeleteVolume` rejects an empty ID with `InvalidArgument`, returns success for
  a non-empty foreign or impossible ID without lookup or tombstone creation,
  and fails closed when a parseable driver handle conflicts with an existing
  logical record or mapping hash;
- delete preparation persists archive/quarantine targets before filesystem
  rename;
- physical delete does not begin until matching allocation and ownership
  records contain the same `deleteRemoveStartedAt`; every crash between the two
  writes and removal resumes forward without deleting from one-sided intent;
- retry after crash between rename and terminal state resumes safely;
- terminal `Deleted` tombstone remains non-reserving after `DeleteVolume`;
- valid driver handle with fully missing state returns idempotent delete success
  without filesystem mutation and persists a minimal deleted-unknown tombstone
  only after all authoritative lookups conclusively report absence;
- `deletedUnknown` schema accepts only its defined fields and is rejected from
  filesystem mutation, capacity accounting, reconstruction, and name reuse;
- unavailable, forbidden, or timed-out delete lookups do not persist a
  deleted-unknown tombstone;
- `DeleteVolume` fails with `FailedPrecondition` while a VolumeAttachment
  remains;
- publish persists a deduplicated node ID before returning success and normal
  unpublish on a Ready matching Node/CSINode removes it idempotently;
- unpublish with an empty `node_id` targets the union of both persisted sets,
  applies the safety decision to every node, and succeeds only when both sets
  are empty and agree;
- all-node unpublish interrupted between targets or between ownership and
  allocation updates preserves every unresolved fence and resumes
  idempotently;
- forced unpublish after Node or CSINode loss, an out-of-service taint, identity
  mismatch, or unreadable Kubernetes state retains the node fence until
  provider fencing is conclusive; a deleted VolumeAttachment alone never clears
  it;
- injected crashes between allocation and ownership publish/unpublish updates
  restore the deduplicated union in allocation-first order; generic recovery
  never removes a node ID, and only a retried, revalidated unpublish may do so;
- delete, archive, and GC remain blocked by a stale `publishedNodeIDs` entry;
- a stale published node is cleared only after provider-confirmed Instance
  deletion or non-running state plus absence of the logical volume's parent
  attachment;
- `starting`, `stopping`, `locked`, unknown, and unreadable Instance states do
  not satisfy the published-node fence;
- the complete Instance state table is enforced consistently; an Instance
  `NotFound` response cannot clear a fence while regional inventory still shows
  an orphan parent attachment;
- publish racing with deletion cannot succeed after the prepared `Deleting`
  transition;
- request hash canonicalization and semantic replay compatibility;
- idempotent `CreateVolume` before PV creation;
- recovery from `Reserved` and `CreatingDirectory` records;
- driver-owned ownership record write/read/validate;
- ownership-only recovery reconstructs the allocation from the complete
  immutable envelope and never from current defaults;
- missing driver-owned ownership record fails closed;
- mismatched driver-owned ownership record fails closed;
- expected predecessor ownership state is repaired only for the explicitly
  documented delete or GC transition after each injected update crash point;
- a compact ownership tombstone on a configured parent reconstructs only its
  exact missing compact allocation tombstone; missing, extra, or mismatched
  permanent tombstones keep startup non-serving;
- workload-visible marker deletion does not authorize or block unsafe deletion;
- pool config validation;
- node compatibility preflight validation;
- publish rejects a node ID that does not match an eligible Node's CSINode
  registration or configured Project/region;
- parent selection with one parent;
- parent selection with multiple parents;
- parent selection skips an actual-full parent when another parent is valid;
- parent lifecycle `active` / `draining`;
- parent removal rejected while reserving records, PVs, or live detailed
  ownership records reference it;
- online parent removal is rejected in v1 even after records are drained;
- the offline decommission validator requires no references, mounts, child bind
  mounts, or attachments before values removal and proves every remaining
  compact ownership tombstone matches its allocation tombstone;
- startup after valid offline decommission accepts detailed and
  `compactDeleted` non-reserving tombstones that retain a historical parent ID,
  as well as `deletedUnknown` tombstones with no parent field, while any
  reserving record, PV, or live detailed ownership record for that parent still
  fails closed; startup and checkpoint do not remount an unconfigured parent to
  inspect compact ownership tombstones;
- parent-global ownership conflict fails startup preflight;
- pool full behavior;
- parent size refresh updates available capacity;
- resize of parent file system affects future placement;
- the fake provider's unexpected size decrease marks only that parent
  `critical-size-regression`, blocks new placement there, leaves existing
  mounts untouched, and clears only after an authoritative observation reaches
  the previous accepted size;
- every `FileSystem.Status` row is enforced, including runtime recovery from
  `creating`/`updating` to `available` and fail-closed unknown values;
- exact fake-`statfs` calculation from `f_bavail * f_bsize`, checked
  multiplication/subtraction, threshold maximum and percentage ceiling,
  including equality, one-byte-below, zero, invalid/negative, overflow, and
  available-larger-than-observed-size cases;
- logical capacity subtracts the larger byte/percentage safety reserve before
  applying the overcommit ratio, including boundary and overflow cases;
- aggregate attach-budget set-union validation deduplicates already attached
  configured parents;
- active official-CSI or manual attachments consume slots and make unsafe
  same-node operation fail closed;
- paginated, cross-zone `ListAttachments` discovers unknown, orphaned, and
  foreign-resource attachments and fails closed on unreadable inventory;
- `NumberOfAttachments` mismatch with the deduplicated regional list is treated
  as transient and never as conclusive absence;
- `ListAttachments` and `Server.Filesystems` disagreement blocks mutations;
- attachment state-machine table tests cover `available`, `attaching`,
  `detaching`, unknown state, conflict, lost response, API timeout,
  cancellation, bounded backoff, deadline, inventory re-read, and exact attach
  call count;
- controller workload publish and controller lifecycle attachment both use the
  same provider state machine and exact target-node zone;
- node eligibility revalidation after node changes;
- archive/retain accounting;
- delete policy `archive`;
- delete policy `delete`;
- delete policy `retain`;
- manual GC refuses non-terminal records;
- manual GC dry-run may persist only its bounded request/audit envelope; it
  does not write lifecycle progress, ownership metadata, or filesystem paths;
- manual GC for archived/retained data releases logical capacity only after
  validated cleanup;
- manual GC mutating path is rejected outside the active leader;
- GC resumes safely after crashes before rename, after rename, after
  `gcRemoveStartedAt`, and after completion;
- a crash after allocation GC completion but before ownership compaction
  terminalizes only the matching ownership record; conflicting GC operation
  evidence remains fail-closed;
- detailed `Deleted` ConfigMaps become `compactDeleted` in place and remain
  permanent;
- matching detailed ownership records become permanent compact ownership
  tombstones and are never deleted by compaction or GC;
- atomic filesystem metadata writes survive every injected crash point;
- logical-directory creation, archive/quarantine rename, recursive removal, and
  bootstrap-temp cleanup each execute the required file/directory durability
  barriers; injected barrier failure leaves the last durable non-terminal state
  and a retry resumes without guessing completion;
- controller Lease loss cancels mutations and terminates the process;
- graceful shutdown writes the exact release marker and a different Pod UID
  consumes it once; failed marker writes, malformed or consumed markers, and a
  manually precreated or restored empty Lease cannot authorize automatic
  handoff;
- graceful shutdown writes no release marker while a bootstrap attempt exists
  or checkpoint mode is active;
- abnormal takeover requires an exact unconsumed immutable approval Secret,
  verifies its Kubernetes UID and complete previous-holder identity, confirms
  the provider fence, and records consumption in the Lease before mutation;
- absent-or-empty-Lease recovery with durable state requires a consumed
  same-cluster approval, while discovery proves a fresh empty installation
  before initialization;
- provisional discovery never replaces an expired non-empty holder before the
  exact approval is consumed, and Helm lifecycle operations do not render or
  alter the runtime Lease coordination annotations and fields;
- leadership duration ordering and watchdog deadline are enforced;
- the leadership Lease name is the fixed
  `scaleway-sfs-subdir-csi-controller` constant in runtime, parent claims,
  checkpoints, RBAC, preflight, and upgrade compatibility; values cannot
  override it;
- controller rollout strategy prevents concurrent mutating controllers;
- `SIGTERM` immediately removes readiness, rejects new work, cancels active
  contexts, writes a graceful marker only after zero mutators and a successful
  Lease compare-and-swap, and exits without that marker at the shutdown
  deadline; the normal ten-minute attach deadline remains unchanged outside
  shutdown;
- a burst of at least 100 fake mutating requests never exceeds the configured
  process-wide limit of 10, queued calls honor cancellation, and lock-order
  tests detect an opposite pool/per-volume acquisition;
- controller and node CSI `Probe` report separate cached readiness without
  provider or filesystem I/O; controller-only failure and leadership state do
  not make an otherwise healthy node plugin unready;
- checkpoint preparation atomically becomes unready, rejects new mutations,
  drains active mutators, pauses compaction and repair, and remains quiesced
  until explicit resume; SIGTERM, Lease loss, or a crash invalidates the
  candidate export and never creates a graceful-release marker;
- checkpoint creation rejects transitional state, incomplete object sets,
  allocation/PV hash mismatches, and missing or duplicate parent inventory
  entries; the fixed immutable checkpoint Secret accepts only a complete
  manifest whose request ID, digest, object aggregate, and parent inventories
  all match;
- at the supported 10,000-tombstone envelope, the checkpoint Secret remains
  bounded by parents/images rather than volumes; restored Kubernetes UIDs and
  resourceVersions may differ, while one missing, extra, duplicate, or
  content-changed logical object changes the aggregate and blocks recovery;
- missing-Lease restore remains non-serving until every pre-recovery controller
  and cluster Instance is offline and fresh regional inventories prove no old
  or unknown attachment; fencing only the historical checkpoint holder is
  rejected, including the A-checkpoint/B-successor/C-recovery sequence;
- restore from a stale checkpoint with newer, extra, or missing
  detailed/compact parent ownership remains non-serving;
- read-only `NodePublishVolume` creates a read-only bind mount;
- idempotent re-publish accepts only the exact same source, capability,
  filesystem type, and read-only mode; every conflict returns `AlreadyExists`
  without remounting;
- `NodePublishVolume` creates a missing absolute target and removes it on mount
  failure only when that call created it and it is unmounted and empty;
- `NodeUnpublishVolume` is idempotent for absent or already-unmounted targets
  and cleans only a validated, unmounted, empty driver target directory;
- `NodeStageVolume` validates but never creates or removes the CO-owned staging
  directory; `NodeUnstageVolume` unmounts but never removes it, while mount
  idempotency, conflict, and rollback remain exact;
- `NodeStageVolume`, `NodePublishVolume`,
  and `ControllerPublishVolume` reject every missing, unknown, or mismatched
  immutable volume-context field before any filesystem, provider, or durable
  side effect; `ValidateVolumeCapabilities` alone may resolve an omitted map
  read-only from the handle and durable records, while a supplied map must match
  exactly;
- `NodeStageVolume` rejects a local node ID absent from the ownership
  `publishedNodeIDs` fence;
- `NodePublishVolume` revalidates the same ownership fence and cannot publish
  from an obsolete staging path after controller unpublish;
- `NodeStageVolume` fails when a `Ready` volume data directory is missing and
  does not recreate it;
- concurrent stages sharing one parent serialize and validate the existing
  parent mount;
- all four Node lifecycle RPCs serialize by logical volume with cancellable
  lock acquisition; stage/publish also take the parent lock in the fixed order,
  and stress tests prove that concurrent publish/unpublish cannot create a
  stacked mount or unmount another call's target;
- publish proves the staging mount is the exact expected logical bind backed by
  the expected parent; unpublish and unstage refuse foreign, aliased, or stacked
  sources before unmounting;
- empty or `virtiofs` filesystem type succeeds, while `ext4`, `xfs`, non-empty
  mount flags, and non-empty accessibility requirements are rejected;
- safe path join;
- unsafe path rejection;
- reserved directory name rejection;
- user data cannot overwrite driver-owned metadata paths;
- symlink in base path rejection;
- symlink replacement race between validation and mutation is rejected;
- symlink as logical volume directory rejection;
- symlink inside a deleted tree does not escape;
- bind-mount or different-device entry inside a deletion tree is rejected;
- archive destination collision rejection;
- directory name sanitization;
- PVC metadata directory naming with `extra-create-metadata`;
- the three external-provisioner metadata keys are accepted while unrelated
  unknown parameters are rejected;
- fallback directory naming without PVC metadata;
- idempotent create;
- idempotent delete;
- idempotent attach;
- idempotent mount with fake mounter;
- `SINGLE_NODE_WRITER` permits only an identical retry at the same node target,
  rejects a second target on that node and a second distinct node, while
  `MULTI_NODE_MULTI_WRITER` permits the documented RWX use; every other access
  mode, including reader-only modes, is unsupported in v1;
- unsupported capabilities and incompatible existing targets return the exact
  RPC-specific status defined in section 6, and
  `ValidateVolumeCapabilities` returns OK with `confirmed` unset for a
  well-formed unsupported capability;
- exact Identity, controller, and node CSI capability advertisement on both
  sockets, including the same plugin name/non-empty vendor version, exactly
  `CONTROLLER_SERVICE`, and no topology capability;
- node configuration generation changes whenever any node-relevant value or
  supported schema generation changes; create and publish remain blocked until
  every eligible Ready node reports the exact expected generation;
- N/N-1 mixed-version fixtures prove that a new controller never writes state
  an old node cannot read, while an incompatible migration remains impossible
  until the documented offline upgrade is used;
- fixed node parent-mount and kubelet roots reject equality, nesting, symlink
  aliasing, and overlap with plugin sockets, registration, pod, stage, or
  publish trees;
- a commercial Instance type outside the release-tested allowlist is rejected
  even if it advertises a positive `MaxFileSystems`, and an allowlisted type is
  still rejected when the live capability is absent or zero;
- `NodeGetInfo` omits `MaxVolumesPerNode` in v1;
- `GET_VOLUME_STATS` is not advertised in v1;
- block volume rejection.

### 12.3 CSI Sanity Tests

Use Kubernetes CSI sanity tests for the driver.

The sanity suite must run in CI through the production Create, Delete,
Publish/Unpublish, Validate, Stage/Unstage, and Node Publish/Unpublish state
machines, with only their Kubernetes, Scaleway, filesystem, and mounter
boundaries replaced by deterministic fakes. A separate in-memory CSI service is
not a conformance proof. Per-RPC counters must prove that each implemented core
was reached. The repository must pin the exact `kubernetes-csi/csi-test`
module version in `go.mod`; CI and release evidence must print that version.
Updating it is a deliberate compatibility change with a reviewed test result,
not an unbounded `latest` dependency.

The pinned v5.4 suite omits CreateVolume's returned context from some otherwise
valid publish fixtures. The harness may replay only the exact context returned
by the production Create core, only for the fixture's exact known NodeGetInfo
identity. It must pass unknown-node probes and every malformed request through
unchanged, so their read-only `NotFound` and `InvalidArgument` behavior is
proved by the product rather than manufactured by the harness. No provider,
filesystem, or durable-state result may be injected.

Run sanity separately against the controller and node Unix sockets with the
service set appropriate to each endpoint. The test harness must fail if the
Controller suite is skipped or reports zero Controller RPC cases; CI output and
release evidence must record the executed test names/counts. Add direct
Identity tests on both sockets because a green Node-only sanity run is not proof
of the complete controller contract.

### 12.4 Helm Tests

Required checks:

- `helm lint`;
- render with one parent;
- render with two parents;
- render rejects `controller.replicas > 1` in v1;
- render uses `strategy: Recreate` for the controller;
- render includes controller leadership Lease RBAC, grants no Lease delete, and
  constrains it to fixed resource name
  `scaleway-sfs-subdir-csi-controller`, exposes no Lease-name override, and does
  not render a mutable runtime Lease object;
- render grants get only on the fixed immutable operator approval Secret by
  resource name, grants no Secret list/watch/write/delete, and does not render
  or own that Secret;
- render grants the same exact get-only access to the fixed immutable
  `sfs-subdir-checkpoint` Secret and never renders, owns, lists, watches, or
  writes either operator-owned Secret;
- render grants the controller get/list/watch, but no write, for Pods in the
  dedicated driver namespace and grants no cross-namespace Pod access;
- render pins `leaseDuration`, `renewDeadline`, and `retryPeriod` with valid
  ordering;
- render rejects narrowed production node placement and disabled homogeneous
  preflight;
- render with `allowVolumeExpansion: false`;
- render with custom driver name;
- render with existing credentials Secret;
- render with existing installation identity Secret;
- verify Scaleway credentials are mounted only into the controller;
- verify project, region, and default zone come only from Helm values;
- verify the node plugin does not receive Scaleway credentials;
- render with dedicated namespace;
- render with controller and node security contexts;
- render with `external-provisioner --extra-create-metadata=true`;
- render the same non-empty `nodeConfigGeneration` annotation on the controller
  and node Pods and prove that every node-relevant value, including the
  immutable driver image digest, changes it deterministically while
  controller-only values do not;
- render passes the configured 12-minute `--timeout` to external-provisioner
  and external-attacher and rejects a timeout that cannot cover attachment;
- render passes explicit `--worker-threads=5` to external-provisioner and
  external-attacher and rejects values above the tested mutation envelope;
- render sets a 90-second shutdown deadline, 120-second pod termination grace,
  and 3900-second Deployment progress deadline, and rejects insufficient
  shutdown or startup margins;
- production render uses `priorityClassName: system-cluster-critical` for the
  singleton controller and does not pin it to one hostname; all controller
  candidates may nevertheless share one zone;
- render with the `CSIDriver` object;
- production render uses `repository@sha256:<digest>` for the controller and
  every sidecar, retains version tags as metadata, and rejects tag-only or
  malformed-digest release values;
- render explicit requests and limits for every container;
- validate the version-matched upstream external-provisioner and
  external-attacher RBAC union;
- verify controller RBAC can get only the `kube-system` Namespace for cluster
  identity and does not gain broad Namespace write access;
- negative `values.schema.json` and template tests for every cross-field rule
  listed in section 7.4;
- render with explicit controller/node security contexts, hostPaths, socket
  paths, and mount propagation;
- render uses the fixed production node parent-mount root and rejects any
  configured parent/kubelet/socket/registration/pod/stage/publish path equality
  or lexical nesting;
- chart-install startup preflight rejects normalized or symlink-resolved path
  overlap before enabling mount propagation;
- verify the controller parent root is `emptyDir`, the controller has no kubelet
  hostPath, and only node mount roots use required `Bidirectional` propagation;
- verify each privileged driver container receives a dedicated private
  `emptyDir` at `/run/scaleway-sfs-subdir-csi-mount-quarantine`, as a sibling
  of the strict `0700` admin-socket directory, with no mount propagation and no
  sidecar mount;
- verify controller and node startup/readiness use their own CSI livenessprobe
  `/healthz`, liveness uses the respective driver's separate `/livez`, and none
  of these endpoints is exposed by a Service;
- verify no credentials are rendered into ConfigMaps;
- install preflight rejects a namespace that does not enforce the documented
  privileged Pod Security level and accepts the explicitly labelled dedicated
  namespace.

### 12.5 Local Integration Tests

Use `kind` or a local Kubernetes cluster for API-level tests that do not require
real Scaleway mounts.

The required fake endpoint is a separate development-only binary and image,
not a runtime flag or dependency linked into the released driver. Helm may
select it only when `release.mode=development`; production rendering must reject
the switch. The fake node uses real bind mounts below the disposable kind
kubelet/driver hostPaths so chart-install evidence covers socket permissions,
registrar access, mount propagation, and kubelet lifecycle. It must not call
Scaleway, claim provider fidelity, or appear in a release image.

Validate:

- StorageClass creation;
- PVC provisioning and deletion through the required fake driver mode;
- controller and node restart with existing PVCs;
- a normal `Recreate` rollout consumes the exact graceful-release marker, while
  a new empty Lease and a failed release write remain non-serving;
- a rollout during an active fake mutation cancels work at a durable boundary;
  it emits a graceful marker only after the mutator exits, while a forced
  shutdown deadline produces no marker and requires abnormal recovery;
- namespace metadata backup accepts only a quiesced, hash-complete checkpoint;
  restore reconciliation with matching fake parent ownership records succeeds,
  while an in-progress or stale checkpoint remains non-serving;
- concurrent fake Create/Delete/Publish/Unpublish/GC requests cannot cross the
  checkpoint quiesce barrier; the exported object resourceVersions and parent
  digests come from the same quiesced interval;
- same-cluster absent-or-empty-Lease restore remains non-serving until the
  checkpoint-bound one-time immutable approval Secret is consumed and all
  pre-recovery Instances are stopped or deleted with fresh attachment
  inventories; fencing only the checkpoint's historical holder is rejected;
- a different fake `activeClusterUID` remains non-serving and cannot mutate
  parent state;
- provider unavailability during cold startup removes readiness without failing
  shallow liveness or restarting the controller; after startup, a failure
  conclusively scoped to one parent blocks that parent but not unrelated
  parents or healthy node-plugin readiness;
- RBAC proves the controller can discover labeled node-plugin Pods only in its
  own namespace and cannot create, update, patch, or delete Pods;
- external-attacher saved-node-ID fallback after Node/CSINode loss cannot clear
  the durable node fence without provider fencing, including the empty-node-ID
  all-node request;
- 100 concurrent fake Controller mutations remain bounded by the configured
  process-wide semaphore and cancelled queued calls do not enter provider or
  filesystem code;
- checkpoint restore detects an extra or missing compact ownership tombstone on
  a configured parent through its inventory digest, while a valid historical
  allocation tombstone for an offline-decommissioned parent is verified only
  through the Kubernetes-object aggregate and causes no remount;
- a staggered node DaemonSet rollout blocks create/publish while any eligible
  Ready node reports the previous configuration generation, then resumes only
  after every node reports the expected generation;
- an N/N-1 chart upgrade keeps existing mounts and old-node reads compatible,
  and prevents new-schema writes until the node rollout is complete;
- the safe-uninstall workflow rejects live PVs, attachments, fences, stages, or
  targets; after normal Kubernetes cleanup it quiesces the controller,
  unmounts node roots, stops the node DaemonSet, unmounts and detaches controller
  parents, gracefully stops the controller while RBAC still exists, and permits
  a later Helm uninstall without touching claims, tombstones, parent data, or
  identity Secrets;
- every mutating admin operation rejects an incompatible CLI/admin protocol,
  while the checksum-verified packaged `csi-admin` artifact completes
  checkpoint, GC, upgrade preflight, and safe uninstall;
- the chart installs only in the dedicated namespace after the documented
  privileged Pod Security labels are applied;
- sidecar wiring.

This chart-install integration test is mandatory in CI. Template rendering and
direct CSI sanity alone do not prove socket, ServiceAccount, RBAC, and sidecar
wiring.

The runner arms exact-cluster cleanup before invoking `kind create`, because a
late kubeconfig failure can occur after the node container already exists. It
also executes the packaged `csi-admin version` from the driver image, proves
the controller and node template configuration generations agree, and uses
Kubernetes authorization review to prove the controller ServiceAccount cannot
create, update, patch, or delete Pods.

### 12.6 Scaleway E2E Tests

Provide e2e scripts that can run against a real Scaleway project.

The e2e suite must be explicit, destructive only inside tagged test resources,
and easy to clean up.

The `base` profile is the first controlled real-provider smoke and is not
release qualification. It uses one run-owned ephemeral Kapsule cluster,
exactly two fresh nodes, two run-owned 25 GB parents, and the exact candidate
artifacts. Its closed scenario set must:

1. pass artifact and install preflight;
2. prove a real `virtiofs` mount;
3. prove one PVC is readable and writable from Pods on two distinct nodes;
4. bind exactly ten logical PVCs, write one unique marker through every logical
   mount, and fail if any new mount exposes another marker;
5. delete one of those PVCs with `archive`, thereby exercising the
   release-qualified `virtiofs` directory-rename path, observe the matching
   durable allocation in `Archived` with archive completion evidence, and
   re-read an untouched sibling marker;
6. force replacement of the singleton controller Pod, create another PVC, and
   re-read the existing cross-node volume;
7. retain provider attachment inventory for only the two planned parent IDs,
   require both parents to be `available`, require every attachment to be an
   `instance_server` in the planned zone, reject duplicate parent/Instance
   pairs, prove the cross-node mount spans exactly the two planned nodes, and
   bound physical attachments to `parents * nodes`;
8. remove workloads, run the complete structured safe-uninstall procedure, and
   delete every run-owned resource by exact retained ID.

Base evidence must contain `profile = base` and `releaseQualified = false`, use
a distinct smoke-evidence filename, and be rejected by every release
qualification decoder. Passing it authorizes only the next engineering step;
it does not satisfy the production matrix below.

The `release-candidate` profile is a bounded production qualification, not an
attempt to reproduce every deterministic crash permutation in a billable
cloud. It must run once against the exact frozen commit, chart, values,
`csi-admin`, driver image digest, and rendered sidecar digests. Its closed real
Kapsule matrix must:

1. create or reuse one dedicated Kapsule cluster in `fr-par`, create one fresh
   run-owned two-node pool and two fresh run-owned 100 GB parents, and record
   product status, quota, region, commercial Instance type, live
   `MaxFileSystems`, planned cost, and exact cleanup command;
2. validate the exact candidate artifacts, positive Project-scoped
   `FileStorageReadOnly` plus `InstancesFullAccess` access, the cluster File
   Storage tag, privileged namespace, production security contexts, immutable
   images, singleton controller Lease, compatible controller candidates, Ready
   node plugins, CSINode registrations, fixed disjoint host paths, and absence
   of Scaleway credentials from node containers;
3. prove a real controller `virtiofs` mount, `statfs`, immutable parent claim,
   archive rename compatibility, restart recovery, and one real
   archive/delete lifecycle that leaves an adjacent sibling sentinel unchanged;
4. create exactly 100 PVCs on the first parent, require all 100 to bind, mount
   at least live `MaxFileSystems + 5` logical PVCs concurrently on one node,
   prove one physical attachment for that parent and node, prove independent
   markers, and verify `NodeGetInfo` omits `MaxVolumesPerNode`;
5. run a bounded soak for at least 20 minutes over at least 10 sampled PVCs.
   Writers and cross-node readers must run concurrently, publish atomically
   replaced checksum-bearing records, reject every corrupt or partial record,
   retain positive read/write operation counts, and remain correct across one
   controller restart and one node-plugin restart;
6. prove cross-node RWX, read-only rejection, `SINGLE_NODE_WRITER` conflict and
   handoff, and `archive`, `retain`, and `delete` behavior without changing a
   sibling volume;
7. add the fresh second parent through Helm, interrupt the controller after the
   provider accepted its first attachment but before the immutable owner claim,
   and prove the same Pod/runtime identity resumes the exact durable bootstrap
   attempt and completes the claim without a temporary claim left behind;
8. attach both parents to the exact standalone run-owned disposable Instance
   outside the Kubernetes node inventory, prove the controller fails closed
   before partial provisioning, detach those exact attachments, prove both
   provider surfaces report absence, and verify provisioning resumes;
9. grow one run-owned parent by one 100 GB product step, wait for a fresh
   authoritative `available` observation, restart the controller to force a
   fresh provider inventory, and prove a new allocation can use the observed
   grown capacity. Transient, ambiguous, timeout, and unavailable provider
   reads are covered deterministically below rather than induced unsafely in
   the cloud;
10. drain and uncordon a workload node, restart a node plugin, then hard-stop
    the exact controller node while workloads on another node continue I/O.
    Prove the successor remains non-serving until exact provider fencing and
    one immutable approval, record recovery time and operator steps, replace
    the stopped run-owned node, and revalidate commercial type, live attach
    limit, node configuration generation, registration, mount, and data;
11. upgrade from the most recent public compatible chart candidate or release
    to the candidate under test while existing PVCs are mounted. Stagger the
    node rollout, require create/publish to fail closed while generations
    differ, and verify existing handles, records, all three deletion policies,
    new provisioning, and production rollout strategy after convergence. The
    predecessor and candidate use only the first parent during this proof; the
    immutable driver image digest supplies the version-specific node generation
    and the second parent remains fresh for item 7;
12. decommission the second parent only after removing every reference, prove
    no mount or attachment remains, preserve non-reserving permanent
    tombstones, remove only that parent from values, and restart without
    reattaching the historical parent;
13. create a quiesced checkpoint, remove and recreate the driver namespace in
    the same cluster while preserving the test PV and parents, replace every
    pre-recovery worker Instance, restore the immutable checkpoint Secret,
    require the missing-Lease controller to remain non-serving until exact
    all-Instance fencing and one-time approval, then verify existing data, new
    provisioning, archive/delete, and tombstone inventory;
14. verify the managed Scaleway File Storage CSI remains installed but idle and
    that its CSIDriver, StorageClass, RBAC, sidecars, and node DaemonSet coexist
    without default-class or object collisions;
15. remove every test workload and PVC through normal Kubernetes workflows,
    complete bounded manual GC for exact run-owned terminal allocations, run
    and validate `csi-admin uninstall prepare`, stop node plugins before the
    controller, prove all exact mounts and attachments absent, uninstall the
    exact Helm release, remove the retained kubeconfig, and delete every
    run-owned cloud resource by exact retained ID.

The soak is a correctness and recovery test, not a throughput benchmark. Its
20-minute minimum is measured after every sampled writer and reader is Ready.
Qualification evidence records duration, sampled PVC identities, aggregate
successful writes and reads, checksum failures, and the controller and
node-plugin Pod UIDs before and after their in-soak restarts. Zero successful
operations, any checksum failure, an early workload exit, or a restart without
a distinct Pod UID fails qualification.

During the new-controller/old-node interval in item 11, the exact controller
startup refusal naming both configuration generations together with a
non-Bound new PVC and a non-Ready new publish Pod is authoritative fail-closed
evidence. The test must not depend on CSI sidecar Event timing while the driver
socket is deliberately unavailable.

The following exhaustive permutations remain mandatory release gates, but run
deterministically on the same frozen source through fake Kubernetes/Scaleway
boundaries and privileged Linux mount/filesystem tests rather than as
timing-sensitive cloud experiments:

- cancellation, timeout, pagination, forbidden, stale, unavailable, ambiguous,
  `attaching`, `detaching`, `updating`, and unknown provider responses;
- interruption before and after every temporary-file fsync, no-replace rename,
  parent-directory fsync, allocation transition, ownership replacement,
  quarantine rename, GC remove marker, compaction, and checkpoint export;
- two isolated installations racing for one parent, mismatched installation or
  cluster identity, missing/mismatched ownership state, foreign or orphan
  attachments, and published-node fencing while an Instance remains live;
- descriptor/path replacement, symlink swap, traversal, bind aliases, stacked
  or nested mounts, reused mount IDs, and sibling-sentinel preservation;
- deleted-handle idempotency, unavailable-state retry, permanent tombstones,
  name-reuse rejection, offline rollback refusal while the recorded Instance
  is live, synthetic different-cluster restore, and all GC retry windows.

These deterministic tests do not weaken any runtime safety invariant. They are
the authoritative proof for controllable crash windows; the real Kapsule run is
the authoritative proof for Scaleway API, Kapsule scheduling, `virtiofs`,
physical attachment, and end-to-end operator behavior. Neither layer may be
skipped or substituted by the other.

### 12.7 E2E Cleanup Contract

Every e2e run must generate a unique run ID.

Every created Scaleway resource must include:

- a unique name prefix containing the run ID;
- a unique tag containing the run ID;
- the target project ID;
- the target region.

Before any real-cloud mutation, the runner must derive and retain a closed v1
preflight plan. The request names the base or release-candidate profile, exact
Project and `fr-par` region, UUID run ID, DNS resource prefix containing that
complete run ID, absolute evidence directory, aggregate positive hourly EUR
cost and bounded pricing source, exact Git commit and chart SHA-256, and the
canonical candidate-manifest SHA-256 plus the immutable digest reference of the
driver and every rendered sidecar. The candidate digest also binds the exact
release values, checksum manifest, native `csi-admin`, and commercial allowlist.
Every checksum-manifest entry must contain one lowercase SHA-256 and one plain
artifact basename. Path-prefixed, recursive, duplicate, or unsafe names fail
closed before provider mutation.
Candidate construction must also fail before writing a candidate manifest
unless the chart, release values, and checksum manifest are exact files in one
artifact directory and the checksum manifest covers both the chart and release
values. Every checksum entry is verified against that directory during
construction; qualification and real-E2E preflight repeat the same proof.
Live preflight treats the provider's typed Kapsule availability values
`available` and `scarce` as creatable because `scarce` explicitly means limited
availability. It fails closed for `shortage`, missing types, and unknown or
empty availability values before provider mutation.
Because the pinned APIs do not expose one stable endpoint for product maturity,
remaining File Storage quota, and pricing, the closed request also carries a
canonical UTC provider-review timestamp no older than 24 hours, the documented
GA product status and source, a positive remaining File Storage quota sufficient
for both parents and its source, and the pricing source used for the aggregate
cost. The legacy closed-schema `publicBetaAccepted` field must be `false`. This
operator review is not a substitute for live API checks: the executor still proves
regional File Storage access, the candidate commercial allowlist, current
Instance-type availability, and live `MaxFileSystems` sufficient for every
planned parent before mutation.
It chooses
either a new ephemeral cluster or one exact reused cluster ID, but it always
creates a fresh run-owned node pool of two or three nodes. Reusing or modifying
a pre-existing node pool is forbidden. Both profiles plan exactly two
run-owned parents. The base profile requires the current product-minimum 25 GB
size for each parent. The release-candidate profile requires 100 GB increments
from 100 GB through 49.9 TB so one 100 GB growth step remains below the 50 TB
per-filesystem maximum; it additionally plans exactly one standalone run-owned
disposable Instance of the same explicitly selected commercial type, reused
serially across its recovery scenarios so it cannot disappear from cost or
cleanup accounting.
Creating a cluster also plans exactly one run-owned Private Network in the same
Project and region. The executor creates and journals that network before the
cluster, passes its exact ID as the cluster's immutable `private_network_id`,
and verifies the cluster reports the same ID. A reused cluster keeps its
existing network; the run neither journals nor mutates that pre-existing
network.

`hack/scaleway-e2e-plan` is the repository's closed-schema, bounded-input
preflight renderer. Its canonical output always has `dryRun = true`,
`mutationAuthorized = false`, and `requiresImmediateApproval = true`. It has no
credentials, live-discovery, or execution backend; its output is review
evidence and never authorization. `hack/scaleway-e2e-run` is the separate live
executor. Dry-run is its default and does not construct a credentialed client.
Execution requires both `--execute` and an exact `--confirm-run-id` matching the
closed request immediately after the reviewed Project, region, resources,
hourly cost, destructive operations, and cleanup command are printed. It then
performs live regional File Storage, commercial-type, attach-limit, and
artifact validation and verifies that the operator product, quota, and pricing
review is explicit and no older than 24 hours. A previously rendered plan or
approval does not authorize a later run.
Before constructing a provider backend it also requires
`SCW_DEFAULT_PROJECT_ID` to equal the planned Project and a non-empty
`SCW_DEFAULT_ORGANIZATION_ID` for the provider CLI's read-only scope. These
values and credentials remain process-only and are not part of retained
evidence.

The executor has two non-interchangeable evidence paths. The base profile may
execute only the fixed smoke matrix defined in section 12.6 and emits explicitly
non-qualifying evidence after complete cleanup. The release-candidate profile
maintains an explicit closed development-interlock list for production
scenarios whose implementation does not yet prove the bounded matrix above.
The checked-in list is empty only after every current scenario has structured
semantic proof validation. While that list is non-empty,
release-candidate `--execute` fails before credentials, live reads, or provider
mutation, and no scenario log, base evidence, or zero exit status can be
encoded as release qualification. A scenario leaves that list only after its
structured evidence proves its complete normative invariant. N/N-1 requires a
distinct compatible public predecessor and is never reported as successful
when no previous chart was supplied. For qualification of the first stable
release, the most recent public release candidate may be that predecessor when
its exact chart, values, driver digest, and compatibility identity are retained
and explicitly labelled as a candidate rather than a prior production release.

Before the first provider mutation the executor fsyncs a provisioning-phase
inventory. Provisioning is sequential. Immediately before each provider
`Create`, the executor fsyncs the one exact resource kind and deterministic
name as `pendingCreate`; it clears that intent only in the same durable update
that records the successful response's exact ID, or after deterministic
discovery conclusively recovers that exact run-owned resource. An unresolved
intent blocks deletion and `complete`. Cleanup also discovers only the
deterministic exact names inside
the exact Project/region and requires the complete run ownership tag before
recovering an ID omitted by a crash between provider commit and ledger fsync.
Discovery is recovery input only: deletion is always by the recovered exact ID
after a fresh identity read. A name collision, duplicate, ambiguous read,
wrong tag, wrong Project, or wrong region blocks cleanup. A failed run is
resumed with `--cleanup-only` and the same exact confirmation; a second full
execution is refused while a retained inventory exists. `--cleanup-only`
requires that retained fsynced inventory and never recreates an authorizing
seed when it is missing. A missing ledger after possible provider mutation is
corruption and requires operator recovery, not inferred absence.

The retained cleanup inventory is a closed exact-ID creation ledger. A ready
scenario run contains exactly one cluster, one newly created node pool, and two
created parents; a run-created cluster also requires its one created Private
Network, and the release-candidate profile additionally contains its one
disposable Instance. A provisioning failure may leave any valid prefix of that
planned set. Cleanup and complete phases retain exactly the resources that were
created
or conclusively rediscovered for that run and never require invented IDs for
resources that were never created. Deterministic discovery checks every planned
name before cleanup can accept that partial ledger. After any provisioning
error, the executor requires a bounded sequence of stable, successful discovery
reads of the complete canonical `kind/name/ID/state` set; equal cardinality is
not stability. These reads may recover a committed intent, but repeated absence
does not clear an unresolved provider `Create` without an authoritative
provider conclusion. One empty list can never turn an ambiguous provider
Create into `complete`. A timeout, changing result, or unresolved intent retains
the provisioning ledger and requires `--cleanup-only` retry. The cluster entry records
whether it was created or reused; every other entry must be run-created. Each
entry records exact ID, name, Project,
region, tags, creation provenance, and the closed observed state `present`,
`absent`, or `unknown`. Created entries must match the complete run prefix and
ownership tag. A conclusive exact-ID provider lookup is required for `absent`;
forbidden, timed-out, partial, failed, or contradictory reads are `unknown`.
Inventory observations older than ten minutes or more than one minute in the
future block deletion review.

`hack/scaleway-e2e-cleanup --dry-run` validates that ledger and emits canonical,
non-authorizing review evidence. It lists no delete action if any observation is
stale/unknown or any required Kubernetes cleanup, PV/VolumeAttachment removal,
unpublish/unstage, published-fence clearing, safe-uninstall, node/controller
stop, mount absence, attachment absence, or post-prepare Helm uninstall barrier
is incomplete. When unblocked it identifies only exact run-owned IDs, ordered
as disposable Instance, node pool, parents, a run-owned cluster, then that
cluster's run-owned Private Network. It never selects a reused cluster or its
pre-existing network. Conclusively absent resources are idempotent success
evidence. That review command deliberately has no deletion backend.
`hack/scaleway-e2e-run --cleanup-only` is the distinct credentialed executor;
after a new immediate approval it repeats discovery, exact-ID reads and every
Kubernetes/unmount/detach barrier before each ordered deletion. It requires the
exact run ID in every mode, distinguishes a conclusively absent Helm release
from Helm/Kubernetes errors, and derives its retained preconditions from a
completed structured `csi-admin` safe-uninstall audit plus fresh exact
inventories. It never writes a conventional all-success object after an
inconclusive command. The safe-uninstall request ID is the exact run ID, its
result filename is run-scoped, and the executor decodes and validates the full
typed plan and audit before Helm removal or provider deletion. Failure to
remove the retained kubeconfig is a cleanup error and cannot yield successful
qualification evidence. Presence of the
runner does not constitute release evidence: the exact candidate still needs a
successful retained Kapsule run and a final complete inventory.

If the first install scenario fails before producing any successful scenario
entry and the exact Helm release is either `failed` or conclusively absent
because the pre-Helm installation gate failed, cleanup may use the narrower
bootstrap-abort path instead of claiming a safe uninstall, but only when the
cluster itself was created by that exact run. Reused clusters always require
the normal safe-uninstall or operator recovery path. The fallback must prove the
dedicated namespace has the exact run label; no workload Pod, PVC, namespace
PV, driver VolumeAttachment, driver CSINode registration, or durable driver
record exists; and both exact run-owned parents have zero provider attachments.
The parent check must agree on both the filtered attachment list and each exact
filesystem's reported attachment count. Workload/PVC absence is captured before
normal cleanup removes anything, so the fallback cannot manufacture absence.
The conclusively absent pre-Helm case additionally requires the retained,
non-empty first-scenario failure log.
Only then may it uninstall that exact failed release when present, or preserve
its conclusively absent state, delete that exact namespace, retain run-bound
bootstrap-abort evidence including the observed `failed` or `absent` Helm
status, and satisfy the cleanup barrier through `bootstrapAbortComplete`. Any
successful scenario entry, deployed or ambiguous release, missing evidence
file, ambiguous read, record, registration, volume object, or provider
attachment keeps cleanup blocked and requires the normal safe-uninstall or
operator recovery path.

Cleanup must:

- require explicit project ID, region, run ID, and non-empty name prefix;
- support dry-run;
- refuse broad selectors;
- delete run-owned workload Pods and PVCs through normal Kubernetes workflows,
  wait for PV deletion, VolumeAttachment removal, unpublish, and unstage, then
  inventory every remaining terminal allocation through the exact run-labelled
  namespace and installation identity. Before safe uninstall it must use the
  normal request-bound `csi-admin gc submit` dry-run and execute workflow to
  collect only those exact run-owned `Archived` or `Retained` volumes, retain
  each audit, and require every allocation to be a permanent non-reserving
  `Deleted` tombstone. The cleanup plan is persisted before execution. A retry
  reuses its planned execute request ID. If an earlier read-only dry-run
  already persisted a valid bounded request envelope for the same exact
  terminal state without lifecycle progress, the plan adopts and audits that
  dry-run request ID instead of submitting a conflicting identity. An execute
  request or lifecycle progress without the matching persisted plan is never
  adopted. A foreign, malformed, non-terminal, unlabelled, or out-of-parent
  allocation blocks cleanup; then run and verify
  `csi-admin uninstall prepare` before deleting Helm-managed RBAC, controller,
  or node resources;
- never delete reused or pre-existing clusters, Private Networks, or
  filesystems unless they were created by the same run ID;
- print an audit summary before deletion;
- be idempotent and runnable separately after failed tests.

### 12.8 CI and release execution boundary

The repository intentionally contains one GitHub Actions workflow,
`.github/workflows/ci.yaml`. It is a local verification boundary only. It must
not receive Scaleway credentials, call Scaleway APIs, provision or mutate a
cloud resource, publish an image or chart, create a GitHub Release, or promote
a release tag. Pull requests and pushes therefore cannot incur Scaleway cost or
perform provider-side cleanup.

The CI workflow must run:

- `go test ./...`;
- `go test -race ./...` on Linux;
- `go vet ./...`;
- `gofmt` check;
- `golangci-lint`;
- CSI sanity tests;
- privileged Linux mount-namespace tests that exercise the real path-safety,
  symlink-swap, nested-mount, exact staging-source proof, stacked-mount
  rejection, publish/unpublish locking, mountinfo-ID reuse after replacement,
  `STATX_MNT_ID_UNIQUE` comparison, exact unmount, and CO-owned staging-directory
  behavior without replacing those checks with a fake mounter;
- privileged Linux tests for internal symlink aliases, descriptor/path
  replacement, unknown descendant mounts, interrupted-quarantine retry, and
  quarantine-empty proof before provider detach;
- privileged Linux filesystem-crash tests that fail each required file and
  parent-directory durability barrier and prove restart observes either the old
  complete generation or the new complete generation, never guessed success;
- Helm lint and template checks;
- required kind chart-install test with fake provider and mounter;
- supported-envelope reconciliation and concurrency tests;
- packaged `csi-admin` protocol/version compatibility tests;
- Docker build.

Release preparation is an explicit operator procedure in v1, not a second
GitHub workflow graph. Before publication, the operator must run the checked-in
`test-release-binaries`, `test-release-manifest`, Linux, kind, CSI, Helm, and
real-Kapsule gates against one frozen commit and one exact set of image digests.
The procedure must verify that the supported Kubernetes/Kapsule, Go, CSI
module, pinned `csi-test`, Scaleway SDK, commercial Instance type, and sidecar
matrix has matching evidence. Untested tuples must not be advertised.

The manual release procedure must produce:

- versioned container images in the configured public registry;
- versioned Helm charts in the configured public chart repository;
- versioned `csi-admin` static binaries for documented operator platforms;
- SHA-256 checksums, SBOMs, and honest build provenance for release artifacts.

Candidate construction and qualification must still bind the commit, chart,
driver image, sidecar images, binaries, commercial allowlist, Linux results,
kind results, Kapsule results, and final cleanup evidence. Publication uses the
already-qualified bytes without a rebuild and refuses existing image tags,
chart versions, or release assets. The checked-in local tools may generate and
verify these manifests, but they do not publish them automatically and must not
claim a GitHub workflow builder identity.

Real Scaleway E2E is exclusively an operator-controlled command in v1. No
GitHub workflow may invoke the credentialed executor. The dry-run plan is
reviewed first, then a new immediate approval is required before the operator
runs `hack/scaleway-e2e-run --execute` from a dedicated non-production test
environment. This keeps the cloud safety boundary visible and avoids a second
CI control plane while preserving the required real-provider evidence.
Provider credentials used by that environment must remain in process memory or
on a root-only volatile filesystem; they must never be copied into the retained
evidence directory or another persistent runner path. The scenario runner must
remove the credentials from the inherited child-process environment, expose
them only to exact provider CLI calls, and stream the controller-only Kubernetes
Secret without putting plaintext or its rendered manifest in a file or process
argument. The nested install preflight must repeat that boundary: `kubectl` and
`jq` receive no Scaleway credentials, while only its exact read-only `scw`
cluster-identity call receives them. Success and failure logs must not contain
credential values.

A v1 release candidate must record a successful real Kapsule E2E result for the
exact Git commit, chart package, driver image digest, and every CSI sidecar image
digest being released. The result and cleanup audit must be retained with the
release evidence. No pull request, push, tag, or GitHub release action runs this
cloud test automatically.

Both qualification creation and final verification independently compare every
Kapsule evidence artifact to that exact candidate commit, candidate-manifest
digest, chart digest, and canonical five-image set. File checksums alone are not
a semantic candidate binding and a hand-crafted qualification manifest cannot
bypass this comparison.

Qualification validates the embedded final inventory itself and requires the
separately retained cleanup audit to be byte-equivalent after canonical
encoding. Run ID, Project, region, profile, resource prefix, ownership tag,
resource IDs, states, preconditions, and observation time must all match; a
complete cleanup from another run can never authorize publication.
Successful production qualification requires the `release-candidate` profile
and its complete historical inventory: one cluster entry, one run-owned node
pool, two run-owned parents, and one run-owned disposable Instance. Every
run-owned resource is conclusively `absent`; only an explicitly reused cluster
may remain conclusively `present`. Partial ledgers remain valid cleanup evidence
for failed provisioning, but can never be encoded as a successful release run.

## 13. Documentation Requirements

### 13.1 README

The README must include:

- what the driver does;
- what problem it solves;
- architecture diagram;
- installation prerequisites;
- Scaleway IAM requirements;
- parent File Storage pool creation commands;
- Helm installation;
- StorageClass examples;
- PVC examples;
- deletion policies;
- resizing behavior;
- limitations;
- troubleshooting;
- production checklist;
- compatibility with Scaleway Kapsule and the official Scaleway CSI driver;
- exact `FileStorageReadOnly` and `InstancesFullAccess` Project-scoped IAM
  requirement and the current permission-granularity limitation;
- current Scaleway File Storage status and qualified Instance families;
- exact release-tested commercial Instance type allowlist and the requirement
  for a positive live `MaxFileSystems` capability;
- supported v1 scale envelope;
- quiesced metadata checkpoint, fixed externally owned immutable checkpoint
  Secret, and offline all-Instance fencing required for same-cluster recovery;
- empty dedicated-parent bootstrap requirement and the absence of existing-data
  adoption in v1;
- atomic root claim path and interrupted-bootstrap recovery boundary;
- production image-digest pinning, controller mutation limit, and graceful
  shutdown timing;
- dedicated privileged Pod Security namespace, fixed disjoint host paths, safe
  uninstall command, and node configuration-generation rollout gate;
- supported `SINGLE_NODE_WRITER` and `MULTI_NODE_MULTI_WRITER` access modes;
- driver name stability and upgrade implications;
- license and attribution.

### 13.2 Limitations

The README must clearly state:

- not an official Scaleway product;
- requires Kubernetes nodes that can attach/mount Scaleway File Storage;
- requires a Linux kernel exposing `statx(STATX_MNT_ID_UNIQUE)` (normally Linux
  6.8 or a distribution kernel with the primitive backported); reusable
  mountinfo IDs alone are never sufficient to authorize unmount; the kernel and
  runtime profile must also support `open_tree`, `move_mount`, `mount_setattr`,
  procfs mount-FD identity, and exact detach as proven by startup preflight;
- designed primarily for Scaleway Kapsule/Instances;
- requires a stable CSI driver name before any real workload deployment;
- may coexist with an installed but idle official Scaleway File Storage CSI
  driver, but active official or manual File Storage attachments on this
  driver's workload nodes are unsupported in v1;
- Kubernetes and Scaleway administrators are trusted not to import a configured
  parent as a raw volume through another CSI driver or mount it manually; such
  administrative access can bypass logical-volume isolation by design;
- uses a small pool of parent File Storage file systems;
- one parent is permanently claimed by one driver installation in v1;
- each claim is bound to one Kubernetes `activeClusterUID`; v1 supports
  same-cluster namespace recovery from a quiesced checkpoint but not arbitrary
  online-backup consistency or cross-cluster takeover;
- same-cluster missing-Lease recovery is an offline operation that requires all
  pre-recovery cluster/controller Instances to be stopped or deleted and fresh
  regional inventories to be clean; the historical checkpoint holder alone is
  not a sufficient fence;
- all schedulable Linux workload nodes must be compatible; narrow topology is a
  future feature;
- v1 runs one controller replica and intentionally has no automatic abnormal
  failover; takeover after an uncleared holder requires external confirmation
  and explicit operator approval;
- normal controller replacement is automatic only after the previous holder
  writes the exact graceful-release marker; a failed release remains an
  abnormal takeover;
- deleting, archiving, or garbage-collecting a volume remains blocked until all
  persisted published nodes are normally unpublished or provider-fenced;
- a `CreateVolumeRequest.name` is never reused within one installation;
- one parent performance envelope is shared by all PVCs placed on that parent;
- PVC size is a logical reservation in v1, not a hard filesystem quota;
- a workload can fill a shared parent at runtime; operators must monitor parent
  free space and isolate pools for untrusted tenants;
- per-PVC `NodeGetVolumeStats` is not exposed in v1 because logical PVCs are
  subdirectories without hard quotas;
- logical PVC expansion is not supported in v1;
- Scaleway parent shrink is unsupported; an unexpected observed decrease enters
  `critical-size-regression` and accepts no new placement until a fresh
  authoritative observation reaches the previous accepted size;
- pool parent count must respect node-level File Storage attach limits;
- online parent removal and live-node stale detach are unsupported in v1;
- CSI mount flags and topology requirements are unsupported, and filesystem
  type is limited to empty or `virtiofs`;
- access modes other than `SINGLE_NODE_WRITER` and
  `MULTI_NODE_MULTI_WRITER`, including reader-only capability modes, are
  unsupported in v1; pod-level read-only publishing remains supported;
- parent file systems are not automatically created or deleted in v1;
- an unclaimed parent must be dedicated and empty; adopting existing data or an
  existing directory tree is not supported in v1;

The README must state that `archive` protects against accidental PVC deletion
but is not an independent backup: archived data remains on the same parent File
Storage failure domain. User-data backup and restore remain operator/platform
responsibilities outside this CSI driver's v1 scope.

### 13.3 Operations Guide

The operations guide must document:

- why v1 runs one controller replica and how future HA must be introduced;
- the exclusive ownership rule for each parent filesystem;
- parent-global owner record and `installationID` bootstrap, backup, restore,
  initial claim, uninstall, and reinstall behavior;
- durable bootstrap-attempt inspection, exact resume, and exact rollback after
  interruption during the first parent claim;
- root temporary-claim inspection and crash-safe no-replace installation of
  `/.sfs-subdir-csi-owner.json`;
- controller-local checkpoint prepare/resume, the immutable checkpoint Secret,
  explicit RPO, same-cluster namespace recovery, stale-checkpoint failure,
  all-pre-recovery-Instance fencing, missing-Lease approval, and the explicit
  absence of cross-cluster recovery in v1;
- controller leadership, graceful-release marker lifecycle, abnormal takeover
  approval, and safe upgrade behavior;
- exact creation, expiry, consumption audit, and post-success deletion of the
  immutable `sfs-subdir-controller-approval` Secret; the controller never
  creates or modifies that Secret;
- exact creation, digest verification, restore use, and post-success deletion
  of the immutable `sfs-subdir-checkpoint` Secret; Helm and the controller never
  create or modify it;
- shutdown behavior, including immediate unready state, bounded cancellation,
  the no-marker abnormal path, controller-node failure recovery, measured RTO,
  and the required pod grace/progress margins;
- homogeneous all-workload-node scheduling contract and the minimum two Ready
  controller candidates;
- adding a new parent file system to the pool;
- growing a parent file system and observing its complete provider-status
  transition;
- diagnosing and recovering a defensive `critical-size-regression` condition
  without disrupting existing mounts;
- moving a parent file system from `active` to `draining`;
- keeping a draining parent configured during normal operation and performing
  the offline unmount/detach procedure before exceptional removal;
- recovering a stale published node by stopping and detaching or deleting the
  corresponding Scaleway Instance before retrying deletion;
- the closed Scaleway Instance state matrix and the support-escalation path for
  a regional attachment that outlives a deleted Instance;
- garbage-collecting archived or retained data;
- compacting old `Deleted` ConfigMaps in place after the configured retention
  window without deleting the permanent tombstone;
- recovering from a failed node mount;
- recovering from accidental PVC deletion when `archive` is enabled;
- recovering from missing or mismatched driver-owned ownership records;
- rotating Scaleway credentials;
- upgrading the Helm release with the preflight command, N/N-1 mixed-version
  rule, node configuration-generation convergence gate, and offline procedure
  for incompatible migrations;
- running `csi-admin uninstall prepare`, verifying its audit result, and only
  then running `helm uninstall`;
- downloading and checksum-verifying the version-compatible `csi-admin`
  release binary;
- preparing the dedicated namespace Pod Security labels and verifying fixed,
  disjoint node parent and kubelet host paths;
- checking pool capacity;
- integrating the opt-in sample alerts for controller readiness/leadership,
  stale reconciliation/inventory, unknown attachments, fences, node coverage,
  generation mismatch, mount errors, parent status, size regression, and free
  space.

### 13.4 Troubleshooting Guide

Include common failure modes:

- PVC stuck Pending;
- parent File Storage not found;
- Scaleway API permission denied;
- attach limit reached;
- mount failed;
- directory permission mismatch;
- pool full;
- actual parent free space below reserve;
- parent metadata refresh failed;
- parent File Storage status is transient, `error`, unknown, or unreadable;
- driver-owned ownership record missing or mismatched;
- incompatible node or parent attach limit reached;
- storage-eligible node set became invalid after node churn;
- active official-CSI or manual File Storage attachment consumes a required
  physical slot;
- regional attachment inventory contains an unknown or orphaned Instance;
- parent claim `activeClusterUID` differs from the current cluster;
- first parent claim is blocked by a missing, mismatched, or ambiguous
  bootstrap attempt;
- root parent claim installation is blocked by a foreign temporary claim or a
  filesystem that cannot provide the required crash-safe no-replace rename;
- graceful controller release marker is missing, invalid, or already consumed;
- controller waits for an abnormal takeover approval after an uncleared holder;
- controller waits for missing-Lease recovery approval after namespace restore;
- missing-Lease recovery is blocked because an old or unknown cluster Instance
  or parent attachment is still visible;
- restored checkpoint is incomplete, in progress, or older than parent
  ownership state;
- restored checkpoint parent ownership count or digest differs because a
  detailed or compact tombstone is missing, extra, or changed;
- deletion is blocked by a persisted published node that has not been
  provider-fenced;
- CSI sidecar timeout is shorter than the attachment readiness deadline;
- an eligible node reports an old or mismatched configuration generation;
- fewer than two Ready compatible controller candidates are available;
- parent size observation regressed below the previous accepted value;
- the node's commercial Instance type is not release-qualified or its live
  `MaxFileSystems` capability is zero or absent;
- node parent-mount and kubelet paths overlap or resolve through a symlink into
  a protected kubelet/plugin tree;
- the dedicated namespace does not enforce the required privileged Pod
  Security level;
- read-only mount requested but target is writable;
- `NodePublishVolume` target already exists with a different source,
  capability, filesystem type, or read-only mode;
- controller termination deadline elapsed without a graceful-release marker;
- safe uninstall is blocked by a live PV, attachment, published-node fence,
  stage, target, parent mount, or provider attachment;
- `csi-admin` protocol version is incompatible with the running driver;
- release values contain a tag-only or malformed image reference;
- node reschedule issue.

## 14. Release Plan

### 14.1 v0.1.0

Development preview.

Required:

- basic CSI Identity/Controller/Node;
- one pool;
- one or more explicit parent filesystem IDs;
- normative Scaleway provider appendix implemented against the pinned SDK and
  validated against official driver behavior;
- dynamic PVC provisioning;
- archive delete policy;
- driver-owned ownership record;
- parent-global owner record with production identity Secret;
- allocation state machine;
- controller leadership coordination enabled and charted with Kubernetes Lease
  RBAC;
- Helm chart with immutable image digests, explicit security contexts, and
  `Recreate` controller rollout strategy;
- unit tests;
- fake CSI sanity tests;
- Helm template tests for the critical production defaults;
- public artifact identity decided, even if the release is marked preview;
- one controlled real Scaleway base smoke before publishing the preview using
  the closed section 12.6 contract: two nodes, two 25 GB parents, ten logical
  PVCs, cross-node RWX, isolation, archive, controller replacement, provider
  attachment inventory, structured safe uninstall, and exact cleanup of every
  tagged run-owned resource.

### 14.2 v0.2.0

Operational preview.

Required:

- multiple pools;
- metrics;
- better failure messages;
- e2e scripts;
- parent metadata refresh;
- attachment inventory and attach-budget preflight;
- logical capacity accounting;
- actual parent free-space metrics;
- active/draining parent lifecycle;
- safe path deletion tests, including symlink replacement and different-device
  entries;
- homogeneous all-workload-node scheduling contract documented and tested;
- documentation complete enough for external users;
- coexistence test profile with the official Scaleway File Storage CSI driver;
- node compatibility preflight tested on a real Kapsule node pool.

### 14.3 v1.0.0

Production-ready release.

Required:

- real Scaleway e2e validated;
- provider appendix validated, including IAM permission sets, Instance
  attachment inventory and polling, stale attachment cleanup, and product
  maturity assumptions;
- node drain/reschedule tested;
- multi-node RWX tested;
- parent upward resize and complete File Storage status matrix tested;
- archive/delete/retain tested on real mounted storage;
- manual GC tested through the active leader on real archived or
  retained data;
- driver-owned ownership record failure cases tested;
- parent-global owner record, `installationID`, duplicate-parent, and
  cross-installation claim failures tested;
- same-cluster `activeClusterUID`, missing-Lease recovery approval, and rejected
  cross-cluster identity tested;
- normal different-Pod handoff, failed graceful release, and preservation of
  live Lease fields across Helm operations tested;
- durable first-claim bootstrap resume and exact rollback tested;
- atomic fixed root claim tested across temporary-file fsync, no-replace rename,
  root fsync, stale-attempt, and competing-attempt crash points on real
  Scaleway `virtiofs`;
- two-installation same-Instance bootstrap isolation and live-Instance rollback
  refusal tested;
- the closed allocation/ownership transition table, exact predecessor repair,
  normal detailed-Deleted/compact-ownership pairing, and conflicting operation
  IDs tested at every dual-write crash point;
- CSI-compatible `CreateVolume` capacity and parameter replay tested;
- conclusive-absence versus unavailable `DeleteVolume` behavior tested;
- in-use `DeleteVolume` rejection tested with VolumeAttachments;
- normal, forced, and empty-node-ID unpublish semantics plus stale
  published-node fencing tested before delete, archive, and GC;
- conservative allocation/ownership union recovery tested for every
  publish/unpublish dual-write crash point;
- compact handle and mapping-hash compatibility tested;
- old allocation, ownership, parent-global owner, and PV compatibility fixtures
  tested;
- controller restart/reschedule tested;
- regional attachment inventory and unknown/orphaned Instance rejection tested;
- controller security context tested on real Kapsule nodes with `virtiofs`
  mount, statfs, create, archive, delete, and restart flows;
- in-place permanent Deleted tombstone compaction tested with later
  `DeleteVolume` retry and rejected name reuse;
- permanent compact ownership tombstones, allocation reconstruction, and final
  GC dual-write crash recovery tested;
- atomic metadata crash recovery and abnormal controller takeover tested;
- hard controller-node failure tested on real Kapsule: existing remote mounts
  continue, an unfenced successor remains blocked, approved recovery succeeds,
  and measured recovery time is retained;
- quiesced namespace metadata checkpoint, stale/in-progress rejection, and
  parent ownership inventory count/digest rejection tested;
- same-cluster namespace restore tested on real Kapsule with the exact release
  chart and every rendered image digest, including checkpoint-bound approval,
  all-pre-recovery-Instance offline fencing, and rejection of fencing only the
  historical checkpoint holder;
- explicit provisioner/attacher timeout and split readiness/liveness behavior
  tested;
- explicit sidecar worker counts and the process-wide ten-mutation limit tested
  under cancellation and burst load;
- graceful shutdown deadline, pod termination grace, Deployment progress
  deadline, and no-marker abnormal path tested;
- exact Node publish target creation, conflict, rollback, and unpublish cleanup
  behavior tested;
- CO-owned Node stage directory behavior, supported access modes, immutable
  context validation, and RPC-specific CSI status semantics tested against the
  pinned CSI conformance suite;
- CSI `volume_context` 128-byte and 4-KiB boundaries tested;
- namespace-scoped node-plugin Pod discovery RBAC tested without Pod writes or
  cross-namespace reads;
- supported scale envelope and default controller resource limits validated;
- `go test -race ./...` and privileged real Linux mount-namespace path-safety
  tests pass;
- real Node mount-source/stacking checks and filesystem durability-barrier
  failure tests pass;
- every advertised commercial Instance type has retained real-E2E evidence and
  a positive live `MaxFileSystems`; unqualified types remain rejected;
- real parent growth and fake-provider size-regression behavior tested;
- fixed disjoint host paths and dedicated privileged Pod Security namespace
  preflight tested on real Kapsule;
- no-hard-quota runtime risk documented with free-space alerts;
- credential rotation documented;
- upgrade preflight, N/N-1 mixed-version compatibility, node configuration-
  generation convergence, and offline incompatible migration documented and
  tested;
- safe uninstall preflight, exact unmount/detach, Helm removal, and reinstall
  behavior documented and tested;
- checksum-verified versioned `csi-admin` binaries, SBOM, provenance, and admin
  protocol compatibility are published and used by release tests;
- deletion safety audited;
- no known data loss bugs;
- no credentials in rendered manifests;
- every production driver and sidecar image is rendered by immutable digest;
- more logical PVCs than the physical `MaxFileSystems` value are concurrently
  mounted on one node through exactly one parent attachment;
- stable driver name;
- stable Helm values contract;
- GA provider maturity gate passed for the exact supported region.

## 15. Acceptance Criteria

The development is complete only when all criteria below are satisfied.

### 15.1 Functional

- A user can install the driver with Helm.
- A user can configure one or more existing Scaleway File Storage parents.
- A user can create many RWX PVCs from one StorageClass.
- Each PVC maps to a unique subdirectory.
- Each logical volume has a valid driver-owned ownership record outside the
  user-mounted data directory.
- Multiple PVCs can share the same parent File Storage.
- One node can mount more distinct logical PVCs than its physical
  `MaxFileSystems` limit while using exactly one parent attachment.
- The same PVC can be mounted by multiple pods.
- The same PVC can be mounted across multiple nodes.
- Read-only mounts are published read-only.
- Deleting a PVC archives data by default.
- Parent resize is detected without reinstalling the driver.
- An unexpected parent-size regression becomes critical and receives no new
  placement while existing mounts remain untouched; the product never presents
  Scaleway shrink as supported.
- Existing PVCs continue to mount after driver pod restart.
- Existing PVCs continue to mount after node rescheduling.
- Logical PVC expansion is disabled in v1.
- The v1 controller runs as one replica and rejects accidental multi-replica
  configuration.
- Production preflight requires at least two Ready compatible controller
  candidate nodes and the chart does not pin the controller to one node.
- The controller holds its Kubernetes Lease before any mutating operation.
- The controller Lease uses the fixed non-configurable v1 name
  `scaleway-sfs-subdir-csi-controller`.
- The driver accepts only `SINGLE_NODE_WRITER` and
  `MULTI_NODE_MULTI_WRITER` in v1 and reports unsupported capabilities with the
  exact CSI response semantics.
- Every schedulable Linux workload node passes compatibility, attachment-budget,
  release-tested commercial-type, live `MaxFileSystems`, node-plugin readiness,
  CSINode registration, and configuration-generation preflight.
- Every configured parent has a complete regional attachment inventory that
  contains only Instances authorized for the active installation.
- Abnormal takeover by a new Pod UID requires an exact one-time immutable
  operator approval Secret after the previous process or Instance is confirmed
  stopped and provider attachment fencing succeeds.
- Normal replacement by a new Pod UID consumes a valid graceful-release marker
  exactly once and does not require operator approval.
- Same-cluster namespace recovery requires explicit approval when durable state
  exists but the Lease is missing, and automatic restore accepts only a
  complete quiesced checkpoint after every pre-recovery Instance is offline and
  fresh attachment inventories are clean; a different cluster UID remains
  blocked.
- The controller limits all mutations to the configured maximum of 10 and
  performs the bounded graceful-shutdown protocol during rollout.
- An operator can complete the audited safe-uninstall workflow before Helm
  removes RBAC or Pods, without deleting durable metadata or parent data.

### 15.2 Safety

- The driver never deletes outside the configured base path.
- The driver refuses destructive operations without a matching driver-owned
  ownership record.
- The driver refuses to operate when exclusive parent ownership cannot be
  proven.
- The driver refuses to operate when the parent-global owner record has a mismatched
  `installationID`.
- The driver refuses to mutate parents when `activeClusterUID` differs from the
  current cluster.
- The volume handle is compact, deterministic, bounded to 128 bytes or less,
  and validated against the persisted mapping hash.
- Every `volume_context` key/value and complete map respects the CSI 128-byte
  and 4-KiB limits before any persistent or filesystem mutation.
- Every immutable `volume_context` field is compared with authoritative state
  before controller attach/fencing or node filesystem work.
- `CreateVolume` is atomic and idempotent for repeated
  `CreateVolumeRequest.name` values.
- Compatible CSI capacity-range replays return the existing volume; incompatible
  immutable requests return `AlreadyExists`.
- A compatible `Ready` replay returns the existing mapping even when provider
  refresh, current placement, or new capacity is unavailable.
- `CreateVolume` does not reuse terminal tombstones as active mappings.
- `DeleteVolume` is crash-consistent across archive, delete, retain, and retry
  paths.
- A valid driver-owned `DeleteVolume` request with conclusively absent durable
  state returns success without guessing or touching the filesystem; unavailable
  lookups never masquerade as absence.
- `DeleteVolume` refuses to mutate a volume with a remaining VolumeAttachment.
- Delete, archive, and GC refuse to mutate a volume with an unresolved
  `publishedNodeIDs` entry.
- Forced or ambiguous unpublish never clears a node fence without conclusive
  normal-node evidence or provider fencing; an empty `node_id` safely evaluates
  every persisted node.
- Terminal allocation records remain durable after `DeleteVolume`.
- Old `Deleted` records may be compacted in place, but the permanent per-volume
  ConfigMap is never removed in v1.
- Matching compact ownership tombstones also remain permanently on their
  parent; GC and checkpoint restore preserve agreement between both proofs.
- Detailed, compact, and deleted-unknown tombstone schemas are unambiguous, and
  valid non-reserving tombstones do not prevent a safely decommissioned parent
  from remaining absent from configuration.
- Manual GC is explicit, idempotent, dry-run capable, and refuses unsafe paths.
- Mutating GC only runs through the active leader.
- GC returns success only after allocation and ownership records contain the
  same terminal operation outcome.
- Allocation and ownership records follow one closed transition table; only an
  exact one-step predecessor from the same operation may be repaired, while
  conflicting evidence remains non-serving.
- Parent-global and ownership metadata writes are atomic and crash-durable.
- Logical-directory creation, rename, removal, and bootstrap cleanup become
  complete only after their required filesystem durability barriers succeed.
- The first parent claim is crash-resumable only through its exact durable
  bootstrap attempt; unrelated attachments are never treated as owned or
  detached, and rollback never detaches from a live Instance.
- The fixed root parent claim is installed by a crash-safe no-replace operation;
  a partial, stale, or competing temporary claim is never adopted.
- The driver never logs secrets.
- Scaleway credentials are not mounted into the node plugin.
- Controller RBAC can read labeled node-plugin Pods only in the driver namespace
  and cannot mutate Pods.
- Controller RBAC has exact get-only access to the externally owned immutable
  approval and checkpoint Secrets and cannot create, modify, list, watch, or
  delete them.
- Controller and node-plugin probes expose independent cached readiness and
  shallow liveness contracts without circular dependencies.
- Invalid StorageClass parameters fail clearly.
- Unsupported filesystem types, mount flags, and topology requirements fail
  clearly and never change shared-parent mount options.
- A conflicting existing Node publish target returns `AlreadyExists` and is
  never remounted in place; target creation, rollback, and cleanup are bounded
  to the validated kubelet path.
- Node publish proves the exact staging/source/parent mount graph, and node
  unpublish/unstage never unmount a foreign, aliased, or stacked mount.
- Node stage validates but never creates or removes the CO-owned staging
  directory; node publish owns only its validated target directory.
- Fixed node parent and kubelet paths are disjoint after normalization and
  symlink resolution, and the chart requires the dedicated privileged Pod
  Security namespace explicitly.
- Unknown, transitional, locked, or unreadable Scaleway Instance states never
  count as a safe process fence, and a deleted Instance does not hide a regional
  orphan attachment.
- Missing parent file systems fail clearly.
- Pool full errors are clear and actionable.
- Actual parent free-space reserve errors are clear and actionable.
- An unexpected observed parent-size regression fails closed for new placement
  without tearing down existing mounts; Scaleway shrink is never advertised or
  tested as a supported operation.
- At the default overcommit ratio, accepted logical reservations exclude the
  configured byte/percentage safety reserve.
- Repeated CSI calls are idempotent.
- Shared-parent capacity is monitored and alerted, because v1 PVC sizes are
  logical reservations rather than hard filesystem quotas.

### 15.3 Open Source Quality

- MIT license present.
- README usable by someone outside URLab.
- No private URLab paths or names in code.
- No private registry references.
- Public chart and image references are versioned and installable.
- Versioned `csi-admin` binaries are public, checksum-verifiable, and protocol
  compatible with the matching release.
- Production chart images are pinned by immutable digest.
- No hardcoded production domains.
- CI passes.
- E2E scripts are documented and cleanup-safe.

## 16. Guidance for the Development Agent

Before writing code:

1. Read the normative Scaleway provider appendix in section 6.6 and preserve its
   API, IAM, node identity, attachment polling, mount, and coexistence contracts.
2. Read the official Scaleway File Storage CSI driver at the pinned reference
   commit and record any intentional divergence in code comments and tests.
3. Read the Kubernetes CSI developer documentation.
4. Read `csi-driver-nfs` and `nfs-subdir-external-provisioner` only for the
   subdirectory provisioning pattern.
5. Read the Scaleway File Storage documentation for Kapsule, mount, resize,
   and Instance selection constraints.
6. Do not copy private URLab code.
7. Do not use private product-specific names in the public codebase.
8. Do not depend on undocumented NFS behavior.

Implementation approach:

1. Scaffold the Go module, Dockerfile, Makefile, Helm chart, and CI first.
2. Implement pure packages first: handle parsing, allocation records,
   ownership records, path safety, and pool accounting.
3. Add fake Scaleway and fake mounter clients.
4. Implement CSI Identity.
5. Implement Controller create/delete with fake clients.
6. Implement Node stage/publish with fake mounter.
7. Add CSI sanity tests.
8. Add Helm chart tests.
9. Implement real Scaleway client integration.
10. Run a minimal real Scaleway smoke test before adding broader operational
    features.
11. Run the full e2e suite on a real Scaleway test project before any
    production-ready release.

Engineering principles:

- keep the code simple;
- keep files focused;
- do not introduce a CRD unless the PV-based accounting model proves
  insufficient;
- use allocation records as the durable source of truth; use Kubernetes PVs and
  CSI volume handles as projections and recovery inputs only;
- prefer explicit failures over best-effort silent behavior;
- document every non-obvious storage safety decision in code comments;
- do not ship without e2e evidence on a real Kapsule cluster.
