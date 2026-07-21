# Operations Guide

This repository contains frozen public identities. The `v0.1.0-rc.1` through
`v0.1.0-rc.9` candidates are superseded and must not be promoted. The third
candidate exposed the Kapsule/container-runtime quarantine mount behavior. The
fourth candidate was rejected before provider mutation because its release
checksum manifest omitted the chart and values; candidate construction now
rejects that incomplete artifact set. The fifth candidate exposed that live
preflight incorrectly rejected the provider's creatable but limited `scarce`
Kapsule availability. The sixth candidate was superseded before provider
mutation because its scenario runner briefly staged controller credentials in
the persistent evidence directory; credentials are now streamed and removed
from unrelated child-process environments. The seventh candidate reached the
real archive path and proved that Scaleway File Storage `virtiofs` rejects
directory `renameat2(RENAME_NOREPLACE)`; it also exposed an incorrect PVC-count
expression in the smoke harness. The eighth candidate proved automatic recovery
of that prepared real archive, but pre-scenario review found a second copy of
the same incorrect count expression. The compatibility path and both harness
sites now have focused regression coverage. The ninth candidate completed the
five functional `run-smoke` scenarios on real Kapsule, but its POSIX-shell result
collector allowed scenario code to overwrite the generic scenario-name
variable. The operation logs and their ordered hashes were retained, but the
resulting scenario names and evidence filenames were not admissible evidence.
The collector now uses reserved variables with behavioral regression coverage.
The next candidate will be `v0.1.0-rc.10`, but it is not yet a qualified
production release. The
source chart rejects `release.mode=production`; only an exact promoted chart
copy with immutable image metadata may enable it. Supported versions and
real-provider evidence still require approval. The CSI runtime and checkpoint
export/restore, parent decommission,
GC, upgrade preflight, and safe-uninstall operator workflows are implemented
and locally testable. The automated disposable kind chart-install/PVC/restart
suite is implemented and runs without Scaleway access. Privileged Linux, exact
packaged-artifact, and real Kapsule qualification still have to pass for the
exact release candidate, and the kind result must be repeated against those
exact artifacts. The procedures below define that review boundary; do not
treat a development chart render as a supported release until it passes every gate in
[`SPECIFICATION.md`](SPECIFICATION.md).

## Safety rules

- Never change a real Scaleway resource without a current approved plan naming
  the exact Project, region, resources, cost, and cleanup.
- Never infer absence from timeout, forbidden/unreadable data, stale cache,
  missing pages, or unknown provider state.
- Never edit allocation or ownership lifecycle fields manually.
- Never run `helm uninstall` before an audited safe-uninstall result.
- Retain the installation identity for the lifetime of every claim/PV/tombstone.

## Release installation prerequisites

A release requires a dedicated privileged namespace; final stable CSI name;
external identity and credential Secrets; Project-scoped
`FileStorageReadOnly` plus `InstancesFullAccess`; existing dedicated empty
parents; the Kapsule `scw-filestorage-csi` tag; homogeneous compatible Linux
nodes; at least two Ready controller candidates; and qualified image tags plus
immutable digests. Render and review with `make helm-test` before installation.
Every node kernel/runtime profile must provide `STATX_MNT_ID_UNIQUE`,
`open_tree`, `move_mount`, `mount_setattr`, `fsopen`, `fsconfig`, `fsmount`,
procfs mount-FD identity, and exact
detach. Driver startup proves those primitives using only its dedicated private
mount-quarantine `emptyDir` and remains non-ready on failure.
That quarantine is mounted at
`/run/scaleway-sfs-subdir-csi-mount-quarantine`, separately from the strict
`0700` admin-socket directory. The driver converts the authenticated dedicated
mount to `MS_PRIVATE` inside its own mount namespace because some container
runtimes initially expose an `emptyDir` as a slave mount; it then verifies that
the same mount generation is private. Cleanup retries reconcile it before
treating an absent public target as success or detaching a parent.
The two candidates may be in the same Scaleway zone; production preflight does
not require a multi-zone cluster.
The chart's sorted `compatibility.qualifiedCommercialTypes` must exactly match
the allowlist embedded in the release binaries and the retained real-E2E
matrix. Changing Helm values alone cannot qualify a new Instance type and makes
production startup fail closed.

Before Helm installation, run the operator-side preflight with `kubectl`,
`scw`, and `jq` authenticated to the intended cluster and Project:

```bash
./hack/install-preflight.sh \
  --namespace=scaleway-sfs-subdir-csi \
  --credentials-secret=scaleway-sfs-subdir-csi-credentials \
  --credentials-access-key=SCW_ACCESS_KEY \
  --credentials-secret-key=SCW_SECRET_KEY \
  --identity-secret=scaleway-sfs-subdir-csi-identity \
  --identity-key=installationID \
  --cluster-id=<kapsule-cluster-uuid> \
  --project-id=<scaleway-project-uuid> \
  --region=fr-par
```

The three key-name arguments must exactly match
`scaleway.credentials.accessKeyKey`, `scaleway.credentials.secretKeyKey`, and
`installation.idKey` in the Helm values used for the installation. Their
defaults match the chart defaults, but spelling them out makes the reviewed
preflight/Helm relationship auditable. The command reads the namespace and
external Secret key names, submits one
non-persistent server-side dry-run Pod to prove effective privileged admission,
and reads the exact Kapsule cluster through the Scaleway CLI to verify its
Project, region, type, and cluster-level `scw-filestorage-csi` tag. It never
prints Secret values and does not create a Kubernetes or Scaleway resource.
Run it again after changing admission policy, namespace, cluster, or Secrets.

## Real E2E planning, execution, and cleanup

The repository provides separate review and execution boundaries. Merely
having the executor does not make this development build release-ready: the
exact candidate still needs a separately approved run and retained successful
Kapsule and cleanup evidence.

No GitHub Actions workflow calls these commands or receives Scaleway
credentials. Real E2E is deliberately an operator-controlled procedure in v1.
It must be started from a dedicated test environment only after the immediate
approval below; normal CI, pull requests, pushes, tags, and GitHub releases
cannot provision, resize, detach, stop, or delete Scaleway resources.

Render a preflight request from an exact regular JSON file and retain stdout:

```bash
go run ./hack/scaleway-e2e-plan --input=/absolute/path/request.json
```

The request is closed-schema. It includes the exact Project, `fr-par`, UUID run
ID, run-containing DNS prefix, absolute evidence directory, cluster
create/reuse choice, one run-owned Private Network for a created cluster, fresh
two-or-three-node run-owned pool, two parents,
reviewed aggregate hourly cost, Git/chart identity, and immutable driver and
sidecar digests. It also carries a canonical provider review no older than 24
hours: documented GA product status and source, remaining File Storage quota
and source, and the pricing source used for the aggregate cost. The legacy
`publicBetaAccepted` field must be `false`. Live regional access, candidate
allowlist membership and positive `MaxFileSystems` are still re-read before
mutation. The base profile requires two product-minimum 25 GB parents. The
release-candidate profile uses 100 GB increments and also accounts for one
standalone run-owned disposable Instance of the selected node commercial type.
Output always says that mutation is not
authorized and that immediate approval is still required. Live product, quota,
availability, attach-limit, commercial-type, artifact, and price review remains
mandatory immediately before execution; API-backed facts are read live and the
documented product/quota/pricing evidence must be no older than 24 hours.

After reviewing the canonical plan, obtain a new explicit approval naming the
exact Project, region, run ID, resources, estimated hourly cost, destructive
operations, and cleanup command. Only then may an operator with the dedicated
test-Project credentials run:

```bash
go run ./hack/scaleway-e2e-run \
  --input=/absolute/path/execution-request.json \
  --execute \
  --confirm-run-id=11111111-1111-4111-8111-111111111111
```

Load those credentials from process memory or a root-only volatile filesystem
such as a verified Linux `tmpfs`. Never copy them into the repository, evidence
directory, shell arguments, a systemd unit, or another persistent runner path.
The executor streams the controller Secret, removes the keys from unrelated
child-process environments, and retains no plaintext credential artifact. The
volatile source must be removed after execution; destroying the disposable
cluster removes the controller Secret.

For a `base` request, this command executes only the fixed non-qualifying smoke:
ten logical PVCs, cross-node RWX, isolation and archive, controller replacement,
provider attachment inventory bounded to the two parents and two nodes,
structured safe uninstall, and exact cleanup.
Its output is named `kapsule-smoke-evidence-<run-id>.json`, contains
`releaseQualified=false`, and cannot be used by release qualification. A
`release-candidate` request remains rejected before credentials, live reads, or
resource creation while its production scenario blocker list is non-empty.
This is a safety interlock, not a test skip. `--cleanup-only` remains supported
for a retained previously approved run.

Dry-run is the default and never loads credentials. The live form creates only
the request's tagged resources, creates and journals a run-owned Private
Network before a run-owned cluster, binds the cluster to that exact immutable
network ID, fsyncs every exact ID to the cleanup ledger,
executes the fixed scenario set, and attempts cleanup after every failure.
Never retry a full execution while its ledger exists. Resume cleanup with a
new immediate approval and the same request/run ID:

```bash
go run ./hack/scaleway-e2e-run \
  --input=/absolute/path/execution-request.json \
  --cleanup-only \
  --confirm-run-id=11111111-1111-4111-8111-111111111111
```

Cleanup re-discovers only exact deterministic names with the complete run tag,
recovers their IDs into the ledger, and still deletes only after exact-ID
identity checks and all Kubernetes, mount, detach, controller-stop, and Helm
barriers succeed. A collision or ambiguous read fails closed. The retained
fsynced ledger is mandatory; losing it is an operator-recovery condition and
`--cleanup-only` never recreates a permissive seed. Helm and Kubernetes errors
remain errors, while successful cleanup preconditions are derived from the
completed structured safe-uninstall audit. A failed first Helm install may use
the run-bound bootstrap-abort proof only on a cluster created by that run and
only when no scenario entry, CSI object, durable record, CSINode registration,
or provider parent attachment exists;
this path never represents a successful smoke test. After a provisioning error,
several stable successful discovery reads are required before a partial resource
prefix can become complete.

After normal workload/PVC/PV cleanup and the complete safe-uninstall procedure,
render a cleanup review from the separately retained exact-ID inventory:

```bash
go run ./hack/scaleway-e2e-cleanup \
  --inventory=/absolute/path/scaleway-e2e-inventory-RUN_ID.json \
  --dry-run
```

The inventory must include the cluster, its run-owned Private Network when the
cluster was created by the run, the newly created node pool, both parents, and
the release-candidate disposable Instance when applicable. Every created
resource carries the exact run prefix/tag and every resource has an exact ID,
Project, region, creation provenance, and fresh closed state. The verifier
blocks the entire deletion list for stale or unknown provider evidence or any
incomplete Kubernetes, unmount, detach, controller-stop, or Helm-uninstall
barrier. Cleanup deletes a run-owned cluster before its exact run-owned Private
Network. A reused cluster and its pre-existing network are retained; neither is
a deletion candidate. Even an unblocked review grants no authority. The
credentialed cleanup command
repeats live reads, but still requires a new explicit approval immediately
before mutation.

The E2E cleanup owns only its dedicated run-labelled namespace, installation
identity, parents, and terminal test allocations. If the test delete policy
produced `Archived` or `Retained` records, the runner durably records an exact
GC plan, runs and retains one `csi-admin gc submit` dry-run and execute audit per
logical volume, and requires every record to reach the permanent `Deleted`
tombstone before safe uninstall. It never weakens the production uninstall
contract or makes `csi-admin uninstall prepare` delete retained user data.

Run the focused local trust-boundary tests with `make test-e2e-safety`. This
does not contact Kubernetes or Scaleway and does not replace real E2E evidence.

## Upgrade preflight and rollback boundary

For a qualified release, render the candidate declaration to a clean absolute
path and run the checksum-verified, version-matched operator command before
stopping the old controller:

```bash
csi-admin upgrade preflight \
  --namespace=driver-system \
  --release=driver \
  --request-id=11111111-1111-4111-8111-111111111111 \
  --candidate-file=/absolute/path/candidate.json \
  --timeout=10m
```

The operator verifies one exact non-symlink regular candidate inode before any
cluster access, sends only its canonical bounded payload through `kubectl exec
-i`, rejects duplicate or unknown flags/fields, and performs the protocol
handshake before the live comparison. Retain the returned live/candidate audit
with the rollout record.

Never bypass the preflight. A driver-name, installation/cluster identity,
parent mapping, schema-reader/writer, or node-configuration mismatch is
expected to produce a safe outage at startup; bypassing the comparison does not
turn that mismatch into a supported upgrade. During an online N/N-1 rollout,
new allocation/ownership writes remain blocked until every eligible Ready node
reports the candidate configuration generation. Reservation-journal and
journal-set schema `1` are immutable throughout v1 and covered by compatibility
fixtures rather than extra operator fields. If a migration cannot preserve N-1
readability, treat it as an explicit offline upgrade: quiesce, complete and
verify a checkpoint, stop controller and nodes, run the idempotent migration,
then start only version N. Once that migration writes a schema an older release
cannot read, rollback to that older release is unsupported unless the specific
release contract explicitly documents a reverse migration.

## Normal controller replacement

Remove readiness, reject/cancel work, drain mutations, then CAS the exact Lease
generation to clear the holder and install the complete graceful-release marker.
Exit only after success. The successor validates and consumes the exact marker
in its acquisition CAS. A failed release or shutdown deadline leaves an
abnormal takeover; Lease expiry alone does not authorize it.

## Abnormal takeover

1. Record the condition time and exact Lease/previous-holder evidence.
2. Stop/delete the previous process or Instance and explicitly unmount/detach
   configured parents where required.
3. Prove every parent absent from both regional and Instance inventories.
4. Create the fixed immutable approval Secret with the exact previous holder,
   installation/cluster identity, new request ID, reason, and validity at most
   one hour.
5. The successor repeats the provider fence and atomically consumes approval
   with its new holder before mutation.
6. Retain audit and delete the consumed approval Secret.

## Checkpoint and recovery

Choose a new request UUID and an absent output path on a local filesystem that
supports hard links. Then prepare and export while the controller remains
quiesced:

```bash
csi-admin checkpoint prepare \
  --namespace=driver-system \
  --release=driver \
  --request-id=11111111-1111-4111-8111-111111111111 \
  --output-file=/absolute/path/checkpoint-11111111.tar \
  --timeout=30m
```

The command closes mutation admission, drains work, requires the committed
reservation-journal set and every permanent pool journal to be exactly
`Ready`/`Idle`, rejects transitional or one-sided state, captures the bounded
ticket, and streams the verified package
through the controller-only Unix socket and `kubectl exec -i`. It writes a
same-directory temporary inode, fsyncs it, publishes without replacing an
existing path, and returns the exact manifest/archive SHA-256 and byte count.
Retain that JSON audit beside the mode-`0600` tar. A nonzero exit before atomic
publication can leave a hidden temporary file. A directory-fsync or
post-publication cleanup failure can instead leave a complete but
durability-unconfirmed requested output; it never publishes a truncated
archive. Inspect the exact error and paths before removing anything, then retry
the same request with an absent output path while the controller remains
quiesced.

After the complete archive and audit are durably copied to the backup location,
resume with the same request:

```bash
csi-admin checkpoint resume \
  --namespace=driver-system \
  --release=driver \
  --request-id=11111111-1111-4111-8111-111111111111 \
  --timeout=30m
```

Resume performs full reconciliation before reopening admission. Crash,
SIGTERM, Lease loss, a changed source UID/resourceVersion, a changed ownership
record, archive truncation, or a ticket mismatch invalidates the attempt and is
never treated as a completed checkpoint.

For same-cluster namespace recovery, first restore the namespace and exact
installation identity Secret, but do not start the release controller or node
DaemonSet. Preserve the cluster-scoped PVs; the restore command verifies them
and never recreates or overwrites them. Run a read-only plan with the completed
archive and its exact embedded request ID:

```bash
csi-admin checkpoint restore \
  --namespace=driver-system \
  --release=driver \
  --request-id=11111111-1111-4111-8111-111111111111 \
  --archive-file=/absolute/path/checkpoint-11111111.tar \
  --identity-secret=scaleway-sfs-subdir-csi-identity \
  --identity-key=installationID \
  --mode=dry-run \
  --timeout=30m
```

The command rejects a changed inode, non-canonical tar, wrong cluster or
identity, missing/extra/changed PV, conflicting/extra allocation, active
release Pod, node DaemonSet, nonzero controller Deployment, or nonempty Lease.
After reviewing its exact archive, PV, journal, and planned-allocation audit,
repeat only with `--mode=execute`. Execute recreates the exact all-`Idle`
permanent journals and their `Ready` set commitment first, then creates missing
allocation ConfigMaps with create-only semantics, rereads and verifies the
complete restore-stable object aggregate, and creates the fixed immutable
`sfs-subdir-checkpoint` Secret last.
A retry accepts only exact records and exact Secret bytes. It never writes a PV
or parent filesystem.

Then install or retain the release with only the controller starting into its
provisional non-serving recovery path. Stop/delete every pre-recovery Instance,
prove parent claims match the current cluster, and prove only the new
provisional Instance appears in complete attachment inventories. Create the
checkpoint-bound `missing-lease-recovery` approval with
`all-pre-recovery-instances` scope. Stop/drain provisional renewal, repeat fresh
fencing, consume approval in the promotion CAS, and reconcile every mapping
before serving. Delete the consumed approval and in-cluster checkpoint Secrets
only after successful retained audit; keep the external archive. V1 never
authorizes a different cluster UID.

## Parent decommission

```text
active -> draining -> no references -> stop driver -> unmount everywhere
       -> detach everywhere -> remove from values
```

Before stop, require no PV, VolumeAttachment, reserving/non-Deleted allocation,
published fence, stage/target/child mount, or detailed ownership. Every compact
ownership tombstone must exactly pair with a non-reserving Kubernetes
`Deleted` tombstone. The target parent must also have no unresolved `Pending`
reservation journal. Node prepare closes and drains Node Stage/Publish/
Unpublish/Unstage admission before inspecting or unmounting its exact root.
After stop, unmount each exact root, detach only enumerated
Instances, and poll both inventories to fresh absence. Only then remove values.
The permanent parent claim and user data remain and the parent is not reusable.

Use one stable request UUID and keep the parent configured as `draining`. Start
with a read-only plan:

```bash
csi-admin decommission prepare \
  --namespace=driver-system \
  --release=driver \
  --request-id=11111111-1111-4111-8111-111111111111 \
  --parent-filesystem-id=22222222-2222-4222-8222-222222222222 \
  --mode=dry-run \
  --timeout=30m
```

Remove every reported PV/PVC/workload/VolumeAttachment reference through
normal Kubernetes workflows. A malformed or one-sided tombstone is a safety
error, not a removable blocker. Rerun dry-run until it is ready, then change
only `--mode=execute`. Execute revalidates under quiesce, persists a
Deployment-owned request progress ConfigMap, unmounts only the selected parent
on every frozen node, deletes the exact node DaemonSet with a UID precondition,
performs controller target-only unmount/detach with fresh dual inventories,
releases the Lease, and scales the controller to zero. Retry the same command
and request ID after interruption; never invoke private local phases manually.
Preserve the final JSON audit before removing only that parent from Helm values
and restarting the release. Other configured parents are never detached by
this workflow.

The final result is an offline values-change boundary, not an online parent
removal. Keep the whole release stopped, remove only the completed parent from
the candidate values, render and review the exact chart change, then apply the
Helm upgrade that recreates the node DaemonSet and returns the controller to one
replica. The completed decommission audit is the authority for this otherwise
unsupported parent-removal change; the normal live upgrade preflight cannot run
after the controller has intentionally stopped. Retain the progress ConfigMap
and final JSON until the restarted release has completed startup reconciliation.

## Tombstone compaction

`controller.detailedTombstoneRetention` defaults to `720h`. After that window,
the active controller selects at most 100 configured-parent detailed `Deleted`
allocations per maintenance pass, authenticates the already-compact ownership
peer, and updates the same ConfigMap by resourceVersion CAS. It never deletes a
tombstone. Records for an already offline-decommissioned parent remain detailed
rather than causing an implicit remount. A compaction mismatch removes
maintenance readiness and requires investigation; do not edit either durable
record manually.

## Manual archive/retain garbage collection

Always run a dry-run with a fresh UUID and the exact current terminal state:

```bash
csi-admin gc submit \
  --namespace=driver-system \
  --release=driver \
  --request-id=11111111-1111-4111-8111-111111111111 \
  --logical-volume-id=lv-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa \
  --mode=dry-run \
  --expected-state=Archived \
  --timeout=30m
```

After reviewing the exact parent and paths, use a new request ID for
`--mode=execute` and keep that ID unchanged for every retry. `Retained` must be
declared explicitly when it is the current state. The operator binary only
submits and audits the request; the active controller owns all locks, fencing,
durable phases, quarantine rename, deletion, and terminal compaction. Never
edit GC lifecycle fields or delete a tombstone ConfigMap manually.

## Safe uninstall

Use only a checksum-verified `csi-admin` from the same qualified release. Start
with a stable UUID and a read-only plan:

```bash
csi-admin uninstall prepare \
  --namespace=driver-system \
  --release=driver \
  --request-id=11111111-1111-4111-8111-111111111111 \
  --mode=dry-run \
  --timeout=30m
```

The caller's kubeconfig must authorize cluster inventory reads, exact release
Pod exec, one request progress ConfigMap, deletion of the exact node DaemonSet,
and scaling the exact controller Deployment. None of those privileges belongs
to the driver ServiceAccounts. Remove every reported workload/PVC/PV blocker
normally, rerun dry-run, then change only `--mode=execute` while retaining the
same request ID. Interrupted execution resumes from the Deployment-owned
progress ConfigMap. Preserve the completed JSON audit before Helm removal; the
progress object is garbage-collected with that exact controller Deployment.

1. Inventory PV/PVC/Pod/VolumeAttachment/allocation/fence/mount state.
2. Remove workloads and PVCs through normal Kubernetes flows; never implicitly.
3. Require no live PV, non-terminal allocation, attachment, fence, stage/target.
4. Quiesce under the uninstall request ID, resolve every exact `Pending`
   reservation, and require every permanent journal to be `Idle`; a resulting
   `Reserved` allocation remains a blocker.
5. On each node, remove readiness, close and drain Node mutation admission,
   then prove/unmount every configured parent on every exact eligible node; retain
   results; delete the exact node DaemonSet with a UID precondition and verify
   Pod termination. DaemonSets have no Kubernetes `scale` subresource.
6. Unmount controller parents, detach exact installation Instances, and prove
   fresh regional/Instance inventories empty.
7. Gracefully release the Lease with the uninstall request ID, scale controller
   to zero while RBAC remains, and verify the exact marker and Pod termination.
8. Only after final audit may the operator run `helm uninstall`.

Helm never deletes tombstones, external Secrets, claims, or user data.
